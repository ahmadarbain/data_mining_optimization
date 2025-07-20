import os
import re
import pandas as pd
import logging
import aiohttp
from datetime import datetime
from zope.interface import implementer
from src.interface.usecase import UseCaseInterface
from src.interface.database import DatabaseInterface

def load_production_logs_from_sql(path: str) -> pd.DataFrame:
    if not os.path.exists(path):
        raise FileNotFoundError(f"{path} not found.")

    with open(path, 'r', encoding='utf-8') as f:
        sql_text = f.read()

    insert_lines = re.findall(
        r"INSERT INTO production_logs\s*\(.*?\)\s*VALUES\s*(.*?);",
        sql_text,
        re.DOTALL
    )

    values = []
    for group in insert_lines:
        for row in re.findall(r"\((.*?)\)", group):
            values.append(eval(f"[{row}]"))

    columns = ["date", "mine_id", "shift", "tons_extracted", "quality_grade"]
    df = pd.DataFrame(values, columns=columns)
    df['date'] = pd.to_datetime(df['date']).dt.date
    return df

def load_equipment_sensors_csv(path: str) -> pd.DataFrame:
    if not os.path.exists(path):
        raise FileNotFoundError(f"{path} not found.")
    return pd.read_csv(path)

def info(tag, msg):
    logging.info(f"[{tag}] {msg}")

def error(tag, msg):
    logging.error(f"[{tag}] {msg}")

# ... (imports and helper functions remain unchanged)

@implementer(UseCaseInterface)
class ETLMetrics:
    def __init__(self, clickhouse: DatabaseInterface = None, production_sql_path: str = None, sensors_csv_path: str = None):
        self.clickhouse = clickhouse
        self.production_sql_path = production_sql_path
        self.sensors_csv_path = sensors_csv_path

    async def run(self) -> None:
        try:
            if self.clickhouse:
                self.clickhouse.connect()

            df_prod = load_production_logs_from_sql(self.production_sql_path)
            if df_prod.empty:
                info("ETL", "No production data found.")
                return

            df_sensors = load_equipment_sensors_csv(self.sensors_csv_path)
            print(f"Loaded {len(df_prod)} production records and {len(df_sensors)} sensor records.")
            print(df_prod.head())
            print(df_sensors.head())

            df_prod['date'] = pd.to_datetime(df_prod['date'])
            start_date = '2025-05-12'
            end_date = df_prod['date'].max().strftime('%Y-%m-%d')

            df_weather = await self.__fetch_weather_range(start_date, end_date)
            df_weather = df_weather[df_weather["date"].notnull()]
            df_weather["date"] = pd.to_datetime(df_weather["date"], format="%Y-%m-%d", errors="coerce")

            logging.info(f"Total weather records fetched: {len(df_weather)}")

            # === Transformation ===
            df_prod["tons_extracted"] = df_prod["tons_extracted"].apply(lambda x: max(x, 0))
            df_metrics = df_prod.groupby(["date", "mine_id"])["tons_extracted"].sum().reset_index(name="total_production_daily")

            avg_quality = df_prod.groupby(["date", "mine_id"])["quality_grade"].mean().reset_index(name="average_quality_grade")

            df_sensors["timestamp"] = pd.to_datetime(df_sensors["timestamp"], errors="coerce")
            df_sensors = df_sensors.dropna(subset=["timestamp"])
            df_sensors["date"] = df_sensors["timestamp"].dt.date

            utilization = df_sensors.groupby(["date", "equipment_id"])["status"].apply(
                lambda x: (x == "active").sum() / len(x) * 100
            ).reset_index(name="equipment_utilization")

            fuel = df_sensors.groupby(["date", "equipment_id"])["fuel_consumption"].sum().reset_index(name="fuel_total")
            tons = df_prod.groupby(["date", "mine_id"])["tons_extracted"].sum().reset_index(name="tons_total")
            fuel["date"] = pd.to_datetime(fuel["date"])
            tons["date"] = pd.to_datetime(tons["date"])

            fuel_efficiency = pd.merge(fuel, tons, on="date", how="inner")
            fuel_efficiency["fuel_efficiency"] = fuel_efficiency.apply(
                lambda row: row["fuel_total"] / row["tons_total"] if row["tons_total"] > 0 else None, axis=1
            )
            fuel_efficiency = fuel_efficiency[["date", "fuel_efficiency"]]

            # Merge all
            df_metrics["date"] = pd.to_datetime(df_metrics["date"])
            avg_quality["date"] = pd.to_datetime(avg_quality["date"])
            utilization["date"] = pd.to_datetime(utilization["date"])
            fuel_efficiency["date"] = pd.to_datetime(fuel_efficiency["date"])
            df_weather["date"] = pd.to_datetime(df_weather["date"])

            df_final = df_metrics.merge(avg_quality, on=["date", "mine_id"], how="left")
            df_final = df_final.merge(utilization, on="date", how="left")
            df_final = df_final.merge(fuel_efficiency, on="date", how="left")
            df_final = df_final.merge(df_weather[["date", "precipitation_sum"]], on="date", how="left")

            df_final.rename(columns={"precipitation_sum": "rainfall_mm"}, inplace=True)
            df_final["rainfall_mm"] = df_final["rainfall_mm"].fillna(0)

            # === Weather impact: Per-day deviation from rolling average ===
            df_final.sort_values("date", inplace=True)
            df_final["prod_rolling"] = df_final["total_production_daily"].rolling(window=3, min_periods=1).mean()
            df_final["weather_impact"] = df_final.apply(
                lambda row: row["total_production_daily"] - row["prod_rolling"]
                if row["rainfall_mm"] > 0 else 0,
                axis=1
            )
            df_final.drop(columns=["prod_rolling"], inplace=True)

            # Final debug and load
            print("\n[DEBUG] Final Metrics with Weather Impact:")
            print(df_final.head())

            self.clickhouse.insert_data(df_final, table="daily_production_metric")
            info("ETL", f"Loaded {len(df_final)} rows into ClickHouse.")

        except Exception as e:
            error("ETL", f"Failed: {str(e)}")

    async def __fetch_weather_range(self, start_date: str, end_date: str) -> pd.DataFrame:
        url = (
            "https://api.open-meteo.com/v1/forecast"
            "?latitude=2.0167&longitude=117.3"
            "&daily=temperature_2m_mean,precipitation_sum"
            "&timezone=Asia/Jakarta"
            f"&start_date={start_date}&end_date={end_date}"
        )
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url) as resp:
                    resp.raise_for_status()
                    json_data = await resp.json()
                    dates = json_data.get("daily", {}).get("time", [])
                    temperature = json_data.get("daily", {}).get("temperature_2m_mean", [])
                    rainfall = json_data.get("daily", {}).get("precipitation_sum", [])

                    if not (len(dates) == len(temperature) == len(rainfall)):
                        error("ETL", "Mismatch in weather data lengths.")
                        return pd.DataFrame(columns=["date", "temperature_2m_mean", "precipitation_sum"])

                    return pd.DataFrame({
                        "date": pd.to_datetime(dates, format="%Y-%m-%d", errors="coerce"),
                        "temperature_2m_mean": temperature,
                        "precipitation_sum": rainfall
                    })
        except Exception as e:
            error("ETL", f"Weather API error: {e}")
            return pd.DataFrame(columns=["date", "temperature_2m_mean", "precipitation_sum"])


def NewEtlMetrics(**kwargs) -> UseCaseInterface:
    return ETLMetrics(
        clickhouse=kwargs.get('clickhouse'),
        production_sql_path=kwargs.get('production_sql_path'),
        sensors_csv_path=kwargs.get('sensors_csv_path')
    )
