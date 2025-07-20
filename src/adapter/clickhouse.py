import os
import pandas as pd
import clickhouse_connect
from dotenv import load_dotenv
from zope.interface import implementer
from src.interface.database import DatabaseInterface

load_dotenv()

@implementer(DatabaseInterface)
class _ClickHouse:
    """ClickHouse database adapter."""

    def __init__(self, credentials: dict = None):
        """Initializes the ClickHouse adapter."""
        self.host = os.getenv('CLICKHOUSE_HOST', 'localhost')
        self.port = int(os.getenv('CLICKHOUSE_PORT', 8123))
        self.username = os.getenv('CLICKHOUSE_USER', 'default')
        self.password = os.getenv('CLICKHOUSE_PASSWORD', '')
        self.database = os.getenv('CLICKHOUSE_DB', 'default')
        self.client = None

    def connect(self) -> None:
        """Connects to the ClickHouse database."""
        self.client = clickhouse_connect.get_client(
            host=self.host,
            port=self.port,
            username=self.username,
            password=self.password,
            database=self.database
        )

    def get_query(self, query: str) -> tuple[list, list]:
        """Executes a query and returns the results.

        Args:
            query: The query to execute.

        Returns:
            A tuple containing the column names and the result rows.
        """
        result = self.client.query(query)
        return result.column_names, result.result_rows

    def insert_data(self, df: pd.DataFrame, table: str) -> None:
        """Inserts a DataFrame into a table.

        Args:
            df: The DataFrame to insert.
            table: The name of the table.
        """
        if 'date' in df.columns:
            df['date'] = pd.to_datetime(df['date']).dt.date

        ddl = f"""
        CREATE TABLE IF NOT EXISTS {table} (
            date Date,
            mine_id UInt32,
            total_production_daily Float64,
            average_quality_grade Float64,
            equipment_utilization Float64,
            fuel_efficiency Float64,
            rainfall_mm Float64
        ) ENGINE = MergeTree()
        ORDER BY (date, mine_id)
        """
        self.client.command(ddl)
        self.client.insert_df(table, df)

    def close(self) -> None:
        """Closes the database connection."""
        self.client = None


def ClickHouse(credentials: dict = None) -> DatabaseInterface:
    return _ClickHouse(credentials)
