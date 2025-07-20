import asyncio
import sys
import time
import logging
from src.adapter.clickhouse import ClickHouse
from src.usecase.daily_production import NewEtlMetrics  # Make sure the function name matches your implementation

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

REGISTERED_USECASES = [
    ('--etl_production', NewEtlMetrics),
]

async def main() -> None:
    args = sys.argv

    if len(args) < 4:
        print("Usage: python main.py --etl_production <production_sql_path> <sensors_csv_path>")
        sys.exit(1)

    use_case = args[1]
    production_sql_path = args[2]
    sensors_csv_path = args[3]

    clickhouse_db = ClickHouse()

    for flag, use_case_fn in REGISTERED_USECASES:
        if use_case == flag:
            use_case_instance = use_case_fn(
                production_sql_path=production_sql_path,
                sensors_csv_path=sensors_csv_path,
                clickhouse=clickhouse_db
            )
            await use_case_instance.run()
            break
    else:
        print(f"Unknown use case: {use_case}")
        sys.exit(1)

if __name__ == '__main__':
    start_time = time.time()
    asyncio.run(main())
    logging.info(f'Process time: {time.time() - start_time:.2f} second(s)')
