import aiomysql
import pandas as pd
from dotenv import load_dotenv
import os
from zope.interface import implementer
from src.interface.database import DatabaseInterface

load_dotenv()

@implementer(DatabaseInterface)
class _MySQL:
    """MySQL database adapter."""

    def __init__(self, credentials: dict = None):
        """Initializes the MySQL adapter."""
        self.host = os.getenv('MYSQL_HOST', 'localhost')
        self.port = int(os.getenv('MYSQL_PORT', 3306))
        self.user = os.getenv('MYSQL_USER', 'root')
        self.password = os.getenv('MYSQL_PASSWORD', '')
        self.db = os.getenv('MYSQL_DB', 'coal_mining')
        self.connection = None

    async def connect(self) -> aiomysql.Connection:
        """Connects to the MySQL database."""
        self.connection = await aiomysql.connect(
            host=self.host,
            port=self.port,
            user=self.user,
            password=self.password,
            db=self.db
        )

    async def get_query(self, query: str) -> tuple[list, list]:
        """Executes a query and returns the results.

        Args:
            query: The query to execute.

        Returns:
            A tuple containing the column names and the result rows.
        """
        async with self.connection.cursor() as cur:
            await cur.execute(query)
            columns = [desc[0] for desc in cur.description]
            results = await cur.fetchall()
            return (columns, [list(row) for row in results])

    async def insert_data(self, df: pd.DataFrame, table: str) -> None:
        """Inserts a DataFrame into a table.

        Args:
            df: The DataFrame to insert.
            table: The name of the table.
        """
        async with self.connection.cursor() as cur:
            cols = ", ".join([f"`{col}`" for col in df.columns])
            placeholders = ", ".join(["%s"] * len(df.columns))
            query = f"REPLACE INTO {table} ({cols}) VALUES ({placeholders})"
            try:
                for _, row in df.iterrows():
                    await cur.execute(query, tuple(row))
                await self.connection.commit()
            except Exception as e:
                await self.connection.rollback()
                raise e

    async def close(self) -> None:
        """Closes the database connection."""
        self.connection.close()


def MySQL(credentials: dict = None) -> DatabaseInterface:
    return _MySQL(credentials)
