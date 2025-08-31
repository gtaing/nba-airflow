import os
import duckdb

from loguru import logger


class DuckDB:

    def __init__(self, database: str = "my_db"):
        self.database = database
        self.conn_str = f'md:{self.database}?motherduck_token={os.getenv("motherduck_token")}'
    
    def create_table_from_file(self, filepath: str, table_name: str) ->  None:
        conn = duckdb.connect(self.conn_str)

        logger.info(f'Creating MotherDuck/DuckDB table "{table_name}" from "{filepath}".')

        conn.execute(f"""
            CREATE TABLE IF NOT EXISTS {table_name}
            AS SELECT * FROM '{filepath}';
        """)

        logger.info(f'Table "{table_name}" has been created.')