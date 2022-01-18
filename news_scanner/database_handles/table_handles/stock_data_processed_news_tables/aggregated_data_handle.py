""" Database handle to 'aggregated_data' table. """

from pathlib import Path
from news_scanner.database_handles.base_handle import BaseHandle
from news_scanner.database_handles.table_handles.\
    stock_data_processed_news_tables.constants import AGG_ID


class AggregatedDataHandle(BaseHandle):
    """ Database handle to 'aggregated_data' table. """

    TABLE_NAME = "aggregated_data"

    # column names
    PRIMARY_KEY = AGG_ID
    TICKER = "ticker"
    EXCHANGE = "exchange"

    def __init__(
        self,
        database_path: Path,
        database_dir: Path
    ):
        """ Initializes database and tables.

        Params:
            database_path: Path to database file.
            database_dir: Path to directory which will house database files.
        """
        super().__init__(
            database_path=database_path,
            database_dir=database_dir,
            table_name=self.TABLE_NAME
        )  # must be first
        self._init_tables()

    def _init_tables(self):
        """ Creates table if it doesn't exist. """
        if not self.table_exists(self.TABLE_NAME):
            query = f"""CREATE TABLE {self.TABLE_NAME}(
                        [{self.PRIMARY_KEY}] INTEGER PRIMARY KEY, 
                        [{self.TICKER}] TEXT, 
                        [{self.EXCHANGE}] TEXT)"""
            self.execute_query(query)

    def insert(
        self,
        agg_id: int,
        ticker: str,
        exchange: str,
    ) -> None:
        """ Inserts data into the table.

        Params:
            agg_id: Primary key value.
            ticker: Stock symbol.
            exchange: Stock exchange where ticker is traded.
        """
        query = f"""INSERT INTO {self.TABLE_NAME} (
                    {self.PRIMARY_KEY}, {self.TICKER}, {self.EXCHANGE} 
                    ) VALUES({agg_id}, '{ticker}', '{exchange}')"""
        self.execute_query(query)
