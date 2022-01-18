""" Database handle to 'stock_data' table. """

from pathlib import Path
from news_scanner.database_handles.base_handle import BaseHandle
from news_scanner.database_handles.table_handles.\
    stock_data_processed_news_tables.constants import AGG_ID
from news_scanner.td_api.td_api_handle import StockData


class StockDataHandle(BaseHandle):
    """ Database handle to 'stock_data' table. """

    TABLE_NAME = "stock_data"

    # column names
    PRIMARY_KEY = AGG_ID
    MARKET_CAP = "market_cap"
    MARKET_CAP_FLOAT = "market_cap_floating"
    SHARES_OUTSTANDING = "shares_outstanding"
    LAST_PRICE = "last_price"

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
                        [{self.MARKET_CAP}] REAL, 
                        [{self.MARKET_CAP_FLOAT}] REAL,
                        [{self.SHARES_OUTSTANDING}] REAL,
                        [{self.LAST_PRICE}] REAL)
                    """
            self.execute_query(query)

    def insert(
        self,
        agg_id: int,
        stock_data: StockData
    ) -> None:
        """ Inserts data into the table.

        Params:
            agg_id: Primary key value.
            stock_data: Stock data to be stored in table.
        """
        query = f"""INSERT INTO {self.TABLE_NAME} (
                    {self.PRIMARY_KEY}, {self.MARKET_CAP}, 
                    {self.MARKET_CAP_FLOAT}, {self.SHARES_OUTSTANDING}, 
                    {self.LAST_PRICE}) VALUES( 
                    {agg_id}, '{stock_data.market_cap}', 
                    '{stock_data.market_cap_float}', 
                    '{stock_data.shares_outstanding}',
                    '{stock_data.last_price}')"""
        self.execute_query(query)
