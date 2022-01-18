""" Database handle to tables that contain stock and news data. """

from pathlib import Path
import pandas as pd
from typing import List
from news_scanner.database_handles.base_handle import BaseHandle, DATABASE_DIR
from news_scanner.result_object import NewsReport
from news_scanner.database_handles.table_handles.stock_data_processed_news_tables.\
    aggregated_data_handle import AggregatedDataHandle
from news_scanner.database_handles.table_handles.stock_data_processed_news_tables.\
    scrapped_news_handle import ScrappedNewsHandle
from news_scanner.database_handles.table_handles.stock_data_processed_news_tables.\
    stock_data_handle import StockDataHandle

_DATABASE_PATH = DATABASE_DIR / "stock_data_processed_news.sqlite"


class StockDataProcessedNewsHandle(BaseHandle):
    """ Database handle to tables that contain stock and news data. """

    def __init__(
        self,
        database_path: Path = _DATABASE_PATH,
        database_dir: Path = DATABASE_DIR
    ):
        """ Initializes database and table handles.

        Params:
            database_path: Path to database file.
            database_dir: Path to directory which will house database files.
        """
        super().__init__(
            database_path=database_path,
            database_dir=database_dir
        )  # must be first
        self.aggregated_data_handle = AggregatedDataHandle(
            database_path=database_path,
            database_dir=database_dir
        )
        self.scrapped_news_handle = ScrappedNewsHandle(
            database_path=database_path,
            database_dir=database_dir
        )
        self.stock_data_handle = StockDataHandle(
            database_path=database_path,
            database_dir=database_dir
        )

    def insert(self, news_reports: List[NewsReport]) -> None:
        """ Inserts a news_report into the respective tables in the database. """
        agg_id = self._get_last_agg_id()
        # init agg_id if none found in table
        if not agg_id:
            agg_id = 1
        # increment next agg_id
        else:
            agg_id += 1

        for news_report in news_reports:
            print("agg_id:", agg_id)
            self.aggregated_data_handle.insert(
                agg_id=agg_id,
                ticker=news_report.ticker,
                exchange=news_report.exchange
            )
            self.scrapped_news_handle.insert(
                agg_id=agg_id,
                scrapped_result=news_report.scrappedNewsResults
            )
            self.stock_data_handle.insert(
                agg_id=agg_id,
                stock_data=news_report.stockData
            )
            agg_id += 1

    def _get_last_agg_id(self) -> int:
        """ Returns the last agg_id from the aggregated_data table. """
        query = f"SELECT MAX(agg_id) FROM " \
                f"{self.aggregated_data_handle.TABLE_NAME}"
        res = self.execute_query(query)
        return res[0][0]

    def get_all(self) -> pd.DataFrame:
        """ Returns related database contents as a dataframe. """
        agg_id = self.aggregated_data_handle.PRIMARY_KEY
        agg_table = self.aggregated_data_handle.TABLE_NAME
        scrape_table = self.scrapped_news_handle.TABLE_NAME
        stock_table = self.stock_data_handle.TABLE_NAME
        temp_table = "temp_table"

        temp_table_query = f"""CREATE TEMP TABLE {temp_table} AS SELECT * FROM (({agg_table} INNER JOIN 
                {scrape_table} ON {agg_table}.{agg_id} = {scrape_table}.{agg_id})
                INNER JOIN {stock_table} ON {agg_table}.{agg_id} = {stock_table}.{agg_id})
                """
        data_query = f"SELECT * FROM {temp_table}"
        col_name_query = f"PRAGMA table_info({temp_table})"

        data = self.execute_query([temp_table_query, data_query])
        columns = self.execute_query([temp_table_query, col_name_query])
        column_names = [col[1] for col in columns]
        res_df = pd.DataFrame(data=data, columns=column_names)
        # dropping columns created by temp_table_query
        res_df = res_df.drop(columns=["agg_id:1", "agg_id:2"])

        return res_df
