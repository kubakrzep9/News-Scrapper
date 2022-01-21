""" Database handle to 'headline_keywords' table. """

from pathlib import Path
from news_scanner.database_handles.base_handle import BaseHandle
from news_scanner.database_handles.table_handles.\
    stock_data_processed_news_tables.constants import AGG_ID


class HeadlineKeywordsHandle(BaseHandle):
    """ Database handle to 'headline_keywords' table. """

    TABLE_NAME = "headline_keywords"

    # column names
    PRIMARY_KEY = "id"
    FOREIGN_KEY = AGG_ID
    KEYWORD = "keyword"
    COUNT = "count"

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
                        [{self.FOREIGN_KEY}] INTEGER, 
                        [{self.KEYWORD}] TEXT,
                        [{self.COUNT}] INTEGER)"""

            self.execute_query(query)

    def insert(
        self,
        primary_key: int,
        agg_id: int,
        keyword: str,
        count: int,
    ) -> None:
        """ Inserts data into the table.

        Params:
            primary_key: Primary key value.
            agg_id: Foreign key value.
            keyword: Keyword of interest.
            count: Number of occurrences.
        """
        query = f"""INSERT INTO {self.TABLE_NAME} (
                    {self.PRIMARY_KEY}, {self.FOREIGN_KEY}, {self.KEYWORD}, 
                    {self.COUNT}
                    ) VALUES({primary_key}, '{agg_id}', '{keyword}', '{count}')"""
        self.execute_query(query)
