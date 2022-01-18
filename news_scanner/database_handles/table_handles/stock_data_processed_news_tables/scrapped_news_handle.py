""" Database handle to 'scrapped_news' table. """

from pathlib import Path
from news_scanner.database_handles.base_handle import BaseHandle
from news_scanner.database_handles.table_handles.\
    stock_data_processed_news_tables.constants import AGG_ID
from news_scanner.news_scrapper.target_news_scrapper import \
    ScrappedNewsResult


class ScrappedNewsHandle(BaseHandle):
    """ Database handle to 'scrapped_news' table. """

    TABLE_NAME = "scrapped_news"

    # column names
    PRIMARY_KEY = AGG_ID
    HEADLINE = "headline"
    LINK = "link"
    PUBLISH_DATE = "publish_date"
    CONTENT = "content"

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
                        [{self.HEADLINE}] TEXT, 
                        [{self.LINK}] TEXT,
                        [{self.PUBLISH_DATE}] TEXT,
                        [{self.CONTENT}] TEXT)
                    """
            self.execute_query(query)

    def insert(
        self,
        agg_id: int,
        scrapped_result: ScrappedNewsResult
    ) -> None:
        """ Inserts data into the table.

        Params:
            agg_id: Primary key value.
            scrapped_result: Scrapped data to be stored in table.
        """
        query = f"""INSERT INTO {self.TABLE_NAME} (
                    {self.PRIMARY_KEY}, {self.HEADLINE}, {self.LINK},
                    {self.PUBLISH_DATE}, {self.CONTENT}) VALUES( 
                    {agg_id}, '{scrapped_result.headline}', 
                    '{scrapped_result.link}', '{scrapped_result.publish_date}',
                    '{scrapped_result.content}')"""

        self.execute_query(query)
