""" Database handle to tables that contain stock and news data. """

from news_scanner.database.database_handles.base_database_handle import BaseDatabaseHandle, DBObjectConfig
from news_scanner.result_object import NewsReport, ALLOWED_NAMED_TUPLES
from news_scanner.database.constants import DB_DIR
from pathlib import Path


class NewsReportDatabaseHandle(BaseDatabaseHandle):
    def __init__(self, db_dir: Path = DB_DIR):
        super().__init__(
            db_object_config=DBObjectConfig(
                complex_nt_type=NewsReport,
                allowed_namedtuples=ALLOWED_NAMED_TUPLES,
            ),
            db_dir=db_dir,
        )
