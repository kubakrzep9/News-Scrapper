""" Database handle to tables that contain stock and news data. """

from news_scanner.database.database_handles.base_database_handle import BaseDatabaseHandle, DBObjectConfig
from news_scanner.result_object import NewsReport, ALLOWED_NAMED_TUPLES


class NewsReportDatabaseHandle(BaseDatabaseHandle):
    def __init__(self):
        super().__init__(
            db_object_config=DBObjectConfig(
                complex_nt_type=NewsReport,
                allowed_namedtuples=ALLOWED_NAMED_TUPLES
            ),
        )
