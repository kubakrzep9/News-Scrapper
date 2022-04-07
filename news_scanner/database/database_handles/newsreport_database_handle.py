""" Database handle to tables that contain stock and news data. """

from pathlib import Path
import pandas as pd
from typing import List
from news_scanner.database.database_handles.base_database_handle import BaseDatabaseHandle
from news_scanner.result_object import NewsReport, ALLOWED_NAMED_TUPLES
from news_scanner.database.constants import (
    PYTHON_TO_SQL_DTYPES,
    DEF_PRIMARY_KEY,
    DB_DIR
)