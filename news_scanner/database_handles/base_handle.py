""" Class to house common database functionality. """

import sqlite3
from sqlite3 import Error
import pandas as pd
from pathlib import Path
from contextlib import closing
from typing import List, Union

DATABASE_DIR = Path(__file__).parent.parent.parent / "databases"


class BaseHandle:
    """ Class to house common database functionality. """

    def __init__(
        self,
        database_path: Path,
        database_dir: Path = DATABASE_DIR,
        table_name: str = "base_table"
    ):
        """ Creates a directory if one doesn't exist to store the database.

        Note: Must be executed first in constructor of inheriting class.

        Params:
            database_path: Path to database file.
            database_dir: Path to directory containing database files.
            table_name: Name of table within database.
        """
        self.database_path = database_path
        self.table_name = table_name
        Path(database_dir).mkdir(parents=True, exist_ok=True)

    def execute_query(self, query: Union[str, List[str]]):
        """ Executes SQL query in database and returns final query's results.

        Params:
            query: SQL query or list of queries to interact with database.
        """
        if isinstance(query, str):
            query = [query]
        try:
            with closing(sqlite3.connect(self.database_path)) as con, con, \
                    closing(con.cursor()) as cur:
                for q in query:
                    cur.execute(q)
                return cur.fetchall()
        except Error as e:
            print(e)

    def table_exists(self, table_name: str = None):
        """ Returns true if table_name exists as a table in the database.

        Params:
            table_name: Name of table in database, not required if table_name is
                passed into super constructor.
        """
        if not table_name:
            table_name = self.table_name
        query = f"SELECT name FROM sqlite_master " \
                f"WHERE type='table' AND name='{table_name}'"
        result = self.execute_query(query)
        df = pd.DataFrame(result)
        if len(df) == 1:
            return True
        return False

    def get_all(self) -> pd.DataFrame:
        """ Returns table contents as dataframe. """
        query = f"PRAGMA table_info({self.table_name})"
        results = self.execute_query(query)
        column_names = [col[1] for col in results]
        query = f""" SELECT * FROM {self.table_name}"""
        result = self.execute_query(query)
        df = pd.DataFrame(result, columns=column_names)
        return df
