""" Class to house common database functionality. """

import sqlite3
from sqlite3 import Error
from pathlib import Path
from contextlib import closing
from typing import List, Union
from news_scanner.database.constants import DB_DIR


class BaseHandle:
    """ Class to interface database file. """

    def __init__(
        self,
        db_file_name: str,
        db_dir: Path = DB_DIR,
    ):
        """ Creates a directory if one doesn't exist to store the database.

        Note: Must be executed first in constructor of inheriting class.

        Params:
            database_path: Path to database file.
            database_dir: Path to directory containing database files.
        """
        self.database_path = db_dir / db_file_name
        db_dir.mkdir(exist_ok=True)

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
