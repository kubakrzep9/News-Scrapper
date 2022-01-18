""" Validates 'BaseHandle' class. """

from typing import Tuple
import pytest
from pathlib import Path
from news_scanner.database_handles.base_handle import BaseHandle
from tests.util import tear_down
from tests.constants import TEST_DATABASE_DIR

_TEST_DATABASE_PATH = TEST_DATABASE_DIR / "base_database.sqlite"


@pytest.fixture
def table_name_and_query() -> Tuple[str, str]:
    """ Returns tuple of table name and query to build table in database. """
    table_name = "test_table"
    query = f"CREATE TABLE {table_name} ([id] INTEGER PRIMARY KEY, " \
            f"[value] INTEGER)"
    return table_name, query


def test_db_dir_creation():
    """ Ensures db directory to house db files is created. """
    db_handle = BaseHandle(
        database_path=_TEST_DATABASE_PATH,
        database_dir=TEST_DATABASE_DIR,
    )
    db_dir_exists = Path(TEST_DATABASE_DIR).is_dir()
    assert db_dir_exists is True
    tear_down()


def test_db_file_creation(table_name_and_query: str):
    """ Ensures db file and table is created.

    Params:
        table_name_and_query: Tuple of table name and query to build the
            table in the database.
    """
    table_name, query = table_name_and_query
    db_handle = BaseHandle(
        database_path=_TEST_DATABASE_PATH,
        database_dir=TEST_DATABASE_DIR,
        table_name=table_name
    )
    db_handle.execute_query(query)
    db_exists = Path(_TEST_DATABASE_PATH).is_file()
    assert db_exists is True
    tear_down()


def test_table_exists(table_name_and_query: str):
    """ Ensures table existence in a database can be identified

    Params:
        table_name_and_query: Tuple of table name and query to build the
            table in the database.
    """
    table_name, query = table_name_and_query
    db_handle = BaseHandle(
        database_path=_TEST_DATABASE_PATH,
        database_dir=TEST_DATABASE_DIR,
        table_name=table_name
    )
    db_handle.execute_query(query)
    assert db_handle.table_exists()
    assert not db_handle.table_exists("not_a_table")
    tear_down()


def test_get_all(table_name_and_query: str):
    """ Ensures all table content is retrieved from database.

    Params:
        table_name_and_query: Tuple of table name and query to build the
            table in the database.
    """
    table_name, build_query = table_name_and_query
    db_handle = BaseHandle(
        database_path=_TEST_DATABASE_PATH,
        database_dir=TEST_DATABASE_DIR,
        table_name=table_name
    )
    db_handle.execute_query(build_query)
    db_handle.execute_query(
        f"INSERT INTO {table_name} (id, value) VALUES(1,1)"
    )
    db_handle.execute_query(
        f"INSERT INTO {table_name} (id, value) VALUES(2,2)"
    )
    df = db_handle.get_all()
    assert len(df) == 2
    tear_down()
