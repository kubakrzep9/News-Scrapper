""" """

import pytest
from pathlib import Path
from news_scanner.database.base_handle import BaseHandle
from tests.database.conftest import TEST_DB_DIR
from tests.database.test_helper.util import remove_dir_tree


@pytest.fixture
def base_handle() -> BaseHandle:
    remove_dir_tree(TEST_DB_DIR)
    return BaseHandle(
        db_file_name="basehandle_db.sqlite",
        db_dir=TEST_DB_DIR
    )


def test_db_dir_creation(base_handle: BaseHandle):
    """ Ensures db directory to house db files is created. """
    assert Path(TEST_DB_DIR).is_dir() is True


# def test_execute_query_missing_db_file(base_handle: BaseHandle):
#     query = "query"
#     with pytest.raises(ValueError) as exec_info:
#         base_handle.execute_query(query)
#
#     assert MISSING_DB_FILE in str(exec_info.value)
