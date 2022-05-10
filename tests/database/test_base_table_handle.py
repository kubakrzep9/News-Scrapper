""" Validates 'BaseHandle' class. """

from typing import NamedTuple, List, Dict

import pytest
from pathlib import Path
from news_scanner.database.table_handles.base_table_handle import (
    BaseTableHandle,
    DEF_PRIMARY_KEY,
    TableConfig,
    generate_name
)
from tests.database.test_helper.util import tear_down, compare_complex_nt_obj_to_df
from tests.database.conftest import TEST_DB_DIR
from tests.database.test_helper.table_handle_validator import \
    validate_table_handle


class MyTestObj(NamedTuple):
    attr1: int = 0
    attr2: float = 0.0
    attr3: str = "hello"


TEST_OBJS_DICT = {
    1: MyTestObj(1, 1.1, "111"),
    2: MyTestObj(2, 2.2, "222"),
    3: MyTestObj(3, 3.3, "333")
}
PRIMARY_KEYS = [*TEST_OBJS_DICT.keys()]
TEST_OBJS = [*TEST_OBJS_DICT.values()]


class MyTestHandle(BaseTableHandle):
    def __init__(self):
        tear_down()
        super().__init__(
            table_config=TableConfig(
                named_tuple_type=MyTestObj,
            ),
            db_dir=TEST_DB_DIR,
        )


@pytest.fixture
def base_test_handle() -> MyTestHandle:
    """ Reused test object to test inheriting from 'BaseHandle'. """
    return MyTestHandle()


def test_db_file_creation(base_test_handle: MyTestHandle):
    """ Ensures db file and table is created. """
    assert Path(base_test_handle.database_path).is_file()
    tear_down()


def test_table_exists(base_test_handle: BaseTableHandle):
    """ Ensures table existence in a database can be identified"""
    assert bool(base_test_handle.extended_table_handles_data) is False  # assert extended_data is empty
    assert base_test_handle.table_exists()
    assert not base_test_handle.table_exists("not_a_table")
    tear_down()


def test_get_all(base_test_handle: BaseTableHandle):
    """ Validating columns and number of rows.

    Params:

    """
    expected_num_cols = 3
    expected_col_names = [*MyTestObj()._fields]
    expected_num_rows = len(TEST_OBJS)

    for test_obj, primary_key in zip(
        TEST_OBJS, PRIMARY_KEYS
    ):
        base_test_handle.insert(
            named_tuple=test_obj,
            primary_key=primary_key,
        )
    table_data = base_test_handle.get_all()
    df = table_data[base_test_handle.table_handle_data.table_config.table_name]
    num_rows, num_cols = df.shape
    assert num_rows == expected_num_rows
    assert num_cols == expected_num_cols
    assert [*df.columns].sort() == expected_col_names.sort()
    assert df.index.name == DEF_PRIMARY_KEY
    tear_down()


def test_insert(base_test_handle: BaseTableHandle):
    """ Validating inserted data.

    Params:

    """
    for test_obj, primary_key in zip(TEST_OBJS, PRIMARY_KEYS):
        base_test_handle.insert(
            named_tuple=test_obj,
            primary_key=primary_key,
        )
    table_data = base_test_handle.get_all()
    df = table_data[base_test_handle.table_handle_data.table_config.table_name]

    test_objs = {}
    for i in range(1, len(TEST_OBJS)+1):
        test_objs[i] = TEST_OBJS[i-1]

    compare_complex_nt_obj_to_df(
        complex_nts=test_objs,
        df=df,
    )


@pytest.mark.parametrize(
    "test_objs, primary_keys, expected_last_pk", [
        ([], [], 0),
        (TEST_OBJS, PRIMARY_KEYS, len(TEST_OBJS))
    ]
)
def test_get_last_primary(
    test_objs: List,
    primary_keys: List,
    expected_last_pk: int,
    base_test_handle: MyTestHandle
):
    """ Validating inserted data.

    Params:

    """
    for test_obj, primary_key in zip(
            test_objs, primary_keys
    ):
        base_test_handle.insert(
            named_tuple=test_obj,
            primary_key=primary_key,
        )
    last_primary_key = base_test_handle.get_last_primary_key()
    assert last_primary_key == expected_last_pk


def test_remove(base_test_handle: MyTestHandle):
    """

    Params:

    """
    test_objs_dict = TEST_OBJS_DICT.copy()
    for test_obj, primary_key in zip(
            TEST_OBJS, PRIMARY_KEYS
    ):
        base_test_handle.insert(
            named_tuple=test_obj,
            primary_key=primary_key,
        )
    table_data = base_test_handle.get_all()
    df = table_data[base_test_handle.table_handle_data.table_config.table_name]
    compare_complex_nt_obj_to_df(
        complex_nts=test_objs_dict,
        df=df
    )
    base_test_handle.remove()
    del test_objs_dict[PRIMARY_KEYS[-1]]
    table_data = base_test_handle.get_all()
    df = table_data[base_test_handle.table_handle_data.table_config.table_name]
    compare_complex_nt_obj_to_df(
        complex_nts=test_objs_dict,
        df=df
    )


def test_validate_table_handle(base_test_handle: MyTestHandle):
    expected_name = "my_test_obj"
    validate_table_handle(
        table_handle=base_test_handle,
        insert_list=TEST_OBJS,
        expected_db_file_name=expected_name+".sqlite",
        expected_table_name=expected_name
    )


def test_generate_name():
    db_file_type = ".sqlite"
    expected_name = "my_test_obj"
    expected_db_file_name = expected_name+db_file_type

    expected_table_data = TableConfig(
        named_tuple_type=MyTestObj,
        table_name=expected_name
    )

    db_file_name, table_data = generate_name(
        db_file_name=None,
        table_config=TableConfig(named_tuple_type=MyTestObj)
    )
    assert db_file_name == expected_db_file_name
    assert table_data == expected_table_data
