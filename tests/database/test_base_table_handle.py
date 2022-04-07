""" Validates 'BaseHandle' class. """

from typing import NamedTuple, List, Dict

import pandas as pd
import pytest
from pathlib import Path
from news_scanner.database.table_handles.base_table_handle import (
    BaseTableHandle,
    DEF_PRIMARY_KEY,
    TableData,
    ForeignkeyConfig,
    generate_name
)
from tests.database.test_helper.util import tear_down, compare_complex_nt_obj_to_df
from tests.database.conftest import TEST_DB_DIR
from tests.database.test_helper.table_handle_validator import \
    validate_table_handle

TEST_DATABASE_PATH = TEST_DB_DIR / "base_database.sqlite"

FKEY1 = "fkey1"
FKEY2 = "fkey2"
FKEYS_LIST = [{FKEY1: 2, FKEY2: 3}, {FKEY1: 4, FKEY2: 5}, {FKEY1: 6, FKEY2: 7}]
EMPTY_FKEYS_LIST = [{} for _ in range(len(FKEYS_LIST))]


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
    def __init__(self, foreign_keys: List[ForeignkeyConfig] = []):
        tear_down()
        table_name = "my_test_obj"
        if len(foreign_keys) > 0:
            table_name += "_fk"
        super().__init__(
            table_data=TableData(
                table_name=table_name,
                named_tuple_type=MyTestObj,
                foreign_keys=foreign_keys
            ),
            db_dir=TEST_DB_DIR,
        )  # must be first


@pytest.fixture
def base_test_handle() -> MyTestHandle:
    """ Reused test object to test inheriting from 'BaseHandle'. """
    return MyTestHandle()


@pytest.fixture
def base_test_handle_fk() -> MyTestHandle:
    """ Reused test object to test inheriting from 'BaseHandle'. """
    foreign_keys = [
        ForeignkeyConfig(
            foreign_key=FKEY1,
            reference_table="table1",
            reference_table_primary_key="pkey1"
        ),
        ForeignkeyConfig(
            foreign_key=FKEY2,
            reference_table="table2",
            reference_table_primary_key="pkey2"
        )
    ]
    return MyTestHandle(foreign_keys=foreign_keys)


def test_db_file_creation(base_test_handle: MyTestHandle):
    """ Ensures db file and table is created. """
    assert Path(base_test_handle.database_path).is_file()
    tear_down()


@pytest.mark.parametrize(
    "handle_fixture", ["base_test_handle_fk", "base_test_handle"]
)
def test_table_exists(handle_fixture: BaseTableHandle, request):
    """ Ensures table existence in a database can be identified"""
    handle = request.getfixturevalue(handle_fixture)
    assert handle.table_exists()
    assert not handle.table_exists("not_a_table")
    tear_down()


@pytest.mark.parametrize(
    "handle_fixture, all_foreign_keys, expected_num_cols", [
         ("base_test_handle_fk", FKEYS_LIST, 5),
         ("base_test_handle", EMPTY_FKEYS_LIST, 3)
    ]
)
def test_get_all(
    handle_fixture: BaseTableHandle,
    all_foreign_keys: List[Dict[str, int]],
    expected_num_cols: int,
    request
):
    """ Validating columns and number of rows.

    Params:

    """
    handle = request.getfixturevalue(handle_fixture)
    fkeys = [fk_config.foreign_key for fk_config in handle.table_data.foreign_keys]
    expected_col_names = [*MyTestObj()._fields] + fkeys
    expected_num_rows = len(TEST_OBJS)

    for test_obj, primary_key, foreign_keys in zip(
        TEST_OBJS, PRIMARY_KEYS, all_foreign_keys
    ):
        handle.insert(
            named_tuple=test_obj,
            primary_key=primary_key,
            foreign_keys=foreign_keys
        )
    df = handle.get_all()
    num_rows, num_cols = df.shape
    assert num_rows == expected_num_rows
    assert num_cols == expected_num_cols
    assert [*df.columns].sort() == expected_col_names.sort()
    assert df.index.name == DEF_PRIMARY_KEY
    tear_down()


@pytest.mark.parametrize(
    "handle_fixture, all_foreign_keys", [
         ("base_test_handle_fk", FKEYS_LIST),
         ("base_test_handle", EMPTY_FKEYS_LIST)
    ]
)
def test_insert(
    handle_fixture: BaseTableHandle,
    all_foreign_keys: List[Dict[str, int]],
    request
):
    """ Validating inserted data.

    Params:

    """
    handle = request.getfixturevalue(handle_fixture)
    fkeys = [fk_config.foreign_key for fk_config in handle.table_data.foreign_keys]

    for test_obj, primary_key, foreign_keys in zip(
            TEST_OBJS, PRIMARY_KEYS, all_foreign_keys
    ):
        handle.insert(
            named_tuple=test_obj,
            primary_key=primary_key,
            foreign_keys=foreign_keys
        )
    df = handle.get_all()
    if handle.table_data.foreign_keys:
        fks_df = df[fkeys]
        expected_fks = pd.DataFrame(FKEYS_LIST)
        expected_fks[DEF_PRIMARY_KEY] = [i for i in range(1, len(TEST_OBJS)+1)]
        expected_fks = expected_fks.set_index(DEF_PRIMARY_KEY)
        assert fks_df.equals(expected_fks)
        df = df.drop(fkeys, axis=1)

    test_objs = {}
    for i in range(1, len(TEST_OBJS)+1):
        test_objs[i] = TEST_OBJS[i-1]

    compare_complex_nt_obj_to_df(
        complex_nts=test_objs,
        df=df,
    )


def test_get_last_primary(base_test_handle: MyTestHandle):
    """ Validating inserted data.

    Params:

    """
    for test_obj, primary_key in zip(
            TEST_OBJS, PRIMARY_KEYS
    ):
        base_test_handle.insert(
            named_tuple=test_obj,
            primary_key=primary_key,
        )
    last_primary_key = base_test_handle.get_last_primary_key()
    assert last_primary_key == len(TEST_OBJS)


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
    df = base_test_handle.get_all()
    compare_complex_nt_obj_to_df(
        complex_nts=test_objs_dict,
        df=df
    )
    base_test_handle.remove()
    del test_objs_dict[PRIMARY_KEYS[-1]]
    df = base_test_handle.get_all()
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

    expected_table_data = TableData(
        named_tuple_type=MyTestObj,
        table_name=expected_name
    )

    db_file_name, table_data = generate_name(
        db_file_name=None,
        table_data=TableData(named_tuple_type=MyTestObj)
    )
    assert db_file_name == expected_db_file_name
    assert table_data == expected_table_data
