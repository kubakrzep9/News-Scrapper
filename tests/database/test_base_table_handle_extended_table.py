""" Validates 'BaseHandle' class. """

from typing import NamedTuple, List

import pytest
from news_scanner.database.table_handles.base_table_handle import (
    BaseTableHandle,
    TableConfig,
)
from tests.database.test_helper.util import (
    tear_down,
    compare_complex_extended_nt_obj_to_df
)
from tests.database.conftest import TEST_DB_DIR
from news_scanner.database.util import (
    extract_attrs,
    NAMED_TUPLE_DTYPE,
)

TEST_DATABASE_PATH = TEST_DB_DIR / "base_database.sqlite"


class MyTestDataObj(NamedTuple):
    attr_a: int = 1
    attr_b: str = "attr_b"


class MyTestObj(NamedTuple):
    extended_data: List[MyTestDataObj] = [MyTestDataObj()]
    attr1: float = 1.1
    attr2: str = "hello"


TEST_EXTENDED_DATA = [MyTestDataObj() for _ in range(3)]

TEST_OBJS_DICT = {
    1: MyTestObj(
        extended_data=TEST_EXTENDED_DATA,
        attr1=1.1,
        attr2="111"
    ),
    2: MyTestObj(
        extended_data=TEST_EXTENDED_DATA,
        attr1=2.2,
        attr2="222"
    ),
    3: MyTestObj(
        extended_data=TEST_EXTENDED_DATA,
        attr1=3.3,
        attr2="333"
    ),
}
PRIMARY_KEYS = [*TEST_OBJS_DICT.keys()]
TEST_OBJS = [*TEST_OBJS_DICT.values()]

EXTENDED_TABLE_INDEX = {
    "my_test_obj_extended_data": {
        1: [1, 2, 3],
        2: [4, 5, 6],
        3: [7, 8, 9],
    }
}


class MyTestHandle(BaseTableHandle):
    def __init__(self):
        print()
        tear_down()
        super().__init__(
            table_config=TableConfig(
                named_tuple_type=MyTestObj,
                allowed_namedtuples=[MyTestDataObj]
            ),
            db_dir=TEST_DB_DIR,
        )  # must be first


@pytest.fixture
def base_test_handle() -> MyTestHandle:
    """ Reused test object to test inheriting from 'BaseHandle'. """
    return MyTestHandle()


def test_table_exists_extended_data(base_test_handle: MyTestHandle):
    """ Ensures table existence in a database can be identified
    """
    assert base_test_handle.extended_table_handles_data  # assert extended_data is not empty
    assert base_test_handle.table_exists()
    for data_name, table_handle in base_test_handle.extended_table_handles_data.items():
        assert base_test_handle.table_exists(
            table_name=table_handle.table_config.table_name
        )

    assert not base_test_handle.table_exists("not_a_table")


def test_get_all(base_test_handle: BaseTableHandle):
    base_table_config = base_test_handle.table_handle_data.table_config
    base_table_name = base_table_config.table_name

    expected_table_columns = {}

    _, attr_pool_dtypes, extended_data = extract_attrs(
        complex_nt=base_table_config.named_tuple_type(),
        allowed_named_tuples=base_table_config.allowed_namedtuples,
        allowed_data_types=base_table_config.allowed_dtypes
    )
    expected_table_columns[base_table_name] = [*attr_pool_dtypes.keys()]

    for extended_data_name, extended_data_values in extended_data.items():
        _, _attr_pool_dtypes, _ = extract_attrs(
            complex_nt=extended_data_values[NAMED_TUPLE_DTYPE](),
            allowed_named_tuples=base_table_config.allowed_namedtuples,
            allowed_data_types=base_table_config.allowed_dtypes
        )
        extended_data_table_name = f"{base_table_name}_{extended_data_name}"
        expected_table_columns[extended_data_table_name] = [
            *_attr_pool_dtypes.keys(), base_table_config.primary_key
        ]

    table_data = base_test_handle.get_all()
    for table_name, table_df in table_data.items():
        df_cols = [*table_df.columns]
        assert df_cols == expected_table_columns[table_name]

        assert table_df.empty


def test_insert_and_get_all(base_test_handle: BaseTableHandle):
    """ Validating inserted data.

    Params:

    """
    index = PRIMARY_KEYS
    for test_obj, primary_key in zip(TEST_OBJS, index):
        base_test_handle.insert(
            named_tuple=test_obj,
            primary_key=primary_key,
        )
    table_data = base_test_handle.get_all()
    compare_complex_extended_nt_obj_to_df(
        base_table_handle=base_test_handle,
        test_objs=TEST_OBJS,
        table_data=table_data,
        index=index,
        extended_table_indexes=EXTENDED_TABLE_INDEX
    )


def test_remove(base_test_handle: BaseTableHandle):
    index = PRIMARY_KEYS
    for test_obj, primary_key in zip(TEST_OBJS, index):
        base_test_handle.insert(
            named_tuple=test_obj,
            primary_key=primary_key,
        )
    base_test_handle.remove()
    ext_indexes = EXTENDED_TABLE_INDEX.copy()

    for ext_table_name, pks in ext_indexes.items():
        del pks[index[-1]]

    table_data = base_test_handle.get_all()
    compare_complex_extended_nt_obj_to_df(
        base_table_handle=base_test_handle,
        test_objs=TEST_OBJS[:-1],
        table_data=table_data,
        index=index[:-1],
        extended_table_indexes=ext_indexes
    )
