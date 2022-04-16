""" Validates 'BaseHandle' class. """

from typing import NamedTuple, List, Dict

import pandas as pd
import pytest
from pathlib import Path
from news_scanner.database.table_handles.base_table_handle import (
    BaseTableHandle,
    DEF_PRIMARY_KEY,
    TableConfig,
    ForeignkeyConfig,
    generate_name
)
from tests.database.test_helper.util import tear_down, compare_complex_nt_obj_to_df
from tests.database.conftest import TEST_DB_DIR
from tests.database.test_helper.table_handle_validator import \
    validate_table_handle
from news_scanner.database.util import (
    extract_attrs,
    get_name,
    NAMED_TUPLE_DTYPE,
    EXTENDED_DATA_VALUE
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

    TODO:
        - Validate main and extended tables
    """
    assert base_test_handle.extended_table_handles_data  # assert extended_data is not empty
    assert base_test_handle.table_exists()
    for data_name, table_handle in base_test_handle.extended_table_handles_data.items():
        assert base_test_handle.table_exists(
            table_name=table_handle.table_config.table_name
        )

    assert not base_test_handle.table_exists("not_a_table")


def test_insert(base_test_handle: BaseTableHandle):
    """ Validating inserted data.

    Params:

    """
    base_table_name = base_test_handle.table_handle_data.table_config.table_name
    base_table_primary_key = base_test_handle.table_handle_data.table_config.primary_key
    allowed_named_tuples = base_test_handle.table_handle_data.table_config.allowed_namedtuples

    for test_obj, primary_key in zip(TEST_OBJS, PRIMARY_KEYS):
        base_test_handle.insert(
            named_tuple=test_obj,
            primary_key=primary_key,
        )
    table_data = base_test_handle.get_all()

    # validating base table
    test_objs = {}
    for i in range(1, len(TEST_OBJS)+1):
        test_objs[i] = TEST_OBJS[i-1]
    compare_complex_nt_obj_to_df(
        complex_nts=test_objs,
        df=table_data[base_table_name],
        allowed_named_tuples=allowed_named_tuples,
        index_name=base_table_primary_key
    )

    # validating extended tables

    # creating python objects to generate dataframe to validate
    # the database against.
    all_extended_data_fks = {}
    all_extended_data_objs = {}
    init_all_ext_data_objs = False
    primary_key = 1
    for complex_nt in TEST_OBJS:
        _, _, extended_data = extract_attrs(
            complex_nt=complex_nt,
            allowed_data_types=[*base_test_handle.table_handle_data.table_config.allowed_dtypes.keys()],
            allowed_named_tuples=allowed_named_tuples
        )
        # init struct to hold ext data
        if not init_all_ext_data_objs:
            for data_name, _ in extended_data.items():
                all_extended_data_objs[data_name] = {}
                all_extended_data_fks[data_name] = {base_table_primary_key: []}
            init_all_ext_data_objs = True
        # extract ext data
        for data_name, meta_data in extended_data.items():
            # print(data_name)
            # print(meta_data["value"])
            # print(base_test_handle.extended_table_handles_data[data_name])
            # all_extended_data_objs
            for extended_data_value in meta_data["value"]:
                pk = len(all_extended_data_objs[data_name]) + 1
                all_extended_data_objs[data_name][pk] = extended_data_value
                all_extended_data_fks[data_name][base_table_primary_key].append(primary_key)
        primary_key += 1

    for data_name in all_extended_data_fks:
        extended_table_name = f"{base_table_name}_{data_name}"
        compare_complex_nt_obj_to_df(
            complex_nts=all_extended_data_objs[data_name],
            df=table_data[extended_table_name],
            foreign_keys=all_extended_data_fks[data_name],
            index_name=base_test_handle.extended_table_handles_data[data_name].table_config.primary_key
        )