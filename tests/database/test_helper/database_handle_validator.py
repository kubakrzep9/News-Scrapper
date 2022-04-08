
from news_scanner.database.database_handles.base_database_handle import BaseDatabaseHandle
from news_scanner.database.util import extract_attrs, get_namedtuple_name
from typing import List, NamedTuple, Dict
from tests.database.test_helper.util import compare_complex_nt_obj_to_df
from news_scanner.database.constants import PYTHON_TO_SQL_DTYPES
from random import randint

def _validate_database_init(
    database_handle: BaseDatabaseHandle,
    insert_data: List,
    allowed_namedtuples: List[type],
    allowed_dtypes: List = PYTHON_TO_SQL_DTYPES
):
    # Extracting expected table column names
    expected_table_columns = {}
    complex_nt = insert_data[0]
    for namedtuple in complex_nt:
        nt_name = get_namedtuple_name(namedtuple)
        _, attr_pool_dtypes = extract_attrs(
            complex_nt=namedtuple,
            allowed_data_types=allowed_dtypes,
            allowed_named_tuples=allowed_namedtuples
        )
        expected_table_columns[nt_name] = [*attr_pool_dtypes.keys()]

    # validating column names and that database initially is empty
    db_table_data = database_handle.get_all()
    for table_name, table_df in db_table_data.items():
        table_columns = ([*table_df.columns])
        assert table_columns == expected_table_columns[table_name]
        assert table_df.empty


def validate_database_handle(
    database_handle: BaseDatabaseHandle,
    insert_data: List,
    allowed_namedtuples: List[type],
    allowed_dtypes: List = PYTHON_TO_SQL_DTYPES
):
    assert len(insert_data) >= 3

    _validate_database_init(
        database_handle=database_handle,
        insert_data=insert_data,
        allowed_namedtuples=allowed_namedtuples,
        allowed_dtypes=allowed_dtypes
    )

    database_handle.insert(insert_data=insert_data)
    db_table_data = database_handle.get_all()


    # for _ in range(0, 100):
    #     rand_int = randint(1, len(insert_data))
    #     if rand_int == len(insert_data):
    #         print(rand_int)




