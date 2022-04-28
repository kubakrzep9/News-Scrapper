
from news_scanner.database.database_handles.base_database_handle import BaseDatabaseHandle
from news_scanner.database.util import extract_attrs, get_table_name2
from typing import List, NamedTuple, Tuple
from tests.database.test_helper.util import (
    compare_complex_nt_obj_to_df,
    extract_sub_complexnt,
    compare_complex_extended_nt_obj_to_df
)
from news_scanner.database.constants import (
    PYTHON_TO_SQL_DTYPES,
    DEF_PRIMARY_KEY,
    EXTENDED_DATA_PRIMARY_KEY
)
from random import randint, sample


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
        table_name = get_table_name2(namedtuple)
        _, attr_pool_dtypes, extended_data = extract_attrs(
            complex_nt=namedtuple,
            allowed_data_types=allowed_dtypes,
            allowed_named_tuples=allowed_namedtuples
        )
        expected_table_columns[table_name] = [*attr_pool_dtypes.keys()]

        if extended_data:
            for ext_data_name, ext_data_metadata in extended_data.items():
                ext_table_name = f"{table_name}_{ext_data_name}"
                ext_def_data = ext_data_metadata["named_tuple_dtype"]()
                _, attr_pool_dtypes, _ = extract_attrs(
                    complex_nt=ext_def_data,
                    allowed_data_types=allowed_dtypes,
                    allowed_named_tuples=allowed_namedtuples
                )
                expected_table_columns[ext_table_name] = [
                    *attr_pool_dtypes.keys(),
                    DEF_PRIMARY_KEY  # base table primary key
                ]

    # validating column names and that database initially is empty
    db_table_data = database_handle.get_all()
    for base_table_name, all_table_data in db_table_data.items():
        for table_name, table_df in all_table_data.items():
            if table_name == base_table_name:
                assert table_df.index.name == DEF_PRIMARY_KEY   # assumes default primary key
            else:
                assert table_df.index.name == EXTENDED_DATA_PRIMARY_KEY  # assumes default primary key
            table_columns = ([*table_df.columns])
            assert table_columns == expected_table_columns[table_name]
            assert table_df.empty


def _validate_database_state(
    database_handle: BaseDatabaseHandle,
    expected_complex_nts: List,
    expected_index: List[int],
    allowed_namedtuples: List[type],
    allowed_dtypes: List[type]
):
    assert len(expected_complex_nts) == len(expected_index)

    sub_complexnt_data = extract_sub_complexnt(
        complex_nts=expected_complex_nts,
        index_list=expected_index
    )

    db_table_data = database_handle.get_all()

    for base_table_name, table_data in db_table_data.items():
        if len(table_data.keys()) > 1:
            complexnt_ext_objs = [*sub_complexnt_data[base_table_name].values()]
            print("complexnt_ext")
            compare_complex_extended_nt_obj_to_df(
                base_table_handle=database_handle.table_handles[base_table_name],
                test_objs=complexnt_ext_objs,
                table_data=table_data,
                index=expected_index
            )
        else:
            print("complexnt")
            compare_complex_nt_obj_to_df(
                complex_nts=sub_complexnt_data[base_table_name],
                df=table_data[base_table_name],
                allowed_named_tuples=allowed_namedtuples,
                allowed_data_types=allowed_dtypes
            )

    expected_next_primary_key = max(expected_index) + 1
    assert database_handle.get_next_primary_key() == expected_next_primary_key


def _database_random_remove(
    database_handle: BaseDatabaseHandle,
    current_data: List[NamedTuple],
    current_index: List[int],
):
    num_rows_to_delete = randint(1, 2)
    delete_rows = set(sample(range(1, len(current_data)), num_rows_to_delete))
    _current_index = set(current_index) - delete_rows

    _current_data = []
    for i in _current_index:
        _current_data.append(current_data[i - 1])

    database_handle.remove(primary_keys=[*delete_rows])

    return [*_current_index], _current_data


def _database_random_insert(
    database_handle: BaseDatabaseHandle,
    current_data: List[NamedTuple],
    current_index: List[int],
) -> Tuple[List[int], List[NamedTuple]]:
    num_rows_to_insert = randint(1, 2)
    insert_rows_index = set(sample(range(1, len(current_data)), num_rows_to_insert))

    _current_index = current_index.copy()
    _current_data = current_data.copy()
    insert_data = []

    for i in insert_rows_index:
        insert_data.append(current_data[i - 1])
        _current_data.append(current_data[i - 1])
        _current_index.append(max(_current_index)+1)

    database_handle.insert(insert_data=insert_data)

    return [*_current_index], _current_data


def validate_database_handle(
    database_handle: BaseDatabaseHandle,
    insert_data: List,
    allowed_namedtuples: List[type],
    allowed_dtypes: List = PYTHON_TO_SQL_DTYPES
):
    assert len(insert_data) >= 3

    # print()
    # for data in insert_data:
    #     print(data)

    _validate_database_init(
        database_handle=database_handle,
        insert_data=insert_data,
        allowed_namedtuples=allowed_namedtuples,
        allowed_dtypes=allowed_dtypes
    )

    # insert data
    database_handle.insert(insert_data=insert_data)
    current_index = [i for i in range(1, len(insert_data)+1)]
    current_data = insert_data

    _validate_database_state(
        database_handle=database_handle,
        expected_complex_nts=current_data,
        expected_index=current_index,
        allowed_namedtuples=allowed_namedtuples,
        allowed_dtypes=allowed_dtypes,
    )
    #
    # delete random rows
    current_index, current_data = _database_random_remove(
        database_handle=database_handle,
        current_data=current_data,
        current_index=current_index
    )
    _validate_database_state(
        database_handle=database_handle,
        expected_complex_nts=current_data,
        expected_index=current_index,
        allowed_namedtuples=allowed_namedtuples,
        allowed_dtypes=allowed_dtypes
    )
    #
    # # insert random rows
    # current_index, current_data = _database_random_insert(
    #     database_handle=database_handle,
    #     current_data=current_data,
    #     current_index=current_index
    # )
    # _validate_database_state(
    #     database_handle=database_handle,
    #     expected_complex_nts=current_data,
    #     expected_index=current_index,
    #     allowed_namedtuples=allowed_namedtuples
    # )

