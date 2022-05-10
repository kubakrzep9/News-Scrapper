
from news_scanner.database.database_handles.base_database_handle import BaseDatabaseHandle
from news_scanner.database.util import extract_attrs, get_table_name2
from typing import List, NamedTuple, Tuple, Dict
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

_COPY_PK = "copy_pk"
_INSERT_PK = "insert_pk"


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
    original_insert_data: List[NamedTuple],
    current_data: List[NamedTuple],
    current_index: List[int],
) -> Tuple[List[int], List[NamedTuple], List[Dict]]:
    insert_index_map = []
    num_rows_to_insert = randint(1, 2)
    insert_rows_index = set(sample(range(1, len(current_data)), num_rows_to_insert))

    _current_index = current_index.copy()
    _current_data = current_data.copy()
    to_insert_data = []

    for copy_pk in insert_rows_index:
        insert_pk = max(_current_index)+1
        to_insert_data.append(original_insert_data[copy_pk - 1])
        _current_data.append(original_insert_data[copy_pk - 1])
        _current_index.append(insert_pk)
        insert_index_map.append({
            _COPY_PK: copy_pk,
            _INSERT_PK: insert_pk
        })
        print(f"Inserting pk {copy_pk} data at pk {insert_pk}")
    database_handle.insert(insert_data=to_insert_data)

    return [*_current_index], _current_data, insert_index_map


def _init_extended_table_indexes(
    insert_data: List
) -> Dict[str, Dict[int, List[int]]]:
    """ Returns a dict where the key corresponds to a table_name and the
        value is a dict. Each dict value contains keys which represent
        primary keys for the base_table and the values are lists of the
        corresponding extended_primary_keys.
    """
    assert len(insert_data) >= 3

    # initializing extended table indexes
    all_sub_complexnt_data = extract_sub_complexnt(insert_data)
    extended_table_indexes = {}
    for base_table_name, sub_complexnt_data in all_sub_complexnt_data.items():
        complex_nt = sub_complexnt_data[1]
        for attr_name, attr_value in zip(complex_nt._fields, complex_nt):
            ext_table_name = f"{base_table_name}_{attr_name}"
            if type(attr_value) == list:
                extended_table_indexes[ext_table_name] = {}
        # creating expected extended table indexes
        for pk, complex_nt in sub_complexnt_data.items():
            for attr_name, attr_value in zip(complex_nt._fields, complex_nt):
                first_root_ext_pk = False
                if type(attr_value) == list:
                    ext_table_name = f"{base_table_name}_{attr_name}"

                    # adding pk to track ext_pks
                    if pk not in extended_table_indexes[ext_table_name].keys():
                        extended_table_indexes[ext_table_name][pk] = []
                        # first pk to be added for ext_table_name starts off the ext_id.
                        if len(extended_table_indexes[ext_table_name].keys()) == 1:
                            first_root_ext_pk = True

                    for _ in attr_value:
                        # calculating ext_pk
                        if first_root_ext_pk:
                            ext_pk = 1
                            first_root_ext_pk = False
                        elif not extended_table_indexes[ext_table_name][pk]:
                            ext_pk = extended_table_indexes[ext_table_name][pk-1][-1] + 1
                        else:
                            ext_pk = extended_table_indexes[ext_table_name][pk][-1] + 1

                        extended_table_indexes[ext_table_name][pk].append(ext_pk)
    return extended_table_indexes


def _validate_database_state(
    database_handle: BaseDatabaseHandle,
    expected_complex_nts: List,
    expected_index: List[int],
    allowed_namedtuples: List[type],
    allowed_dtypes: List[type],
    extended_table_indexes: Dict[str, Dict[int, List[int]]] = {}
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
            compare_complex_extended_nt_obj_to_df(
                base_table_handle=database_handle.table_handles[base_table_name],
                test_objs=complexnt_ext_objs,
                table_data=table_data,
                index=expected_index,
                extended_table_indexes=extended_table_indexes
            )
        else:
            compare_complex_nt_obj_to_df(
                complex_nts=sub_complexnt_data[base_table_name],
                df=table_data[base_table_name],
                allowed_named_tuples=allowed_namedtuples,
                allowed_data_types=allowed_dtypes
            )

    expected_next_primary_key = max(expected_index) + 1
    assert database_handle.get_next_primary_key() == expected_next_primary_key


def _get_next_ext_id(extended_table_indexes):
    next_ext_ids = {}

    for ext_table_name, pk_dict in extended_table_indexes.items():
        base_pks = [*pk_dict.keys()]
        if not base_pks:
            next_ext_id = 1
        else:
            last_base_pk = max(base_pks)
            ext_indexes = pk_dict[last_base_pk]
            next_ext_id = max(ext_indexes) + 1

        next_ext_ids[ext_table_name] = next_ext_id

    return next_ext_ids


def _get_current_extended_indexes(
    current_index,
    insert_index_map,
    extended_table_indexes
):
    next_ext_ids = _get_next_ext_id(extended_table_indexes)
    current_extended_table_indexes = extended_table_indexes.copy()

    # adding new ext_ids
    for index_map in insert_index_map:
        copy_i = index_map[_COPY_PK]
        insert_i = index_map[_INSERT_PK]
        for ext_table_name, next_ext_id in next_ext_ids.items():
            num_ext_ids = len(extended_table_indexes[ext_table_name][copy_i])
            current_extended_table_indexes[ext_table_name][insert_i] = []
            for _ in range(num_ext_ids):
                current_extended_table_indexes[ext_table_name][insert_i].append(next_ext_id)
                next_ext_id += 1
            next_ext_ids[ext_table_name] = next_ext_id

    # removing ext_ids not in current_index
    _to_remove = {}
    for ext_table_name, pk_index in current_extended_table_indexes.items():
        _to_remove[ext_table_name] = []
        for pk in pk_index:
            if pk not in current_index:
                _to_remove[ext_table_name].append(pk)
    for ext_table_name in current_extended_table_indexes.keys():
        for pk in _to_remove[ext_table_name]:
            del current_extended_table_indexes[ext_table_name][pk]

    return current_extended_table_indexes


def validate_database_handle(
    database_handle: BaseDatabaseHandle,
    insert_data: List,
    allowed_namedtuples: List[type],
    allowed_dtypes: List = PYTHON_TO_SQL_DTYPES
):

    extended_table_indexes = _init_extended_table_indexes(insert_data)
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
        extended_table_indexes=extended_table_indexes
    )

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
        allowed_dtypes=allowed_dtypes,
        extended_table_indexes=extended_table_indexes
    )

    # insert random rows
    current_index, current_data, insert_index_map = _database_random_insert(
        database_handle=database_handle,
        original_insert_data=insert_data,
        current_data=current_data,
        current_index=current_index
    )

    current_extended_indexes = _get_current_extended_indexes(
        current_index=current_index,
        insert_index_map=insert_index_map,
        extended_table_indexes=extended_table_indexes
    )

    _validate_database_state(
        database_handle=database_handle,
        expected_complex_nts=current_data,
        expected_index=current_index,
        allowed_namedtuples=allowed_namedtuples,
        allowed_dtypes=allowed_dtypes,
        extended_table_indexes=current_extended_indexes  # needs to take removed rows into account
    )
