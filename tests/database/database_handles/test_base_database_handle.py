""" Validates 'BaseDatabaseHandle' class. """

import pytest
from typing import NamedTuple, List
from news_scanner.database.database_handles.base_database_handle import (
    BaseDatabaseHandle,
    DBObjectConfig,
    PYTHON_TO_SQL_DTYPES
)
from tests.database.conftest import (
    TEST_DB_DIR,
    NamedTuple1,
    NamedTuple2,
    ALLOWED_NAMED_TUPLES,
)
from tests.database.test_helper.util import tear_down, compare_complex_nt_obj_to_df, extract_sub_complexnt, compare_complex_extended_nt_obj_to_df
from mock import patch


class NamedTuple4(NamedTuple):
    attr1: str = "attr1"
    extended_data: List[NamedTuple1] = [NamedTuple1()]


class ComplexNT_ExtData(NamedTuple):
    named_tuple4: NamedTuple4 = NamedTuple4()
    named_tuple2: NamedTuple2 = NamedTuple2()


_ALLOWED_NAMED_TUPLES = ALLOWED_NAMED_TUPLES.copy()
_ALLOWED_NAMED_TUPLES.append(NamedTuple4)
ALLOWED_DATA_TYPES = PYTHON_TO_SQL_DTYPES.copy()


class ComplexNTHandle(BaseDatabaseHandle):
    def __init__(self):
        tear_down()
        super().__init__(
            db_object_config=DBObjectConfig(
                complex_nt_type=ComplexNT_ExtData,
                allowed_namedtuples=_ALLOWED_NAMED_TUPLES,
            ),
            db_dir=TEST_DB_DIR
        )


@pytest.fixture
def complexnt_ext_testobjs() -> List[ComplexNT_ExtData]:
    num_test_objs = 3
    test_objs = []
    for i in range(1, num_test_objs+1):
        test_objs.append(
            ComplexNT_ExtData(
                named_tuple4=NamedTuple4(),
                named_tuple2=NamedTuple2()
            )
        )
    return test_objs


@pytest.fixture
def complexnt_handle() -> ComplexNTHandle:
    """ Reused test object inheriting"""
    return ComplexNTHandle()


def test_init(complexnt_handle):
    # ensure db file is created
    assert complexnt_handle.database_path.is_file()

    # ensure default filename contains object name.
    path_tokens = str(complexnt_handle.database_path).split("/")
    assert ComplexNT_ExtData.__name__ in path_tokens[-1]

    db_table_data = complexnt_handle.get_all()

    # validate column names, index, and no data
    for base_table_name, all_table_data in db_table_data.items():
        # validate base table
        base_table_handle = complexnt_handle.table_handles[base_table_name]
        base_table_handle_data = base_table_handle.table_handle_data
        assert base_table_name in all_table_data.keys()
        base_table_df = db_table_data[base_table_name][base_table_name]
        assert base_table_df.empty
        column_names = [*base_table_df.columns]
        expected_column_names = [*base_table_handle_data.attr_dtypes.keys()]
        assert column_names == expected_column_names
        assert base_table_df.index.name == base_table_handle_data.table_config.primary_key

        # validate extended table
        ext_table_handles_data = base_table_handle.extended_table_handles_data
        assert len(all_table_data)-1 == len(ext_table_handles_data)
        for ext_data_name, ext_table_handle_data in ext_table_handles_data.items():
            extended_table_name = f"{base_table_name}_{ext_data_name}"
            assert extended_table_name in all_table_data.keys()
            ext_table_df = all_table_data[extended_table_name]
            assert ext_table_df.empty
            column_names = [*ext_table_df.columns]
            expected_column_names = [*ext_table_handle_data.attr_dtypes.keys()]
            for f_key in ext_table_handle_data.foreign_keys:
                expected_column_names.append(f_key.foreign_key)
            assert expected_column_names == column_names
            assert ext_table_df.index.name == ext_table_handle_data.table_config.primary_key


def test_get_next_primary_key(complexnt_handle):
    """ Ensures the next primary key is 1 when the database is empty.

    Primary key progression validated in 'test_insert'.

    """
    next_pk = complexnt_handle.get_next_primary_key()
    assert next_pk is 1


@pytest.mark.parametrize(
    "num_insert", [0, 1, 3]
)
def test_insert_and_get_all(complexnt_handle, complexnt_ext_testobjs, num_insert):
    print()
    complex_nts = {}
    i = 1
    for test_obj in complexnt_ext_testobjs[:num_insert]:
        complexnt_handle.insert(insert_data=test_obj)
        complex_nts[i] = test_obj
        i += 1

    db_table_data = complexnt_handle.get_all()

    # ensure db tables are empty
    if num_insert == 0:
        for base_table_name, all_table_data in db_table_data.items():
            for table_name, all_table_data in all_table_data.items():
                assert all_table_data.empty

    # ensure db tables contain expected values
    else:
        all_sub_complexnt_data = extract_sub_complexnt(
            complex_nts=complexnt_ext_testobjs[:num_insert]
        )
        for base_table_name, all_table_data in db_table_data.items():
            if len(all_table_data.keys()) > 1:
                complexnt_ext_objs = [*all_sub_complexnt_data[base_table_name].values()]
                index = [*all_sub_complexnt_data[base_table_name].keys()]
                compare_complex_extended_nt_obj_to_df(
                    base_table_handle=complexnt_handle.table_handles[base_table_name],
                    test_objs=complexnt_ext_objs,
                    table_data=all_table_data,
                    index=index,
                )
            else:
                compare_complex_nt_obj_to_df(
                    complex_nts=all_sub_complexnt_data[base_table_name],
                    df=all_table_data[base_table_name],
                    allowed_named_tuples=_ALLOWED_NAMED_TUPLES,
                    allowed_data_types=ALLOWED_DATA_TYPES,
                )


@patch("news_scanner.database.database_handles.base_database_handle.validate_complex_nt_obj")
@pytest.mark.parametrize(
    "throw_exception, expected_val", [
        (True, 2),
        (False, 3)
    ]
)
def test_insert_invalid_subcomplexnt(
    mock_validate,
    complexnt_handle,
    complexnt_testobj_unknown_dtype,
    complexnt_ext_testobjs,
    throw_exception,
    expected_val
):
    """ Mocking an exception being throw mid-inserting a complexnt. """
    complexnts_to_insert = [
        complexnt_ext_testobjs[0],
        complexnt_testobj_unknown_dtype,
        complexnt_ext_testobjs[1]
    ]

    # throw exception and stop
    if throw_exception:
        # Can be any exception, want to ensure next primary key matches in each tables
        with pytest.raises(KeyError) as exec_info:
            complexnt_handle.insert(
                insert_data=complexnts_to_insert,
                throw_exception=throw_exception
            )
    # print/log exception and continue
    else:
        complexnt_handle.insert(
            insert_data=complexnts_to_insert,
            throw_exception=throw_exception
        )

    # get and ensure each tables last primary key matches
    assert complexnt_handle.get_next_primary_key() == expected_val
    assert mock_validate.call_count == expected_val


def test_remove(complexnt_handle, complexnt_ext_testobjs):
    assert len(complexnt_ext_testobjs) == 3  # test configured for 3

    complexnt_handle.insert(insert_data=complexnt_ext_testobjs)
    complexnt_handle.remove([1, 3])
    db_table_data = complexnt_handle.get_all()

    # getting expected extended table primary keys
    base_table_pks = {}
    extended_table_pks = {}
    for complexnt_ext_testobj in complexnt_ext_testobjs:
        for base_table_name, complextnt in zip(complexnt_ext_testobj._fields, complexnt_ext_testobj):
            if base_table_name not in base_table_pks:
                base_table_pks[base_table_name] = 1
            _complexnt = complextnt
            for attr_name, attr_value in zip(_complexnt._fields, _complexnt):
                if type(attr_value) == list:
                    extended_table_name = f"{base_table_name}_{attr_name}"
                    base_table_pk = base_table_pks[base_table_name]
                    if extended_table_name not in extended_table_pks:
                        extended_table_pks[extended_table_name] = {}
                    if base_table_pk not in extended_table_pks[extended_table_name]:
                        extended_table_pks[extended_table_name][base_table_pk] = []

                    # creating expected extended data table primary keys
                    if base_table_pk == 1:
                        ext_table_pk = 1
                    else:
                        ext_table_pk = extended_table_pks[extended_table_name][base_table_pk-1][-1] + 1
                    for _ in attr_value:
                        extended_table_pks[extended_table_name][base_table_pk].append(ext_table_pk)
                        ext_table_pk += 1
            base_table_pks[base_table_name] += 1

    # validating table keys
    for base_table_name, all_table_df in db_table_data.items():
        assert all_table_df[base_table_name].shape[0] == 1
        if len(all_table_df.keys()) > 1:
            for table_name, table_df in all_table_df.items():
                if table_name != base_table_name:
                    assert table_df.shape[0] == len(extended_table_pks[table_name][2])
    assert complexnt_handle.get_next_primary_key() == 3
