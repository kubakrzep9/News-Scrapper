""" Validates 'BaseDatabaseHandle' class. """

import pytest
from news_scanner.database.database_handles.base_database_handle import (
    BaseDatabaseHandle,
    DBObjectConfig
)
from tests.database.conftest import ComplexNT, ALLOWED_NAMED_TUPLES, ALLOWED_DATA_TYPES, TEST_DB_DIR
from tests.database.test_helper.util import tear_down, compare_complex_nt_obj_to_df, extract_sub_complexnt
from mock import patch


class ComplexNTHandle(BaseDatabaseHandle):
    def __init__(self):
        tear_down()
        super().__init__(
            db_object_config=DBObjectConfig(
                complex_nt_type=ComplexNT,
                allowed_namedtuples=ALLOWED_NAMED_TUPLES,
            ),
            db_dir=TEST_DB_DIR
        )


@pytest.fixture
def complexnt_handle() -> ComplexNTHandle:
    """ Reused test object inheriting"""
    return ComplexNTHandle()


def test_init(complexnt_handle):
    # ensure db file is created
    assert complexnt_handle.database_path.is_file()

    # ensure default filename contains object name.
    path_tokens = str(complexnt_handle.database_path).split("/")
    assert ComplexNT.__name__ in path_tokens[-1]

    # validate column names, no data
    db_table_data = complexnt_handle.get_all()
    for table_name, table_handle in complexnt_handle.table_handles.items():
        assert table_name in db_table_data.keys()
        assert db_table_data[table_name].empty
        column_names = [*db_table_data[table_name].columns]
        expected_column_names = [*table_handle.attr_dtypes.keys()]
        assert column_names == expected_column_names


def test_get_next_primary_key(complexnt_handle):
    """ Ensures the next primary key is 1 when the database is empty.

    Primary key progression validated in 'test_insert'.

    """
    next_pk = complexnt_handle.get_next_primary_key()
    assert next_pk is 1


@pytest.mark.parametrize(
    "num_insert", [0, 1, 3]
)
def test_insert_and_get_all(complexnt_handle, complexnt_testobjs, num_insert):
    complex_nts = {}
    i = 1
    for test_obj in complexnt_testobjs[:num_insert]:
        complexnt_handle.insert(complex_nt=test_obj)
        complex_nts[i] = test_obj
        i += 1

    db_table_data = complexnt_handle.get_all()

    if num_insert == 0:
        for table_name, table_data in db_table_data.items():
            assert table_data.empty
    else:
        sub_complexnt_data = extract_sub_complexnt(complex_nts=complexnt_testobjs[:num_insert])
        for table_name, table_data in db_table_data.items():
            # validating column data
            compare_complex_nt_obj_to_df(
                complex_nts=sub_complexnt_data[table_name],
                df=table_data,
                allowed_named_tuples=ALLOWED_NAMED_TUPLES,
                allowed_data_types=ALLOWED_DATA_TYPES
            )

            # validating primary keys
            primary_keys = ([*table_data.index])
            expected_pks = [i for i in range(1, i)]
            assert primary_keys == expected_pks


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
    complexnt_testobjs,
    throw_exception,
    expected_val
):
    """ Mocking an exception being throw mid-inserting a complexnt. """
    complexnts_to_insert = [
        complexnt_testobjs[0],
        complexnt_testobj_unknown_dtype,
        complexnt_testobjs[1]
    ]

    # throw exception and stop
    if throw_exception:
        # Can be any exception, want to ensure next primary key matches in each tables
        with pytest.raises(ValueError) as exec_info:
            for complex_nt in complexnts_to_insert:
                complexnt_handle.insert(
                    complex_nt=complex_nt,
                    throw_exception=throw_exception
                )
    # print/log exception and continue
    else:
        for complex_nt in complexnts_to_insert:
            complexnt_handle.insert(
                complex_nt=complex_nt,
                throw_exception=throw_exception
            )

    # get and ensure each tables last primary key matches
    assert complexnt_handle.get_next_primary_key() == expected_val
    assert mock_validate.call_count == expected_val


def test_remove(complexnt_handle, complexnt_testobjs):
    assert len(complexnt_testobjs) == 3  # test configured for 3

    for complex_nt in complexnt_testobjs:
        complexnt_handle.insert(complex_nt=complex_nt)

    complexnt_handle.remove([1, 3])

    db_table_data = complexnt_handle.get_all()

    for table_name, table_df in db_table_data.items():
        assert table_df.shape[0] == 1
    assert complexnt_handle.get_next_primary_key() == 3
