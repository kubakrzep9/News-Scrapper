""" """

from typing import Union, NamedTuple
import pytest
from news_scanner.database.constants import PYTHON_TO_SQL_DTYPES
from news_scanner.database.util import (
    get_datatype,
    get_table_name,
    extract_attrs,
    validate_complex_nt_schema,
    validate_complex_nt_obj,
    get_namedtuple_name,
    ERROR_INVALID_NT_NAME,
    ERROR_FIRST_UPPER,
    DUPLICATE_ATTR_NAME,
    INVALID_DATA_TYPE,
    INVALID_COMPLEX_NT_OBJ_TYPE,
    ATTR_TYPE_MISMATCH,
    NAMEDTUPLE_NAME_NOT_FOUND
)
from tests.database.conftest import (
    ComplexNT,
    NamedTuple1,
    NamedTuple3,
    WrongNamedTuple,
    NamedTupleDuplicateAttr,
    ComplexNTDuplicateAttr,
    ALLOWED_DATA_TYPES,
    ALLOWED_NAMED_TUPLES,
)

RANDOM_VAR = "hoopla"


@pytest.mark.parametrize(
    "value, expected_type", [
        (0, PYTHON_TO_SQL_DTYPES[int]),
        (0.0, PYTHON_TO_SQL_DTYPES[float]),
        ("str", PYTHON_TO_SQL_DTYPES[str])
    ]
)
def test_get_datatype(
    value: Union[int, float, str],
    expected_type: str
):
    assert get_datatype(value=value, allowed_dtypes=PYTHON_TO_SQL_DTYPES) == expected_type


def test_get_datatype_failed():
    with pytest.raises(ValueError, match=INVALID_DATA_TYPE):
        get_datatype(value=set(), allowed_dtypes=PYTHON_TO_SQL_DTYPES)


def test_get_formatted_name():
    nt_name = "NamedTuple"
    expected_formatted_name = "named_tuple"
    formatted_name = get_table_name(nt_name)
    assert formatted_name == expected_formatted_name


@pytest.mark.parametrize(
    "nt_name, expected_error", [
        ("NamedTuple2-", ERROR_INVALID_NT_NAME),
        ("namedtuple", ERROR_FIRST_UPPER)
    ]
)
def test_get_formatted_name(nt_name: str, expected_error: str):
    with pytest.raises(ValueError) as exec_info:
        get_table_name(nt_name)
    assert expected_error in str(exec_info)


def test_extract_attrs():
    complexnt = ComplexNT()

    # both sub namedtuples have same expected
    expected_attr_pool_values = {
        "attr1": "str",
        "attr2": 1,
        "attr3": 0.01
    }
    expected_attr_pool_dtypes = {
        "attr1": str,
        "attr2": int,
        "attr3": float
    }

    for namedtuple in [complexnt.namedtuple1, complexnt.namedtuple2]:
        attr_pool_values, attr_pool_dtypes, _ = extract_attrs(
            complex_nt=namedtuple,
            allowed_named_tuples=ALLOWED_NAMED_TUPLES,
            allowed_data_types=ALLOWED_DATA_TYPES
        )
        assert attr_pool_values == expected_attr_pool_values
        assert attr_pool_dtypes == expected_attr_pool_dtypes


def test_extract_attrs_duplicate_attr_name():
    class DuplicateAttr(NamedTuple):
        attr3: str = "dd"  # duplicate name
        named_tuple3: NamedTuple3 = NamedTuple3()

    with pytest.raises(ValueError, match=DUPLICATE_ATTR_NAME):
        attr_pool = extract_attrs(
            complex_nt=DuplicateAttr(),
            allowed_named_tuples=ALLOWED_NAMED_TUPLES,
            allowed_data_types=ALLOWED_DATA_TYPES,
        )


def test_extract_attrs_invalid_data_types():
    with pytest.raises(ValueError, match=INVALID_DATA_TYPE):
        attr_pool = extract_attrs(
            complex_nt=NamedTuple1(),
            allowed_named_tuples=ALLOWED_NAMED_TUPLES,
            allowed_data_types=[int, float]  # missing str
        )


def test_validate_complex_nt_schema():
    complexnt_object = validate_complex_nt_schema(
        complexnt_type=ComplexNT,
        allowed_data_types=ALLOWED_DATA_TYPES,
        allowed_namedtuple_types=ALLOWED_NAMED_TUPLES
    )
    assert isinstance(complexnt_object, ComplexNT)


def test_validate_complex_nt_schema_invalid_attr_type():
    """ attr type not in allowed datatype list"""
    with pytest.raises(ValueError) as exec_info:
        validate_complex_nt_schema(
            complexnt_type=ComplexNT,
            allowed_data_types=ALLOWED_DATA_TYPES[:-1],
            allowed_namedtuple_types=ALLOWED_NAMED_TUPLES
        )

    assert INVALID_DATA_TYPE in str(exec_info)


def test_validate_complex_nt_schema_duplicate_attr_name():
    """ A layer 1 namedtuple has duplicate attr names. """
    with pytest.raises(ValueError) as exec_info:
        validate_complex_nt_schema(
            complexnt_type=ComplexNTDuplicateAttr,
            allowed_data_types=ALLOWED_DATA_TYPES,
            allowed_namedtuple_types=[NamedTupleDuplicateAttr, NamedTuple3]
        )
        
    assert DUPLICATE_ATTR_NAME in str(exec_info)
    

def test_validate_complex_nt_obj_invalid_complex_nt_type():
    """ passed complex_nt schema does not match expected complex_nt schema."""
    with pytest.raises(ValueError) as exec_info:
        validate_complex_nt_obj(
            complex_nt=WrongNamedTuple(),
            expected_complex_nt_type=ComplexNT,
            expected_allowed_namedtuples=ALLOWED_NAMED_TUPLES
        )

    assert INVALID_COMPLEX_NT_OBJ_TYPE in str(exec_info)


@pytest.mark.parametrize(
    "complex_nt", [
        "complexnt_testobj_mismatched_attr_dtype",
        "complexnt_testobj_unknown_dtype"
    ]
)
def test_validate_complex_nt_obj_mismatched_attr_type(complex_nt, request):
    """ passed complex_nt """
    complex_nt = request.getfixturevalue(complex_nt)
    with pytest.raises(ValueError) as exec_info:
        validate_complex_nt_obj(
            complex_nt=complex_nt,
            expected_complex_nt_type=ComplexNT,
            expected_allowed_namedtuples=ALLOWED_NAMED_TUPLES
        )
    assert ATTR_TYPE_MISMATCH in str(exec_info)


def test_validate_complex_nt_obj(complexnt_testobjs):
    validate_complex_nt_obj(
        complex_nt=complexnt_testobjs[0],
        expected_complex_nt_type=ComplexNT,
        expected_allowed_namedtuples=ALLOWED_NAMED_TUPLES
    )


def test_get_namedtuple_name():
    assert get_namedtuple_name(ComplexNT()), "ComplexNT"


def test_get_namedtuple_name_invalid():
    with pytest.raises(ValueError) as exec_info:
        get_namedtuple_name(RANDOM_VAR)

    assert NAMEDTUPLE_NAME_NOT_FOUND in str(exec_info)
