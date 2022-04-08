""" """

from typing import NamedTuple, Dict, List, Tuple, Any


DTYPE = "dtype"
VALUE = "value"
NAMEDTUPLE = "namedtuple"

ERROR_INVALID_NT_NAME = "Error: nt_name must contain only letters or numbers."
ERROR_FIRST_UPPER = "Error nt_name's first letter must be uppercase."
DUPLICATE_ATTR_NAME = "Error: duplicate attr name found in complex_nt object."
INVALID_DATA_TYPE = "Error: datatype found not in allowed list."
INVALID_FIRST_LEVEL_TYPE = "Error: first level attrs of a complext_nt must be a namedtuple."
INVALID_COMPLEX_NT_OBJ_TYPE = "Error: the complex_nt type does not match the expected DBObjectConfig.complex_nt_type."
ATTR_TYPE_MISMATCH = "Error: an attribute type does not match the expected DBObjectConfig.allowed_dtypes."
NAMEDTUPLE_NAME_NOT_FOUND = "Error: no namedtuple name found."
INVALID_COMPLEX_NT_ROOT_TYPE = "Error: complexnt_type must be a class which inherits from NamedTuple."


def get_datatype(value: Any, allowed_dtypes: Dict, allowed_namedtuples: List = []) -> str:
    """ """
    for dtype in allowed_dtypes.keys():
        if isinstance(value, dtype):
            return allowed_dtypes[dtype]

    for namedtuple_type in allowed_namedtuples:
        if isinstance(value, namedtuple_type):
            return NAMEDTUPLE
    print(value, type(value))
    raise ValueError(INVALID_DATA_TYPE)


def get_name(nt_name: str) -> str:
    for c in nt_name:
        if not c.isnumeric() and not c.isalpha():
            raise ValueError(ERROR_INVALID_NT_NAME)
    if not nt_name[0].isupper():
        raise ValueError(ERROR_FIRST_UPPER)

    formatted_name = nt_name[0].lower()
    for char in nt_name[1:]:
        prefix = "_" if char.isupper() else ""
        formatted_name = f"{formatted_name}{prefix}{char.lower()}"

    return formatted_name


# Used by BaseTableHandle
def extract_attrs(
        complex_nt: NamedTuple,
        allowed_data_types: List,
        allowed_named_tuples: List[type] = None,
        attr_pool_values: Dict = None,
        attr_pool_dtypes: Dict = None,
) -> Tuple[Dict, Dict]:
    # initializing default values
    if not allowed_named_tuples:
        allowed_named_tuples = []
    if not attr_pool_values:
        attr_pool_values = {}
    if not attr_pool_dtypes:
        attr_pool_dtypes = {}

    attr_pool_values, attr_pool_dtypes = _extract_attrs(
        complex_nt=complex_nt,
        allowed_data_types=allowed_data_types,
        allowed_named_tuples=allowed_named_tuples,
        attr_pool_values=attr_pool_values,
        attr_pool_dtypes=attr_pool_dtypes
    )
    return attr_pool_values, attr_pool_dtypes


def _extract_attrs(
    complex_nt: NamedTuple,
    allowed_data_types: List,
    allowed_named_tuples: List[type],
    attr_pool_values: Dict,
    attr_pool_dtypes: Dict,
):
    """ Recursive"""
    for attr_name, attr_value in zip(complex_nt._fields, complex_nt):
        attr_type = type(attr_value)
        if attr_type in allowed_named_tuples:
            _extract_attrs(
                complex_nt=attr_value,
                allowed_named_tuples=allowed_named_tuples,
                allowed_data_types=allowed_data_types,
                attr_pool_values=attr_pool_values,
                attr_pool_dtypes=attr_pool_dtypes,
            )
        elif attr_type in allowed_data_types:
            if attr_name in attr_pool_values.keys():
                raise ValueError(f"{DUPLICATE_ATTR_NAME} ('{attr_name}')")
            else:
                attr_pool_values[attr_name] = attr_value
                attr_pool_dtypes[attr_name] = attr_type
        else:
            raise ValueError(f"{INVALID_DATA_TYPE} ('{attr_type}")
    return attr_pool_values, attr_pool_dtypes


def validate_complex_nt_schema(
    complexnt_type: type,
    allowed_data_types: List,
    allowed_namedtuple_types: List
):
    """ Validates complex_nt can be turned into a DatabaseHandle.

    Params:
        complex_nt_type: complex_nt to be turned into a database.

    Returns default complex_nt.
    """
    default_complex_nt = complexnt_type()

    if "tuple" not in str(complexnt_type.__base__):
        raise ValueError(INVALID_COMPLEX_NT_ROOT_TYPE)

    for named_tuple in default_complex_nt:
        base_type = str(type(named_tuple).__base__)
        if "tuple" not in base_type:
            raise ValueError(INVALID_FIRST_LEVEL_TYPE)

        extract_attrs(
            complex_nt=named_tuple,
            allowed_data_types=allowed_data_types,
            allowed_named_tuples=allowed_namedtuple_types
        )

    return default_complex_nt


def validate_complex_nt_obj(
    complex_nt: NamedTuple,
    expected_complex_nt_type: type,
    expected_allowed_namedtuples: List[type]
):
    if type(complex_nt) != expected_complex_nt_type:
        raise ValueError(INVALID_COMPLEX_NT_OBJ_TYPE)

    default_expected_complex_nt = expected_complex_nt_type()
    attr_values = _validate_complex_nt_obj(
        complex_nt=complex_nt,
        default_expected_complex_nt=default_expected_complex_nt,
        expected_allowed_namedtuples=expected_allowed_namedtuples,
    )
    return attr_values


def _validate_complex_nt_obj(
    complex_nt: NamedTuple,
    default_expected_complex_nt: NamedTuple,
    expected_allowed_namedtuples: List[type],
):
    """ Recursive function """
    # looking for attr type mismatches / unknown types
    for attr_value, expected_attr_value, expected_attr_name in zip(
        complex_nt, default_expected_complex_nt, default_expected_complex_nt._fields
    ):
        attr_type = type(attr_value)
        expected_type = type(expected_attr_value)
        if attr_type != expected_type:
            raise ValueError(f"{ATTR_TYPE_MISMATCH} For '{expected_attr_name}' expected:{expected_type} got:{attr_type}")
        elif type(expected_attr_value) in expected_allowed_namedtuples:
            _validate_complex_nt_obj(
                complex_nt=attr_value,
                default_expected_complex_nt=expected_attr_value,
                expected_allowed_namedtuples=expected_allowed_namedtuples,
            )


def get_namedtuple_name(namedtuple):
    name_end_index = str(namedtuple).find("(")
    if name_end_index == -1:
        raise ValueError(NAMEDTUPLE_NAME_NOT_FOUND)
    return str(namedtuple)[:name_end_index]
