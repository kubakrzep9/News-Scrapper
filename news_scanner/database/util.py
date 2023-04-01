""" """

from typing import NamedTuple, Dict, List, Tuple, Any


DTYPE = "dtype"
VALUE = "value"
NAMEDTUPLE = "namedtuple"
NAMED_TUPLE_DTYPE = "named_tuple_dtype"
EXTENDED_DATA_VALUE = "value"

ERROR_INVALID_NT_NAME = "Error: nt_name must contain only letters or numbers."
ERROR_FIRST_UPPER = "Error nt_name's first letter must be uppercase."
DUPLICATE_ATTR_NAME = "Error: duplicate attr name found in complex_nt object."
INVALID_DATA_TYPE = "Error: datatype found not in allowed list."
INVALID_FIRST_LEVEL_TYPE = "Error: first level attrs of a complext_nt must be a namedtuple."
INVALID_COMPLEX_NT_OBJ_TYPE = "Error: the complex_nt type does not match the expected DBObjectConfig.complex_nt_type."
ATTR_TYPE_MISMATCH = "Error: an attribute type does not match the expected DBObjectConfig.allowed_dtypes."
NAMEDTUPLE_NAME_NOT_FOUND = "Error: no namedtuple name found."
INVALID_COMPLEX_NT_ROOT_TYPE = "Error: complexnt_type must be a class which inherits from NamedTuple."
INVALID_EXTENDED_DTYPE = "Error: extended_data type not found in allowed namedtuples list."


def get_datatype(value: Any, allowed_dtypes: Dict, allowed_namedtuples: List = []) -> str:
    """ """
    for dtype in allowed_dtypes.keys():
        if isinstance(value, dtype):
            return allowed_dtypes[dtype]

    for namedtuple_type in allowed_namedtuples:
        if isinstance(value, namedtuple_type):
            return NAMEDTUPLE
    raise ValueError(INVALID_DATA_TYPE)


def get_table_name(nt_name: str) -> str:
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
) -> Tuple[Dict, Dict, Dict]:
    """ Extracts values and data types for complex_nts.

        Will identify extended data but will not extract its attributes
        values and data types.
    """
    if not allowed_named_tuples:
        allowed_named_tuples = []

    attr_pool_values, attr_pool_dtypes, extended_data = _extract_attrs(
        complex_nt=complex_nt,
        allowed_data_types=allowed_data_types,
        allowed_named_tuples=allowed_named_tuples,
        attr_pool_values={},
        attr_pool_dtypes={},
        extended_data={},
    )
    return attr_pool_values, attr_pool_dtypes, extended_data


def _extract_attrs(
    complex_nt: NamedTuple,
    allowed_data_types: List,
    allowed_named_tuples: List[type],
    attr_pool_values: Dict[str, Any],
    attr_pool_dtypes: Dict[str, type],
    extended_data: Dict[str, Any]
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
                extended_data=extended_data,
            )
        elif attr_type in allowed_data_types:
            if attr_name in attr_pool_values.keys():
                raise ValueError(f"{DUPLICATE_ATTR_NAME} ('{attr_name}')")
            elif attr_type == list and len(attr_value) > 0:
                extended_data_type = type(attr_value[0])
                # Validate
                # extended_data must be a named tuple
                if extended_data_type not in allowed_named_tuples:
                    raise ValueError(f"{INVALID_EXTENDED_DTYPE} ('{attr_name}' '{extended_data_type}')")
                extended_data[attr_name] = {
                    NAMED_TUPLE_DTYPE: extended_data_type,
                    EXTENDED_DATA_VALUE: attr_value
                }
            else:
                attr_pool_values[attr_name] = attr_value
                attr_pool_dtypes[attr_name] = attr_type
        else:
            print("allowed_data_types:", allowed_data_types)
            raise ValueError(f"{INVALID_DATA_TYPE} ('{attr_name}') ('{attr_type})")
    return attr_pool_values, attr_pool_dtypes, extended_data


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


def get_table_name2(namedtuple) -> str:
    return get_table_name(get_namedtuple_name(namedtuple))
