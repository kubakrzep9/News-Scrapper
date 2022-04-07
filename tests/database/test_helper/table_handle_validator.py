""" """

from news_scanner.database.table_handles.base_table_handle import BaseTableHandle
from typing import List, NamedTuple, Dict
from tests.database.test_helper.util import compare_complex_nt_obj_to_df


# foreign keys
# - validate by splitting df from results of get all
def validate_table_handle(
    table_handle: BaseTableHandle,
    insert_list: List[NamedTuple],
    expected_db_file_name: str,
    expected_table_name: str,
    foreign_keys: List[Dict[str, int]] = [],
):
    len_insert_list = len(insert_list)
    if len_insert_list < 2:
        raise ValueError("Error: insert_list must contain at least 2 values.")

    assert table_handle.db_file_name == expected_db_file_name
    assert table_handle.table_data.table_name == expected_table_name

    if not foreign_keys:
        foreign_keys = [{} for _ in range(len_insert_list)]
    assert len_insert_list == len(foreign_keys)
    assert table_handle.table_exists()

    complex_nts = {}
    # insert into table
    primary_key = 1
    for named_tuple, foreign_key in zip(insert_list, foreign_keys):
        table_handle.insert(
            named_tuple=named_tuple,
            primary_key=primary_key,
            foreign_keys=foreign_key
        )
        complex_nts[primary_key] = named_tuple
        primary_key += 1
    primary_key -= 1  # set to last insert pk
    # get all from table
    df = table_handle.get_all()
    # validate using compare_complex..
    compare_complex_nt_obj_to_df(
        complex_nts=complex_nts,
        df=df,
    )
    # remove from table
    table_handle.remove()
    del complex_nts[primary_key]
    # get all from table
    df = table_handle.get_all()
    # validate using compare_complex..
    compare_complex_nt_obj_to_df(
        complex_nts=complex_nts,
        df=df,
    )
