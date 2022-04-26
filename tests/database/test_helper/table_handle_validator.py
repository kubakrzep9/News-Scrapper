""" """

from news_scanner.database.table_handles.base_table_handle import BaseTableHandle, TableConfig
from typing import List, NamedTuple, Dict
from tests.database.test_helper.util import (
    compare_complex_nt_obj_to_df,
    compare_complex_extended_nt_obj_to_df
)


def validate_table_handle(
    table_handle: BaseTableHandle,
    insert_list: List[NamedTuple],
    expected_db_file_name: str,
    expected_table_name: str,
):
    len_insert_list = len(insert_list)
    if len_insert_list < 2:
        raise ValueError("Error: insert_list must contain at least 2 values.")

    assert table_handle.db_file_name == expected_db_file_name
    assert table_handle.table_handle_data.table_config.table_name == expected_table_name

    assert table_handle.table_exists()

    complex_nts = {}
    # insert into table
    primary_key = 1
    for named_tuple in insert_list:
        table_handle.insert(
            named_tuple=named_tuple,
            primary_key=primary_key,
        )
        complex_nts[primary_key] = named_tuple
        primary_key += 1
    primary_key -= 1  # set to last insert pk
    # get all from table
    table_data = table_handle.get_all()
    df = table_data[table_handle.table_handle_data.table_config.table_name]
    # validate using compare_complex..
    compare_complex_nt_obj_to_df(
        complex_nts=complex_nts,
        df=df,
    )
    # remove from table
    table_handle.remove()
    del complex_nts[primary_key]
    # get all from table
    table_data = table_handle.get_all()
    df = table_data[table_handle.table_handle_data.table_config.table_name]
    # validate using compare_complex..
    compare_complex_nt_obj_to_df(
        complex_nts=complex_nts,
        df=df,
    )
