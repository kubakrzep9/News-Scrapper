""" Test helper functions. """

from typing import NamedTuple, Dict, List, Tuple
from pathlib import Path
import pandas as pd
import numpy as np
from news_scanner.database.util import extract_attrs, get_namedtuple_name, get_table_name
from news_scanner.database.constants import PYTHON_TO_SQL_DTYPES
from tests.database.conftest import TEST_DIR
from news_scanner.database.table_handles.base_table_handle import BaseTableHandle

TEST_DATABASE_DIR = Path(__file__).parent.parent / "test_databases"
DUPLICATE_ATTR_NAME = "Error: duplicate attr name found in complex_nt object."
INVALID_DATA_TYPE = "Error: datatype found not in allowed list."


def _destroy_database(database_dir: Path = TEST_DATABASE_DIR):
    """ Deletes sqlite database files from test_databases directory. """
    for path in database_dir.iterdir():
        if path.is_file():
            path_tokens = str(path).split("/")
            file_name = path_tokens[-1]
            if ".sqlite" in file_name:
                path.unlink()


def tear_down(
    dir_containing_log_dir: Path = TEST_DIR,
    database_dir: Path = TEST_DATABASE_DIR
):
    """ Tear down fixture that will remove database and log files. """
    _destroy_test_logs(dir_containing_log_dir)
    _destroy_database(database_dir)


def _destroy_test_logs(root_dir: Path) -> None:
    """ Recursive function to delete logs from test directory.

    Params:
        root_dir: Path to root directory to have logs deleted.
    """
    delete_dirs = ['logs']
    for path in root_dir.iterdir():
        if path.is_dir():
            path_tokens = str(path).split("/")
            dir_name = path_tokens[-1]
            # ignoring nontest dirs
            if "__" not in dir_name:
                if dir_name in delete_dirs:
                    remove_dir_tree(path)
                else:
                    _destroy_test_logs(path)


def remove_dir_tree(root_dir_path: Path):
    """ Recursively deletes a directory tree from root.

    Params:
        root_dir_path: Path to root directory to be recursively deleted.
    """
    if not root_dir_path.is_dir():
        return

    for path in root_dir_path.iterdir():
        if path.is_dir():
            remove_dir_tree(path)
        else:
            path.unlink()
    root_dir_path.rmdir()


def compare_complex_nt_obj_to_df(
    complex_nts: Dict[int, NamedTuple],
    df: pd.DataFrame,
    allowed_data_types: List = PYTHON_TO_SQL_DTYPES,
    allowed_named_tuples: List[type] = [],
    index_name: str = None,
    foreign_keys: Dict[str, List] = {}
) -> None:
    """ Returns true if complex object attrs match respective df values.

    Validates a table in the database.

    complex: A NamedTuple object that has NamedTuple attributes.
    """
    all_attr_pool_values = {}
    attr_pool_dtypes = {}

    for key_id in complex_nts.keys():
        attr_pool_values, attr_pool_dtypes, _ = extract_attrs(
            complex_nt=complex_nts[key_id],
            allowed_named_tuples=allowed_named_tuples,
            allowed_data_types=allowed_data_types
        )
        all_attr_pool_values[key_id] = attr_pool_values

    generated_df = pd.DataFrame(all_attr_pool_values).T.astype(attr_pool_dtypes)

    # print("foreign_keys:", foreign_keys)
    if foreign_keys:
        for fk_name, fk_values in foreign_keys.items():
            generated_df[fk_name] = fk_values
    generated_df = generated_df.reindex(sorted(generated_df.columns), axis=1)

    if index_name:
        generated_df.index = generated_df.index.rename(index_name)
    df = df.reindex(sorted(df.columns), axis=1)

    # comparing df generated from complex_nts to df passed in
    assert len(generated_df.columns) == len(df.columns)
    assert np.all(generated_df.columns.sort_values() == df.columns.sort_values())
    # print("\ngenerated\n", generated_df, "\ndatabase\n", df)
    assert generated_df.equals(df)


def extract_sub_complexnt(
    complex_nts: List,
    index_list: List = None
) -> Dict[str, Dict[int, NamedTuple]]:
    """ Returns a dict where keys are names of sub complext_nts and values are lists of their values. """
    if index_list:
        assert len(complex_nts) == len(index_list)
    else:
        index_list = [i for i in range(1, len(complex_nts) + 1)]

    # setting up results dict
    sub_complexnt_data = {}
    complex_nt = complex_nts[0]
    for sub_complexnt in complex_nt:
        sub_nt_name = get_table_name(get_namedtuple_name(sub_complexnt))
        # dict for each subcomplexnt (table)
        sub_complexnt_data[sub_nt_name] = {}

    # extracting sub_complexnt data
    for complex_nt, key in zip(complex_nts, index_list):
        for sub_complexnt in complex_nt:
            sub_nt_name = get_table_name(get_namedtuple_name(sub_complexnt))
            sub_complexnt_data[sub_nt_name][key] = sub_complexnt

    return sub_complexnt_data


def extract_extended_data(
    complex_nts: Dict[int, NamedTuple],
    extended_table_indexes: Dict[str, Dict[int, List[int]]],
    allowed_data_types: List,
    allowed_named_tuples: List,
    base_table_primary_key: str
) -> Tuple[Dict, Dict]:
    # creating python objects to generate dataframe to validate
    # the database against.
    all_extended_data_fks = {}
    all_extended_data_objs = {}
    init_all_ext_data_objs = False

    arb_complex_nt = [*complex_nts.values()][0]
    base_table_name = get_table_name(type(arb_complex_nt).__name__)

    for primary_key, complex_nt in complex_nts.items():
        _, _, extended_data = extract_attrs(
            complex_nt=complex_nt,
            allowed_data_types=allowed_data_types,
            allowed_named_tuples=allowed_named_tuples
        )
        # init struct to hold ext data
        if not init_all_ext_data_objs:
            for data_name, _ in extended_data.items():
                all_extended_data_objs[data_name] = {}
                all_extended_data_fks[data_name] = {base_table_primary_key: []}
            init_all_ext_data_objs = True
        # extract ext data
        for ext_data_name, meta_data in extended_data.items():
            ext_table_name = f"{base_table_name}_{ext_data_name}"
            for extended_data_value, ext_pk in zip(
                meta_data["value"],
                extended_table_indexes[ext_table_name][primary_key]
            ):
                all_extended_data_objs[ext_data_name][ext_pk] = extended_data_value
                all_extended_data_fks[ext_data_name][base_table_primary_key].append(primary_key)

    return all_extended_data_objs, all_extended_data_fks


#######################################
# Need extended table keys still fd
######################################
def compare_complex_extended_nt_obj_to_df(
    base_table_handle: BaseTableHandle,
    test_objs: List[NamedTuple],
    table_data: Dict[str, pd.DataFrame],
    index: List[int],
    extended_table_indexes: Dict[str, Dict[int, List[int]]] = {}
):
    assert len(test_objs) == len(index)

    # extracting reused variables
    base_table_config = base_table_handle.table_handle_data.table_config
    base_table_name = base_table_config.table_name
    base_table_primary_key = base_table_config.primary_key
    allowed_named_tuples = base_table_config.allowed_namedtuples
    allowed_data_types = base_table_config.allowed_dtypes
    extended_table_handles_data = base_table_handle.extended_table_handles_data

    # initing test objs with primary keys
    _test_objs_w_pks = {}
    for i, test_obj in zip(index, test_objs):
        _test_objs_w_pks[i] = test_obj
    # validating base table
    compare_complex_nt_obj_to_df(
        complex_nts=_test_objs_w_pks,
        df=table_data[base_table_name],
        allowed_data_types=[*allowed_data_types.keys()],
        allowed_named_tuples=allowed_named_tuples,
        index_name=base_table_primary_key
    )

    # extracting extended data objs
    all_extended_data_objs, all_extended_data_fks = extract_extended_data(
        complex_nts=_test_objs_w_pks,
        extended_table_indexes=extended_table_indexes,
        allowed_data_types=[*allowed_data_types.keys()],
        allowed_named_tuples=allowed_named_tuples,
        base_table_primary_key=base_table_primary_key,
    )

    if extended_table_indexes:
        for ext_data_name, ext_data_objs_w_pks in all_extended_data_objs.items():
            ext_table_name = f"{base_table_name}_{ext_data_name}"
            compare_complex_nt_obj_to_df(
                    complex_nts=ext_data_objs_w_pks,
                    df=table_data[ext_table_name],
                    foreign_keys=all_extended_data_fks[ext_data_name],
                    index_name=extended_table_handles_data[ext_data_name].table_config.primary_key
                )
























