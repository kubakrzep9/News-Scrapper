""" Class to house common database functionality. """

import pandas as pd
from pathlib import Path
from typing import List, NamedTuple, Dict, Tuple, Union
from news_scanner.database.util import extract_attrs, get_name
from news_scanner.database.constants import (
    PYTHON_TO_SQL_DTYPES,
    DEF_PRIMARY_KEY,
)
from news_scanner.database.base_handle import BaseHandle, DB_DIR


TABLE_DOESNT_EXIST = "Error: table doesn't exist - "
MISSING_NT_TYPE = "Error: cannot generate_name, missing named_tuple_type in table_data."


class ForeignkeyConfig(NamedTuple):
    foreign_key: str
    reference_table: str
    reference_table_primary_key: str


class TableData(NamedTuple):
    named_tuple_type: type = None
    table_name: str = None
    primary_key: str = DEF_PRIMARY_KEY
    foreign_keys: List[ForeignkeyConfig] = []
    allowed_dtypes: Dict = PYTHON_TO_SQL_DTYPES  # add validation
    allowed_namedtuples: List = []


class BaseTableHandle(BaseHandle):
    """ Class to house common database functionality. """

    def __init__(
        self,
        table_data: TableData,
        db_file_name: str = None,
        db_dir: Path = DB_DIR,
    ):
        """ Creates a directory if one doesn't exist to store the database.

        Note: Must be executed first in constructor of inheriting class.

        Params:
            database_path: Path to database file.
            database_dir: Path to directory containing database files.
            table_name: Name of table within database.
            primary_key: Name of primary key for table.
            complex_nt: NamedTuple that may contain nested NamedTuples
                of attributes containing values of allowed data types.
            allowed_named_tuples: List of NamedTuple types that are allowed
                to be attributes in the complex_nt.
        """
        # generate name from NamedTuple class name
        self.db_file_name, self.table_data = generate_name(
            db_file_name=db_file_name,
            table_data=table_data
        )
        # setting up db file
        super().__init__(
            db_file_name=self.db_file_name,
            db_dir=db_dir
        )
        self.default_nt_instance = table_data.named_tuple_type()

        # getting named_tuple/table data types
        _, self.attr_dtypes = extract_attrs(
            attr_pool_dtypes={},
            attr_pool_values={},
            complex_nt=self.default_nt_instance,
            allowed_data_types=[*self.table_data.allowed_dtypes.keys()],
            allowed_named_tuples=self.table_data.allowed_namedtuples
        )

        # creating table column types
        self.table_column_dtypes = self.attr_dtypes.copy()
        foreign_key_names = [key.foreign_key for key in self.table_data.foreign_keys]
        for key_name in [*foreign_key_names, table_data.primary_key]:
            self.table_column_dtypes[key_name] = int

        # building table
        self.build_table()

    def table_exists(self, table_name: str = None):
        """ Returns true if table_name exists as a table in the database.

        Params:
            table_name: Name of table in database, not required if table_name
             is passed into super constructor.
        """
        if not table_name:
            table_name = self.table_data.table_name
        query = f"SELECT name FROM sqlite_master " \
                f"WHERE type='table' AND name='{table_name}'"
        result = self.execute_query(query)
        df = pd.DataFrame(result)
        if len(df) == 1:
            return True
        return False

    def build_table(self) -> None:
        """ Builds table from NamedTuple with set default values. """
        if self.table_exists():
            return

        build_query = f"CREATE TABLE {self.table_data.table_name} ("

        # adding columns (attrs, primary key, foreign keys)
        for attr_name in self.table_column_dtypes:
            pk_tag = "PRIMARY KEY" if attr_name == self.table_data.primary_key else ""
            col_dtype = PYTHON_TO_SQL_DTYPES[self.table_column_dtypes[attr_name]]
            build_query += f"[{attr_name}] {col_dtype} {pk_tag}, "

        # add foreign key references
        if len(self.table_data.foreign_keys) > 0:
            for foreign_key in self.table_data.foreign_keys:
                build_query += f"FOREIGN KEY({foreign_key.foreign_key}) " \
                               f"REFERENCES {foreign_key.reference_table}(" \
                               f"{foreign_key.reference_table_primary_key}) " \
                               f"ON DELETE SET NULL, "
        build_query = build_query[:-2]  # removing last ', '
        build_query += ")"
        # print(build_query)
        self.execute_query(build_query)

    def get_all(self) -> pd.DataFrame:
        """ Returns table contents as dataframe. """
        query = f"PRAGMA table_info({self.table_data.table_name})"
        results = self.execute_query(query)
        column_names = [col[1] for col in results]
        query = f"SELECT * FROM {self.table_data.table_name}"
        result = self.execute_query(query)
        df = pd.DataFrame(
            result, columns=column_names
        ).set_index(self.table_data.primary_key)
        return df

    def insert(
        self,
        named_tuple: NamedTuple,
        primary_key: int,
        foreign_keys: Dict[str, int] = None
    ) -> None:
        """ """
        if not foreign_keys:
            foreign_keys = {}

        attr_pool_values, _ = extract_attrs(
            complex_nt=named_tuple,
            allowed_data_types=[*self.table_data.allowed_dtypes],
            allowed_named_tuples=self.table_data.allowed_namedtuples
        )

        columns = f"({self.table_data.primary_key}"
        values = f"VALUES({primary_key}"
        for key in foreign_keys:
            columns += f", {key}"
            values += f", {foreign_keys[key]}"
        for column, value in attr_pool_values.items():
            if column is not self.table_data.primary_key and column not in foreign_keys:
                columns += f", {column}"
                values += f", "
                if isinstance(value, str):
                    values += f"'{value}'"
                else:
                    values += f"{value}"
        columns += ")"
        values += ")"
        insert_query = f"INSERT INTO {self.table_data.table_name} {columns} {values}"
        #print(insert_query)
        self.execute_query(insert_query)

    def get_last_primary_key(self):
        if self.table_exists():
            last_key_query = f"SELECT MAX({self.table_data.primary_key}) FROM {self.table_data.table_name}"
            res = self.execute_query(last_key_query)
            return res[0][0]
        else:
            raise ValueError(TABLE_DOESNT_EXIST+self.table_data.table_name)

    def remove(self, primary_key: int = None):
        """ Removes last row if no primary_key is passed in. """
        if self.table_exists():
            if not primary_key:
                primary_key = self.get_last_primary_key()
            delete_query = f"DELETE FROM {self.table_data.table_name} " \
                           f"WHERE {self.table_data.primary_key} = {primary_key}"
            # print(delete_query)
            self.execute_query(delete_query)
        else:
            raise ValueError(TABLE_DOESNT_EXIST+self.table_data.table_name)


def generate_name(
    db_file_name: Union[str, None],
    table_data: Union[TableData, None]
) -> Tuple[str, TableData]:
    if not table_data.named_tuple_type:
        raise ValueError(MISSING_NT_TYPE)

    db_file_type = ".sqlite"
    name = get_name(table_data.named_tuple_type.__name__)
    if not db_file_name:
        db_file_name = name+db_file_type
    if not table_data.table_name:
        table_data_dict = table_data._asdict()
        del table_data_dict["table_name"]
        table_data = TableData(table_name=name, **table_data_dict)
    return db_file_name, table_data

