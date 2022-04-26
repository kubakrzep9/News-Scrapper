""" Class to house common database functionality. """

import pandas as pd
from pathlib import Path
from typing import List, NamedTuple, Dict, Tuple, Union, Any
from news_scanner.database.util import (
    extract_attrs,
    get_table_name,
    NAMED_TUPLE_DTYPE,
    EXTENDED_DATA_VALUE
)
from news_scanner.database.constants import (
    PYTHON_TO_SQL_DTYPES,
    DEF_PRIMARY_KEY,
    EXTENDED_DATA_PRIMARY_KEY
)
from news_scanner.database.base_handle import BaseHandle, DB_DIR


TABLE_DOESNT_EXIST = "Error: table doesn't exist - "
MISSING_NT_TYPE = "Error: cannot generate_name, missing named_tuple_type in table_data."
INSERT_PRIMARY_KEY_ERROR = "Error: there must be only one primary key."
EXTENDED_DATA_EXTENDED_DATA_ERROR = "Error: extended data is not permitted to have extended data within itself."


class TableConfig(NamedTuple):
    """ Configured by user and passed into BaseTableHandle init. """
    named_tuple_type: type = None
    table_name: str = None
    primary_key: str = DEF_PRIMARY_KEY
    allowed_dtypes: Dict = PYTHON_TO_SQL_DTYPES
    allowed_namedtuples: List = []


class ForeignkeyConfig(NamedTuple):
    """ Config for foreign keys in extended tables. """
    foreign_key: str
    reference_table: str
    reference_table_primary_key: str


class TableHandleData(NamedTuple):
    """ Config for table within database. """
    table_config: TableConfig
    attr_dtypes: Dict[str, type]
    column_dtypes: Dict[str, type]
    foreign_keys: List[ForeignkeyConfig] = []


class BaseTableHandle(BaseHandle):
    """ Class to house common database functionality. """

    def __init__(
        self,
        table_config: TableConfig,
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
        self.db_file_name, table_config = generate_name(
            db_file_name=db_file_name,
            table_config=table_config
        )
        # setting up db file
        super().__init__(
            db_file_name=self.db_file_name,
            db_dir=db_dir
        )

        self.table_handle_data, extended_data = self.init_table(
            table_config=table_config
        )
        self.extended_table_handles_data = self.init_extended_tables(
            table_config=table_config,
            extended_data=extended_data
        )

    def init_table(self, table_config):
        default_nt_instance = table_config.named_tuple_type()

        # getting named_tuple/table data types
        _, attr_dtypes, extended_data = extract_attrs(
            complex_nt=default_nt_instance,
            allowed_data_types=[*table_config.allowed_dtypes.keys()],
            allowed_named_tuples=table_config.allowed_namedtuples
        )

        # creating table column types
        table_column_dtypes = attr_dtypes.copy()
        table_column_dtypes[table_config.primary_key] = int

        table_data_handle = TableHandleData(
            table_config=table_config,
            attr_dtypes=attr_dtypes,
            column_dtypes=table_column_dtypes
        )

        # building table
        self.build_table(table_data_handle=table_data_handle)

        return table_data_handle, extended_data

    def init_extended_tables(
        self,
        table_config: TableConfig,
        extended_data: Dict
    ):
        extended_table_handles_data = {}
        for extended_data_name, extended_data_metadata in extended_data.items():
            extended_namedtuple_type = extended_data_metadata[NAMED_TUPLE_DTYPE]
            # getting named_tuple/table data types
            _, attr_dtypes, extended_data = extract_attrs(
                complex_nt=extended_namedtuple_type(),
                allowed_data_types=[*table_config.allowed_dtypes.keys()],
                allowed_named_tuples=table_config.allowed_namedtuples
            )

            # extended data is not permitted to have extended data
            if extended_data:
                raise ValueError(EXTENDED_DATA_EXTENDED_DATA_ERROR)

            # creating table column types
            table_column_dtypes = attr_dtypes.copy()
            for key in [table_config.primary_key, EXTENDED_DATA_PRIMARY_KEY]:
                table_column_dtypes[key] = int

            # creating table_handle_data
            table_name = f"{table_config.table_name}_{extended_data_name}"
            table_handle_data = TableHandleData(
                table_config=TableConfig(
                    named_tuple_type=extended_namedtuple_type,
                    table_name=table_name,
                    primary_key=EXTENDED_DATA_PRIMARY_KEY,
                    allowed_dtypes=table_config.allowed_dtypes,
                    allowed_namedtuples=table_config.allowed_namedtuples
                ),
                attr_dtypes=attr_dtypes,
                column_dtypes=table_column_dtypes,
                foreign_keys=[ForeignkeyConfig(
                    foreign_key=table_config.primary_key,
                    reference_table=table_config.table_name,
                    reference_table_primary_key=table_config.primary_key
                )]
            )
            extended_table_handles_data[extended_data_name] = table_handle_data

            # building table
            self.build_table(table_handle_data)

        return extended_table_handles_data

    def table_exists(self, table_name: str = None):
        """ Returns true if table_name exists as a table in the database.

        Params:
            table_name: Name of table in database, not required if table_name
             is passed into super constructor.
        """
        # feat can not be used in init.
        if not table_name:
            table_name = self.table_handle_data.table_config.table_name

        query = f"SELECT name FROM sqlite_master " \
                f"WHERE type='table' AND name='{table_name}'"
        result = self.execute_query(query)
        df = pd.DataFrame(result)
        if len(df) == 1:
            return True
        return False

    def build_table(
        self,
        table_data_handle: TableHandleData
    ) -> None:
        """ Builds table from NamedTuple with set default values.

        Params:
            table_data: config object to create database table.
            table_column_dtypes: data types for each column of the table.
        """
        if self.table_exists(table_name=table_data_handle.table_config.table_name):
            return

        build_query = f"CREATE TABLE {table_data_handle.table_config.table_name} ("

        # adding columns (attrs, primary key, foreign keys)
        for attr_name in table_data_handle.column_dtypes:
            pk_tag = "PRIMARY KEY" if attr_name == table_data_handle.table_config.primary_key else ""
            col_dtype = PYTHON_TO_SQL_DTYPES[table_data_handle.column_dtypes[attr_name]]
            build_query += f"[{attr_name}] {col_dtype} {pk_tag}, "

        # add foreign key references
        if table_data_handle.foreign_keys:
            for foreign_key in table_data_handle.foreign_keys:
                build_query += f"FOREIGN KEY({foreign_key.foreign_key}) " \
                               f"REFERENCES {foreign_key.reference_table}(" \
                               f"{foreign_key.reference_table_primary_key}) " \
                               f"ON DELETE CASCADE, "
        build_query = build_query[:-2]  # removing last ', '
        build_query += ")"
        # print(build_query)
        self.execute_query(build_query)

    def get_all(self) -> Dict[str, pd.DataFrame]:
        """ Returns table contents as dataframe. """
        table_name = self.table_handle_data.table_config.table_name
        primary_key = self.table_handle_data.table_config.primary_key

        table_data = {
            table_name: self._get_all(
                table_name=table_name,
                primary_key=primary_key
            )
        }

        if self.extended_table_handles_data:
            for data_name, table_data_handle in self.extended_table_handles_data.items():
                table_name = table_data_handle.table_config.table_name
                table_data[table_name] = self._get_all(
                        table_name=table_name,
                        primary_key=table_data_handle.table_config.primary_key
                    )

        return table_data

    def _get_all(self, table_name: str, primary_key: str):
        query = f"PRAGMA table_info({table_name})"
        results = self.execute_query(query)
        column_names = [col[1] for col in results]
        query = f"SELECT * FROM {table_name}"
        result = self.execute_query(query)
        df = pd.DataFrame(
            result, columns=column_names
        ).set_index(primary_key)
        return df

    def insert(
        self,
        named_tuple: NamedTuple,
        primary_key: int = None,
    ) -> None:
        """ """
        if not primary_key:
            primary_key = self.get_last_primary_key()

        attr_pool_values, _, extended_data = extract_attrs(
            complex_nt=named_tuple,
            allowed_data_types=[*self.table_handle_data.table_config.allowed_dtypes],
            allowed_named_tuples=self.table_handle_data.table_config.allowed_namedtuples
        )

        self._insert(
            table_name=self.table_handle_data.table_config.table_name,
            primary_key={self.table_handle_data.table_config.primary_key: primary_key},
            attr_pool_values=attr_pool_values,
        )

        if extended_data:
            for extended_data_name, meta_data in extended_data.items():
                table_handle_data = self.extended_table_handles_data[extended_data_name]
                for named_tuple in meta_data[EXTENDED_DATA_VALUE]:
                    attr_pool_values, _, extended_data = extract_attrs(
                        complex_nt=named_tuple,
                        allowed_data_types=[*table_handle_data.table_config.allowed_dtypes],
                        allowed_named_tuples=table_handle_data.table_config.allowed_namedtuples
                    )
                    extended_primary_key = self.get_last_primary_key(
                        table_config=table_handle_data.table_config
                    ) + 1
                    self._insert(
                        table_name=table_handle_data.table_config.table_name,
                        primary_key={table_handle_data.table_config.primary_key: extended_primary_key},
                        foreign_keys={self.table_handle_data.table_config.primary_key: primary_key},
                        attr_pool_values=attr_pool_values,
                    )

    def _insert(
        self,
        table_name: str,
        primary_key: Dict[str, int],
        attr_pool_values: Dict[str, Any],
        foreign_keys: Dict[str, int] = None,
    ):
        # ensuring primary_key is size 1
        if len(primary_key) != 1:
            raise ValueError(INSERT_PRIMARY_KEY_ERROR)

        if not foreign_keys:
            foreign_keys = {}

        columns = "("
        values = "VALUES("

        # add primary key
        for key_name, key_value in primary_key.items():
            columns += f"{key_name}"
            values += f"{key_value}"
        # add foreign keys
        for key_name, key_value in foreign_keys.items():
            columns += f", {key_name}"
            values += f", {key_value}"
        # add data columns
        for column, value in attr_pool_values.items():
            columns += f", {column}"
            values += f", "
            if isinstance(value, str):
                values += f"'{value}'"
            else:
                values += f"{value}"
        columns += ")"
        values += ")"
        insert_query = f"INSERT INTO {table_name} {columns} {values}"
        # print(insert_query)
        self.execute_query(insert_query)

    def get_last_primary_key(self, table_config: TableConfig = None):
        if not table_config:
            table_config = self.table_handle_data.table_config

        table_name = table_config.table_name
        if self.table_exists(table_name=table_name):
            last_key_query = f"SELECT MAX({table_config.primary_key}) FROM {table_name}"
            res = self.execute_query(last_key_query)
            last_pk = res[0][0]
            last_pk = last_pk if last_pk else 0
            return last_pk
        else:
            raise ValueError(TABLE_DOESNT_EXIST + table_name)

    def remove(self, primary_key: int = None):
        """ Removes last row if no primary_key is passed in. """
        table_name = self.table_handle_data.table_config.table_name
        primary_key_name = self.table_handle_data.table_config.primary_key

        if self.table_exists(table_name=table_name):
            if not primary_key:
                primary_key = self.get_last_primary_key()
            self._remove(
                table_name=table_name,
                key_name=primary_key_name,
                key_value=primary_key
            )

            if self.extended_table_handles_data:
                for extended_data_name, table_data_handle in self.extended_table_handles_data.items():
                    print(f"removing from {table_name} {primary_key} {primary_key_name}")
                    self._remove(
                        table_name=table_data_handle.table_config.table_name,
                        key_name=primary_key_name,
                        key_value=primary_key
                    )

        else:
            raise ValueError(TABLE_DOESNT_EXIST + table_name)

    def _remove(self, table_name, key_name, key_value):
        delete_query = f"DELETE FROM {table_name} " \
                       f"WHERE {key_name} = {key_value}"
        # print(delete_query)
        self.execute_query(delete_query)


def generate_name(
    db_file_name: Union[str, None],
    table_config: Union[TableConfig, None]
) -> Tuple[str, TableConfig]:
    if not table_config.named_tuple_type:
        raise ValueError(MISSING_NT_TYPE)

    db_file_type = ".sqlite"
    name = get_table_name(table_config.named_tuple_type.__name__)
    if not db_file_name:
        db_file_name = name+db_file_type
    # rebuilding table_config with generated table name.
    if not table_config.table_name:
        table_config_dict = table_config._asdict()
        del table_config_dict["table_name"]
        table_config = TableConfig(table_name=name, **table_config_dict)
    return db_file_name, table_config
