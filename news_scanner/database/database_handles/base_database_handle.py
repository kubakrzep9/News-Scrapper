""" Class to house common database functionality. """

from pathlib import Path
from typing import NamedTuple, Dict, List
from news_scanner.database.util import (
    validate_complex_nt_schema,
    validate_complex_nt_obj,
    get_namedtuple_name,
    extract_attrs,
    get_table_name
)
from news_scanner.database.constants import (
    PYTHON_TO_SQL_DTYPES,
    DEF_PRIMARY_KEY,
    DB_DIR
)
from news_scanner.database.base_handle import BaseHandle
from news_scanner.database.table_handles.base_table_handle import BaseTableHandle, TableConfig


DB_PK_ERROR = "Error: primary keys do not match in database tables."


class DBObjectConfig(NamedTuple):
    complex_nt_type: type
    allowed_dtypes: Dict = PYTHON_TO_SQL_DTYPES
    allowed_namedtuples: List = []
    primary_key: str = DEF_PRIMARY_KEY


class BaseDatabaseHandle(BaseHandle):
    """ Class to house common database functionality.

    This class will transform a complex-namedtuple (nested namedtuple) into a database.

    The first level of the complex-namedtuple must contain only namedtuples. Each of
    these named tuples will be transformed into a table within the database. The name
    of the namedtuple will be used as the name of the table.

    The attribute names of the remaining subsequent layers will be added as columns into the
    respective table. Remaining namedtuples will simply have their attribute names added
    as columns.

    This class manage all aspects of the database.
    """

    def __init__(
        self,
        db_object_config: DBObjectConfig,
        db_file_name: str = None,
        db_dir: Path = DB_DIR,
    ):
        """

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
        self.db_object_config = db_object_config
        self.def_db_obj_instance = validate_complex_nt_schema(
            complexnt_type=self.db_object_config.complex_nt_type,
            allowed_data_types=[*self.db_object_config.allowed_dtypes.keys()],
            allowed_namedtuple_types=self.db_object_config.allowed_namedtuples
        )

        # creating default filename
        if not db_file_name:
            db_file_name = f"{self.db_object_config.complex_nt_type.__name__}.sqlite"
        # setting up db file
        super().__init__(
            db_file_name=db_file_name,
            db_dir=db_dir
        )
        # turning the complex_nt into a table
        self.table_handles = {}
        for namedtuple in self.def_db_obj_instance:
            nt_name = get_table_name(type(namedtuple).__name__)
            self.table_handles[nt_name] = BaseTableHandle(
                table_config=TableConfig(
                    named_tuple_type=type(namedtuple),
                    allowed_namedtuples=self.db_object_config.allowed_namedtuples
                ),
                db_file_name=db_file_name,
                db_dir=db_dir
            )

    def insert(self, insert_data: List[NamedTuple], throw_exception: bool = True):
        """ """
        if type(insert_data) != list:
            insert_data = [insert_data]

        for complex_nt in insert_data:
            validate_complex_nt_obj(
                complex_nt=complex_nt,
                expected_complex_nt_type=self.db_object_config.complex_nt_type,
                expected_allowed_namedtuples=self.db_object_config.allowed_namedtuples
            )

            success_set = []
            primary_key = self.get_next_primary_key()

            try:
                for namedtuple in complex_nt:
                    nt_name = get_table_name(type(namedtuple).__name__)
                    self.table_handles[nt_name].insert(
                        named_tuple=namedtuple,
                        primary_key=primary_key
                    )
                    success_set.append(nt_name)
            except Exception as e:
                # transactional insert, remove successful inserts prior to failed insert
                for nt_name in success_set:
                    self.table_handles[nt_name].remove(primary_key)
                if throw_exception:
                    raise e
                else:
                    # LOG DATABASE INSERT FAIL
                    print(str(e))

    def get_next_primary_key(self):
        """ Checks each table to ensure they agree on next primary key. """
        table_handles = [*self.table_handles.items()]
        first_table_handle = [*self.table_handles.items()][0][1]
        last_pk = first_table_handle.get_last_primary_key()
        next_pk = last_pk + 1 if last_pk else 1

        for table_handle in table_handles[1:]:
            table_handle = table_handle[1]
            _last_pk = table_handle.get_last_primary_key()
            if last_pk != _last_pk:
                raise ValueError(DB_PK_ERROR)

        return next_pk

    def get_all(self):
        """" Returns dict of database where key is table_name and value is table data. """
        db_table_data = {}
        for table_name, table_handle in self.table_handles.items():
            db_table_data[table_name] = table_handle.get_all()

        return db_table_data

    def remove(self, primary_keys: List[int]):
        for pk in primary_keys:
            for _, table_handle in self.table_handles.items():
                table_handle.remove(primary_key=pk)
