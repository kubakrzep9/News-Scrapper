""" Database handle to table controlling main system power. """

import pandas as pd
from pathlib import Path
from typing import NamedTuple
from news_scanner.database.table_handles.base_table_handle import (
    BaseTableHandle,
    TableConfig,
    DB_DIR
)
from news_scanner.database.util import get_table_name


ON = 1
OFF = 0
# column names
POWER = "power"
ID = "id"


class PowerSwitch(NamedTuple):
    power: int = ON


FORMATTED_NAME = get_table_name(PowerSwitch.__name__)
DB_FILE_NAME = f"{FORMATTED_NAME}.sqlite"

TABLE_DATA = TableConfig(
    named_tuple_type=PowerSwitch,
    table_name=FORMATTED_NAME,
    primary_key=ID
)


# Technically TableHandle but the table resides in its own database.
class PowerSwitchHandle(BaseTableHandle):
    """ Database handle to table controlling main system power. """
    _power_index = 0

    def __init__(
        self,
        db_file_name: str = DB_FILE_NAME,
        db_dir: Path = DB_DIR
    ):
        """ Initializes database and tables.

        Params:

        """
        super().__init__(
            table_config=TABLE_DATA,
            db_file_name=db_file_name,
            db_dir=db_dir
        )  # must be first
        self._init_table()

    def _init_table(self):
        """ Creates and initializes table if it does not exist. """
        num_rows = len(self.get_all()[self.table_handle_data.table_config.table_name])
        if num_rows == 0:
            self.insert(
                named_tuple=PowerSwitch(power=ON),
                primary_key=self._power_index
            )

    def set_power(self, on: bool) -> None:
        """ Sets system power.

        Params:
            on: Bool indicating if the system should be on or off.
        """
        mode = 0
        if on:
            mode = 1
        query = f"UPDATE {self.table_handle_data.table_config.table_name} SET {POWER} = {mode}" \
                f" WHERE {ID} = {self._power_index}"
        self.execute_query(query)

    def power_on(self) -> bool:
        """ Returns true if the table indicates the system power is on. """
        query = f"SELECT {ID}, {POWER} FROM {self.table_handle_data.table_config.table_name}"
        result = self.execute_query(query)
        df = pd.DataFrame(result, columns=[ID, POWER])
        if df.iloc[self._power_index][POWER] == ON:
            return True
        return False
