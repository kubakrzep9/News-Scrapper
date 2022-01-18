""" Database handle to table controlling main system power. """

import pandas as pd
from pathlib import Path
from news_scanner.database_handles.base_handle import BaseHandle, DATABASE_DIR

_DATABASE_PATH = DATABASE_DIR / "power_switch.sqlite"
_ON = 1
_OFF = 0


class PowerSwitchHandle(BaseHandle):
    """ Database handle to table controlling main system power. """

    TABLE_NAME = "power_switch"

    # column names
    POWER = "power"
    ID = "id"

    _power_index = 0

    def __init__(
        self,
        database_path: Path = _DATABASE_PATH,
        database_dir: Path = DATABASE_DIR
    ):
        """ Initializes database and tables.

        Params:
            database_path: Path to database file.
            database_dir: Path to directory which will house database files.
        """
        super().__init__(
            database_path=database_path,
            database_dir=database_dir,
            table_name=self.TABLE_NAME
        )  # must be first
        try:
            self._init_table()
        except Exception as e:
            print(f"Error building {self.TABLE_NAME} table.", str(e))

    def _init_table(self):
        """ Creates and initializes table if it does not exist. """
        if not self.table_exists(self.TABLE_NAME):
            table_query = f"CREATE TABLE {self.TABLE_NAME}(" \
                          f"[{self.ID}] INTEGER PRIMARY KEY, " \
                          f"[{self.POWER}] INTEGER)"
            insert_query = f"INSERT INTO {self.TABLE_NAME} " \
                           f"({self.ID}, {self.POWER}) " \
                           f"VALUES({self._power_index},{_ON})"
            self.execute_query([table_query, insert_query])

    def set_power(self, on: bool) -> None:
        """ Sets system power.

        Params:
            on: Bool indicating if the system should be on or off.
        """
        mode = 0
        if on:
            mode = 1
        query = f"UPDATE {self.TABLE_NAME} SET {self.POWER} = {mode} WHERE " \
                f"{self.ID} = {self._power_index}"
        self.execute_query(query)

    def power_on(self) -> bool:
        """ Returns true if the table indicates the system power is on. """
        query = f"SELECT {self.ID}, {self.POWER} FROM {self.TABLE_NAME}"
        result = self.execute_query(query)
        df = pd.DataFrame(result, columns=[self.ID, self.POWER])
        if df.iloc[self._power_index][self.POWER] == _ON:
            return True
        return False
