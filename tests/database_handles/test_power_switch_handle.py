""" Validates 'PowerSwitchHandle' class. """

from pathlib import Path
from news_scanner.database_handles.power_switch_handle import PowerSwitchHandle
from tests.util import tear_down
from tests.constants import TEST_DATABASE_DIR

_TEST_DATABASE_PATH = TEST_DATABASE_DIR / "power_switch.sqlite"


def test_init():
    """ Ensures database, table and power have been properly initialized."""
    db_handle = PowerSwitchHandle(
        database_path=_TEST_DATABASE_PATH,
        database_dir=TEST_DATABASE_DIR
    )
    db_dir_exists = Path(TEST_DATABASE_DIR).is_dir()
    assert db_dir_exists is True
    db_exists = Path(_TEST_DATABASE_PATH).is_file()
    assert db_exists is True
    assert db_handle.table_exists()
    assert db_handle.power_on() is True
    tear_down()


def test_set_power():
    """ Ensures power can be set in table. """
    db_handle = PowerSwitchHandle(
        database_path=_TEST_DATABASE_PATH,
        database_dir=TEST_DATABASE_DIR
    )
    assert db_handle.power_on() is True
    db_handle.set_power(False)
    assert db_handle.power_on() is False
    db_handle.set_power(True)
    assert db_handle.power_on() is True
    tear_down()


def test_power_on():
    """ Ensures power status can be retrieved from table. """
    db_handle = PowerSwitchHandle(
        database_path=_TEST_DATABASE_PATH,
        database_dir=TEST_DATABASE_DIR
    )
    assert db_handle.power_on() is True
    db_handle.set_power(False)
    assert db_handle.power_on() is False
    tear_down()
