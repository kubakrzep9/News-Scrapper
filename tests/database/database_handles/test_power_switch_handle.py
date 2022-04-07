""" """

import pytest
from news_scanner.database.database_handles.power_switch_handle import (
    PowerSwitchHandle, ON, OFF, POWER
)
from tests.database.conftest import TEST_DB_DIR
from tests.database.test_helper.util import tear_down


@pytest.fixture
def power_switch_handle() -> PowerSwitchHandle:
    tear_down()
    return PowerSwitchHandle(
        db_dir=TEST_DB_DIR
    )


def test_init(power_switch_handle: PowerSwitchHandle):
    assert power_switch_handle.table_exists()
    df = power_switch_handle.get_all()
    assert len(df) == 1
    assert df.iloc[power_switch_handle._power_index][POWER] == ON
    tear_down()


def test_init_table(power_switch_handle: PowerSwitchHandle):
    """Ensuring _init_table does nothing if already called before. """
    power_switch_handle._init_table()
    df = power_switch_handle.get_all()
    assert len(df) == 1
    assert df.iloc[power_switch_handle._power_index][POWER] == ON
    tear_down()


def test_set_power(power_switch_handle: PowerSwitchHandle):
    power_modes = [False, True]
    expected_power_values = [OFF, ON]
    for mode, expected in zip(power_modes,expected_power_values):
        power_switch_handle.set_power(mode)
        df = power_switch_handle.get_all()
        assert len(df) == 1
        assert df.iloc[power_switch_handle._power_index][POWER] == expected
    tear_down()


def test_power_on(power_switch_handle: PowerSwitchHandle):
    power_modes = [False, True]
    for mode in power_modes:
        power_switch_handle.set_power(mode)
        assert power_switch_handle.power_on() == mode
    tear_down()
