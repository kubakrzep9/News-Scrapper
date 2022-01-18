""" Validates 'StockDataHandle' class. """

from news_scanner.database_handles.table_handles.\
    stock_data_processed_news_tables.stock_data_handle import \
    StockDataHandle, StockData
from tests.util import tear_down
from tests.constants import (
    TEST_DATABASE_DIR,
    TEST_STOCK_DATA_NEWS_DATABASE_PATH
)


def test_init():
    """ Ensures table is constructed. """
    db_handle = StockDataHandle(
        database_path=TEST_STOCK_DATA_NEWS_DATABASE_PATH,
        database_dir=TEST_DATABASE_DIR
    )
    assert db_handle.table_exists()
    tear_down()


def test_insert():
    """ Ensures table insertions. """
    db_handle = StockDataHandle(
        database_path=TEST_STOCK_DATA_NEWS_DATABASE_PATH,
        database_dir=TEST_DATABASE_DIR
    )
    agg_id = 1
    stock_data = StockData(
        market_cap=1,
        market_cap_float=2,
        shares_outstanding=3,
        last_price=4
    )
    db_handle.insert(
        agg_id=agg_id,
        stock_data=stock_data
    )
    df = db_handle.get_all()
    assert len(df) == 1
    assert df[db_handle.PRIMARY_KEY][0] == agg_id
    assert df[db_handle.MARKET_CAP][0] == stock_data.market_cap
    assert df[db_handle.MARKET_CAP_FLOAT][0] == stock_data.market_cap_float
    assert df[db_handle.SHARES_OUTSTANDING][0] == stock_data.shares_outstanding
    assert df[db_handle.LAST_PRICE][0] == stock_data.last_price
    tear_down()
