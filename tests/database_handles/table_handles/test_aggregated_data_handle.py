""" Validates 'AggregatedDataHandle' class. """

from news_scanner.database_handles.table_handles.\
    stock_data_processed_news_tables.aggregated_data_handle \
    import AggregatedDataHandle
from tests.util import tear_down
from tests.constants import (
    TEST_DATABASE_DIR,
    TEST_STOCK_DATA_NEWS_DATABASE_PATH
)


def test_init():
    """ Ensures table is constructed. """
    db_handle = AggregatedDataHandle(
        database_path=TEST_STOCK_DATA_NEWS_DATABASE_PATH,
        database_dir=TEST_DATABASE_DIR
    )
    assert db_handle.table_exists()
    tear_down()


def test_insert():
    """ Ensures table insertions. """
    db_handle = AggregatedDataHandle(
        database_path=TEST_STOCK_DATA_NEWS_DATABASE_PATH,
        database_dir=TEST_DATABASE_DIR
    )
    agg_id = 1
    ticker = "TKR"
    exchange = "EXHE"
    db_handle.insert(
        agg_id=agg_id,
        ticker=ticker,
        exchange=exchange,
    )
    df = db_handle.get_all()
    assert len(df) == 1
    assert df['agg_id'][0] == agg_id
    assert df['ticker'][0] == ticker
    assert df['exchange'][0] == exchange
    tear_down()
