""" Validates 'HeadlineKeywords' class. """

from news_scanner.database_handles.table_handles.\
    stock_data_processed_news_tables.headline_keywords_handle \
    import HeadlineKeywordsHandle
from tests.util import tear_down
from tests.constants import (
    TEST_DATABASE_DIR,
    TEST_STOCK_DATA_NEWS_DATABASE_PATH
)


def test_init():
    """ Ensures table is constructed. """
    db_handle = HeadlineKeywordsHandle(
        database_path=TEST_STOCK_DATA_NEWS_DATABASE_PATH,
        database_dir=TEST_DATABASE_DIR
    )
    assert db_handle.table_exists()
    tear_down()


def test_insert():
    """ Ensures table insertions. """
    db_handle = HeadlineKeywordsHandle(
        database_path=TEST_STOCK_DATA_NEWS_DATABASE_PATH,
        database_dir=TEST_DATABASE_DIR
    )

    primary_key = 1
    agg_id = 2
    keyword = "TSLA"
    count = 1000
    db_handle.insert(
        primary_key=primary_key,
        agg_id=agg_id,
        keyword=keyword,
        count=count,
    )
    df = db_handle.get_all()
    print(df.columns)
    assert len(df) == 1
    assert df['id'][0] == primary_key
    assert df['agg_id'][0] == agg_id
    assert df['keyword'][0] == keyword
    assert df['count'][0] == count
    tear_down()
