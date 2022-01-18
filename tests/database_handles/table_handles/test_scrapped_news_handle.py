""" Validates 'ScrappedNewsHandle' class. """

from news_scanner.database_handles.table_handles.\
    stock_data_processed_news_tables.scrapped_news_handle import \
    ScrappedNewsHandle, ScrappedNewsResult
from tests.util import tear_down
from tests.constants import (
    TEST_DATABASE_DIR,
    TEST_STOCK_DATA_NEWS_DATABASE_PATH
)


def test_init():
    """ Ensures table is constructed. """
    db_handle = ScrappedNewsHandle(
        database_path=TEST_STOCK_DATA_NEWS_DATABASE_PATH,
        database_dir=TEST_DATABASE_DIR
    )
    assert db_handle.table_exists()
    tear_down()


def test_insert():
    """ Ensures table insertions. """
    db_handle = ScrappedNewsHandle(
        database_path=TEST_STOCK_DATA_NEWS_DATABASE_PATH,
        database_dir=TEST_DATABASE_DIR
    )
    agg_id = 1
    scrapped_result = ScrappedNewsResult()
    db_handle.insert(
        agg_id=agg_id,
        scrapped_result=scrapped_result
    )
    df = db_handle.get_all()
    assert len(df) == 1
    assert df['agg_id'][0] == agg_id
    assert df['headline'][0] == scrapped_result.headline
    assert df['link'][0] == scrapped_result.link
    assert df['publish_date'][0] == scrapped_result.publish_date
    assert df['content'][0] == scrapped_result.content
    tear_down()
