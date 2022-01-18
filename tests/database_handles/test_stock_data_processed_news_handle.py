""" Tests functionality of 'StockDataProcessedNewsHandle' """

from typing import List
from pathlib import Path
import numpy as np
import pytest
from news_scanner.database_handles.stock_data_processed_news_handle import \
    StockDataProcessedNewsHandle
from news_scanner.result_object import NewsReport, ScrappedNewsResult, \
    ProcessedNewsResult, StockData
from tests.util import tear_down
from tests.constants import (
    TEST_DATABASE_DIR,
    TEST_STOCK_DATA_NEWS_DATABASE_PATH
)


@pytest.fixture
def newsreport_testdata1() -> NewsReport:
    """ Test data of NewsReport object. """
    return NewsReport(
        scrappedNewsResults=ScrappedNewsResult(
            headline="headline1",
            link="link1",
            publish_date="publish_date1",
            content="content1"
        ),
        processedNewsResults=ProcessedNewsResult(),
        stockData=StockData(
            market_cap=1.1,
            market_cap_float=1.2,
            shares_outstanding=1,
            last_price=1.3
        ),
        ticker="ticker1",
        exchange="exchange1"
    )


@pytest.fixture
def newsreport_testdata2() -> NewsReport:
    """ Test data of NewsReport object. """
    return NewsReport(
        scrappedNewsResults=ScrappedNewsResult(
            headline="headline2",
            link="link2",
            publish_date="publish_date2",
            content="content1"
        ),
        processedNewsResults=ProcessedNewsResult(),
        stockData=StockData(
            market_cap=2.1,
            market_cap_float=2.2,
            shares_outstanding=2,
            last_price=2.3
        ),
        ticker="ticker2",
        exchange="exchange2"
    )


def test_init():
    """ Ensures database and tables are properly initialized. """
    db_handle = StockDataProcessedNewsHandle(
        database_path=TEST_STOCK_DATA_NEWS_DATABASE_PATH,
        database_dir=TEST_DATABASE_DIR
    )
    db_exists = Path(TEST_STOCK_DATA_NEWS_DATABASE_PATH).is_file()
    assert db_exists is True
    assert db_handle.aggregated_data_handle.table_exists()
    assert db_handle.scrapped_news_handle.table_exists()
    assert db_handle.stock_data_handle.table_exists()
    tear_down()


def test_insert(newsreport_testdata1: NewsReport):
    """ Ensures that NewsReport objects are properly store in the database.

    Params:
        newsreport_testdata1: Test data of NewsReport object.
    """

    db_handle = StockDataProcessedNewsHandle(
        database_path=TEST_STOCK_DATA_NEWS_DATABASE_PATH,
        database_dir=TEST_DATABASE_DIR
    )
    db_handle.insert([newsreport_testdata1])
    results = db_handle.get_all()
    assert len(results) == 1
    tear_down()


@pytest.mark.parametrize(
    "news_reports_fixtures, expected_agg_id",
    [
        ([], None),
        (["newsreport_testdata1"], 1),
        (["newsreport_testdata1", "newsreport_testdata2"], 2),
    ]
)
def test_get_last_agg_id(
    news_reports_fixtures: List[NewsReport],
    expected_agg_id: int,
    request
):
    """ Ensures that the last agg_id can be retrieved from the database.

    Params:
        news_reports_fixtures: A list of NewsReport test data.
        expected_agg_id: Expected agg_id from table.
        request: pytest object to retrieve fixture values.
    """
    db_handle = StockDataProcessedNewsHandle(
        database_path=TEST_STOCK_DATA_NEWS_DATABASE_PATH,
        database_dir=TEST_DATABASE_DIR
    )
    news_reports = []
    for news_report in news_reports_fixtures:
        news_reports.append(request.getfixturevalue(news_report))

    db_handle.insert(news_reports)
    agg_id = db_handle._get_last_agg_id()
    assert agg_id == expected_agg_id
    tear_down()


def test_get_all(
    newsreport_testdata1: NewsReport,
    newsreport_testdata2: NewsReport,
):
    """ Ensuring table data can be properly retrieved.

    Params:
        newsreport_testdata1: Test data of NewsReport object.
        newsreport_testdata2: Test data of NewsReport object.
    """
    news_reports = [newsreport_testdata1, newsreport_testdata2]
    expected_cols = ['agg_id', 'ticker', 'exchange', 'headline', 'link',
                     'publish_date', 'content', 'market_cap',
                     'market_cap_floating', 'shares_outstanding', 'last_price']

    db_handle = StockDataProcessedNewsHandle(
        database_path=TEST_STOCK_DATA_NEWS_DATABASE_PATH,
        database_dir=TEST_DATABASE_DIR
    )
    db_handle.insert(news_reports)
    results = db_handle.get_all()

    assert np.all(results.columns == expected_cols)
    assert len(results) == len(news_reports)
    for i in range(len(news_reports)):
        # agg_id starts from 1
        agg_id = i+1
        scrape_result = news_reports[i].scrappedNewsResults
        stock_data = news_reports[i].stockData

        assert results["agg_id"][i] == agg_id
        assert results["ticker"][i] == news_reports[i].ticker
        assert results["exchange"][i] == news_reports[i].exchange
        assert results["headline"][i] == scrape_result.headline
        assert results["link"][i] == scrape_result.link
        assert results["publish_date"][i] == scrape_result.publish_date
        assert results["content"][i] == scrape_result.content
        assert results["market_cap"][i] == stock_data.market_cap
        assert results["market_cap_floating"][i] == stock_data.market_cap_float
        assert results["shares_outstanding"][i] == \
               stock_data.shares_outstanding
        assert results["last_price"][i] == stock_data.last_price
        tear_down()
