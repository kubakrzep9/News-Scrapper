
from mock import patch, MagicMock
import os
import responses
from news_scanner.news_scanner import NewsScanner
from news_scanner.td_api.td_api_handle import StockData
from regression_tests.mock_data_builder import MockDataBuilder
from news_scanner.config import Config
from news_scanner.news_scrapper.filter.article_filter import FilterCriteria
from pathlib import Path
from tests.database.test_helper.util import tear_down

# Latest article is first


TEST_DIR = Path(__file__).parent
TEST_DATABASE_DIR = TEST_DIR / "databases"
LARGE_NUM = 10000000


@responses.activate
def test_run_db_insert():
    """
    The method 'scan_news' is used instead of 'run' as 'run' calls 'scan_news',
    and 'scan_news' returns results from the scan used to validate behavior.
    """
    print()

    tear_down(
        dir_containing_log_dir=TEST_DIR,
        database_dir=TEST_DATABASE_DIR
    )

    iterations = 3
    config = Config()
    mock_data_builder = MockDataBuilder(
        news_page_url=config.website_url,
        iterations=iterations
    )
    test_data_summary = "Test data:\n" \
                        f"- iterations: {mock_data_builder.iterations}\n"\
                        f"- articles_per_iteration: {mock_data_builder.articles_per_iteration}\n"\
                        f"- new_invalid_articles_per_iteration: {mock_data_builder.new_invalid_articles_per_iteration}\n"\
                        f"- new_valid_articles_per_iteration: {mock_data_builder.new_valid_articles_per_iteration}"
    print(test_data_summary)

    news_page_request_responses, article_request_responses, all_stock_data = mock_data_builder.build_mock_data()
    all_responses = []
    for request_response in [*news_page_request_responses, *article_request_responses]:
        response = responses.get(
            url=request_response["url"],
            body=request_response["body"]
        )
        all_responses.append(response)
    # Mocking handle to TD Api
    mock_stock_data_config = {"get_stock_data.side_effect": all_stock_data}
    mock_td_api = MagicMock(**mock_stock_data_config)
    with patch("news_scanner.news_scanner.TDApiHandle", return_value=mock_td_api) as _:
        print()
        news_scanner = NewsScanner(
            filter_criteria=FilterCriteria(
                market_cap=LARGE_NUM,
                shares_outstanding=LARGE_NUM,
                last_price=LARGE_NUM
            ),
            proxy_on=False,
            twitter_on=False,
            database_on=True,
            multithreaded_on=False,
            keep_alive=1,
            ignore_warnings=False,
            config=config,
            database_dir=TEST_DATABASE_DIR
        )
        for i in range(iterations):
            news_results, news_reports = news_scanner.scan_news()

    table_data = news_scanner.database_handle.get_all()
    #print(table_data)
    for table_name, data in table_data.items():
        print(table_name)
        for t, d in data.items():
            print(t)
            print(d)
        print()

    print(all_stock_data)

