
from mock import patch, MagicMock
import os
import responses
from news_scanner.news_scanner import NewsScanner
from news_scanner.td_api.td_api_handle import StockData
from regression_tests.mock_data_builder import MockDataBuilder
from news_scanner.config import Config
from news_scanner.news_scrapper.filter.article_filter import FilterCriteria

# Latest article is first
# handle case [exchange:ticker]
# - article_processor._process_ticker_code not working properly

# Add validation for publish dates and article
# - ensure system is not messing up order

LARGE_NUM = 10000000


@responses.activate
def test_run():
    """
    The method 'scan_news' is used instead of 'run' as 'run' calls 'scan_news',
    and 'scan_news' returns results from the scan used to validate behavior.

    The news scanner scans for news 'iterations' number of times. This test
    ensures the correct number of target articles are found.
    """
    print()
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
            database_on=False,
            multithreaded_on=False,
            keep_alive=1,
            ignore_warnings=False,
            config=config,
        )
        for i in range(iterations):
            news_results, news_reports = news_scanner.scan_news()
            if i == 0:
                assert len(news_results) == mock_data_builder.articles_per_iteration
            else:
                assert len(news_results) == (
                        mock_data_builder.new_invalid_articles_per_iteration +
                        mock_data_builder.new_valid_articles_per_iteration
                )
            assert len(news_reports) == mock_data_builder.new_valid_articles_per_iteration
            print(all_stock_data[i])

            print("news_results")
            print(news_results)
            print()
            print("news_reports")
            print(news_reports)
            print()

    for response in all_responses:
        assert response.call_count == 1

    for article in mock_data_builder.viewed_articles:
        url = f"{mock_data_builder.news_page_url}{article['url']}"
        assert url in news_scanner.news_scrapper.viewed_links
