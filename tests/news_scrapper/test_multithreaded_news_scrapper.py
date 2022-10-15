from news_scanner.news_scrapper.multithreaded_target_news_scrapper import MultiThreadedTargetNewsScrapper
from news_scanner.news_scrapper.target_news_scrapper import ScrappedNewsResult
from typing import List
import datetime
from unittest.mock import patch
import pytest


class ResultsTracker:
    def __init__(self):
        self.results = []
        self.iterations = 0

    def add_results(self, headlines, links, publish_dates):
        results = []
        contents = create_mock_article_content(links)
        for headline, link, publish_date, content in zip(
            headlines, links, publish_dates, contents
        ):
            results.append(
                ScrappedNewsResult(
                    headline=headline,
                    link=link,
                    publish_date=publish_date,
                    content=content
                )
            )

        if self.iterations == 0:
            self.results.append(results)
        else:
            new_results = []
            for result in results:
                result_exists = False
                for i in range(self.iterations):
                    if result in self.results[i] or result_exists:
                        result_exists = True
                if not result_exists:
                    new_results.append(result)
            self.results.append(new_results)
        self.iterations += 1


# global variables to track test state and expected results
RESULTS_TRACKER = ResultsTracker()
DATA_OFFSET = 0
DATA_OFFSET_VALUE = 5


def create_mock_article_content(links: List[str]):
    """ Creates mock content from a list of links.

    Params:
        links: List of links built from 'generate_mock_headlines'
    """
    content = []
    for link in links:
        _id = link[1:]
        content.append(f"c{_id}")
    return content


def generate_mock_headlines():
    """ Adhoc generator using global variable."""
    global DATA_OFFSET
    global DATA_OFFSET_VALUE
    global RESULTS_TRACKER

    # using slice [::-1] to reverse lists to imitate real data order
    headlines = [f"h{i + DATA_OFFSET}" for i in range(50)][::-1]
    links = [f"l{i + DATA_OFFSET}" for i in range(50)][::-1]
    publish_dates = [datetime.datetime(2022, 6, 27) + datetime.timedelta(hours=idx + DATA_OFFSET) for idx in range(50)][::-1]

    RESULTS_TRACKER.add_results(headlines, links, publish_dates)
    return headlines, links, publish_dates


def test_init():
    website_url = "website"
    scrapper_api_key = "scrapper api key"
    proxy_on = True
    scrapper = MultiThreadedTargetNewsScrapper(
        website_url=website_url,
        scrapper_api_key=scrapper_api_key,
        proxy_on=proxy_on
    )
    assert scrapper.website_url == website_url
    assert scrapper.scrapper_api_key == scrapper_api_key
    assert scrapper.proxy_on == proxy_on


@pytest.mark.parametrize(
    "new_content", [True, False]
)
def test_get_news(new_content):
    """

    """
    print()
    if new_content:
        print("Test where new data is added each iteration.")
    else:
        print("Test where data is only added the first iteration")

    global DATA_OFFSET
    global RESULTS_TRACKER
    DATA_OFFSET = 0
    RESULTS_TRACKER = ResultsTracker()

    scrapper = MultiThreadedTargetNewsScrapper(
        website_url="website_url",
        scrapper_api_key="scrapper_api_key",
    )

    # validating results are what is expected
    with patch.object(scrapper, "_get_headline_data", new=generate_mock_headlines):
        with patch.object(scrapper, "_get_article_content", new=create_mock_article_content):
            for i in range(3):
                results = scrapper.get_news()
                expected = RESULTS_TRACKER.results[i]
                print()
                print(f"\texpected: {expected}")
                print(f"\tresults:  {results}")
                assert results == expected
                if new_content:
                    DATA_OFFSET += DATA_OFFSET_VALUE

    # validating all found links are tracked
    for result_set in RESULTS_TRACKER.results:
        for result in result_set:
            assert result.link in scrapper.viewed_links






