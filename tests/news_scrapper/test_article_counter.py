""" This file is testing the article counter class.

Written by: Bart Nowobilski.
"""

from news_scanner.news_scrapper.article_counter import ArticleCounter
import pytest
from news_scanner.result_object import NewsReport, NameData
from typing import List, Dict


@pytest.mark.parametrize(
    "news_reports, expected_runtime_article_counts",
    [
        # first case
        (
            [
                NewsReport(nameData=NameData(ticker="XYZ")),
                NewsReport(nameData=NameData(ticker="ABC")),
                NewsReport(nameData=NameData(ticker="NOWO")),
                NewsReport(nameData=NameData(ticker="AAA")),
                NewsReport(nameData=NameData(ticker="XYZ")),
            ],
            {
                "XYZ": 2,
                "ABC": 1,
                "NOWO": 1,
                "AAA": 1,
            }
        ),
        # second case
        ([], {})
    ]
)
def test_article_counter(
    news_reports: List[NewsReport],
    expected_runtime_article_counts: Dict
):
    """ Ensures raw_processed_articles are properly counted for each stock.

    Params:
        news_reports: List of test data NewsReport.
        expected_runtime_article_counts: Dict of recorded article counts.
    """
    article_counter = ArticleCounter()
    article_counter.count_stock_articles(news_reports)
    assert article_counter.runtime_article_counts == \
           expected_runtime_article_counts


def test_article_counter_updated():
    """" Ensures article counter is updated properly. """
    news_reports_1 = [
        NewsReport(nameData=NameData(ticker="XYZ")),
        NewsReport(nameData=NameData(ticker="ABC")),
        NewsReport(nameData=NameData(ticker="NOWO")),
        NewsReport(nameData=NameData(ticker="AAA")),
        NewsReport(nameData=NameData(ticker="XYZ")),
    ]
    news_reports_2 = [
        NewsReport(nameData=NameData(ticker="XYZ")),
    ]
    expected_runtime_article_counts = {
        "XYZ": 3,
        "ABC": 1,
        "NOWO": 1,
        "AAA": 1,
    }

    article_counter = ArticleCounter()
    article_counter.count_stock_articles(news_reports_1)
    article_counter.count_stock_articles(news_reports_2)
    assert article_counter.runtime_article_counts == \
           expected_runtime_article_counts
