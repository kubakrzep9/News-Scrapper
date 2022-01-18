""" This file is testing the article counter class.

Written by: Bart Nowobilski.
"""

from news_scanner.news_scrapper.article_counter import ArticleCounter
import pytest
from news_scanner.news_scanner import NewsReport
from typing import List, Dict


@pytest.mark.parametrize(
    "news_reports, expected_runtime_article_counts",
    [
        # first case
        (
            [
                NewsReport(ticker="XYZ"),
                NewsReport(ticker="ABC"),
                NewsReport(ticker="NOWO"),
                NewsReport(ticker="AAA"),
                NewsReport(ticker="XYZ"),
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
    """ Ensures articles are properly counted for each stock.

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
        NewsReport(ticker="XYZ"),
        NewsReport(ticker="ABC"),
        NewsReport(ticker="NOWO"),
        NewsReport(ticker="AAA"),
        NewsReport(ticker="XYZ"),
    ]
    news_reports_2 = [
        NewsReport(ticker="XYZ"),
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
