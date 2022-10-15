""" Class to track the number of raw_processed_articles released per stock.

Written by: Bart Nowobilski.
"""

from news_scanner.news_scanner import NewsReport
from typing import List


class ArticleCounter:
    def __init__(self):
        """ Initializes dict of key ticker, value article count. """
        self.runtime_article_counts = dict()

    def count_stock_articles(self, news_reports: List[NewsReport]):
        """ Counts the amount of raw_processed_articles per stock.

        Params:
            news_reports: List of NewsReport.
        """
        for news_report in news_reports:
            key = news_report.nameData.ticker
            if key in self.runtime_article_counts:
                self.runtime_article_counts[key] += 1
            else:
                self.runtime_article_counts[key] = 1
