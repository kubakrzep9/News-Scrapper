""" Contains final result object used by 'NewsScanner' class. """

from news_scanner.news_scrapper.target_news_scrapper import ScrappedNewsResult
from news_scanner.news_scrapper.article_processor import ProcessedNewsResult
from news_scanner.td_api.td_api_handle import StockData
from typing import NamedTuple


class NewsReport(NamedTuple):
    """ Result object of scrapped news articles.

    Attrs:
        scrappedNewsResults: Result object containing data about a scrapped article.
        processedNewsResults: Result object containing processed article data.
        stockData: Result object containing stock data.
        ticker: Stock symbol.
        exchange: Exchange stock is traded on.
    """
    scrappedNewsResults: ScrappedNewsResult = ScrappedNewsResult
    processedNewsResults: ProcessedNewsResult = ProcessedNewsResult
    stockData: StockData = StockData
    ticker: str = "ticker"
    exchange: str = "exchange"
