""" Functions to process the contents of a news article. """

from typing import List, Tuple, Dict, NamedTuple
import re
import news_scanner.news_scrapper.target_news_scrapper as tns

ALLOWED_EXCHANGES = ["NASDAQ", "NYSE"]   # OTC, pink sheets?


class ProcessedNewsResult(NamedTuple):
    """ Result object containing processed article data.

    Attrs:
        article_keywords: Keywords found in article content.
        headline_keywords: Keywords found in headline.
    """
    article_keywords: Dict = {}
    headline_keywords: Dict = {}


def process_articles(
    scrape_results: List[tns.ScrappedNewsResult],
) -> Tuple[List[ProcessedNewsResult], List[tns.ScrappedNewsResult], List[str], List[str]]:
    """


    Param:
        scrape_results: List of scrapped Morning Star news content

    """
    results = []
    valid_scrape_results = []
    tickers = []
    exchanges = []
    for scrape_result in scrape_results:
        ticker_code, article_keywords = _process_content(scrape_result.content)
        if ticker_code is not None:
            exchange, ticker = _process_ticker_code(ticker_code)
            if exchange in ALLOWED_EXCHANGES:
                headline_keywords = {}  # _process_headline(scrape_result.headline)
                processed_result = ProcessedNewsResult(
                    article_keywords=article_keywords,
                    headline_keywords=headline_keywords,
                )
                results.append(processed_result)
                valid_scrape_results.append(scrape_result)
                tickers.append(ticker)
                exchanges.append(exchange)
    return results, valid_scrape_results, tickers, exchanges

def _process_ticker_code(ticker_code: str) -> Tuple[str, str]:
    exchange_index = 0
    ticker_index = 1
    tmp = re.sub("[()]", "", ticker_code)
    tmp = tmp.split(":")
    return tmp[exchange_index], tmp[ticker_index]



def _process_headline(headline: str) -> Dict:
    """

    Param:
        headline: A news articles headline
    """
    pass


def _process_content(content: str) -> Tuple[str, Dict]:
    """ Searches article content for tickers and key words.

    Param:
        contents:
        max_tickers:
    """
    ticker_codes = _find_ticker_codes(content)
    num_tickers = len(ticker_codes)
    if num_tickers != 1:
        return None, None

    # Find key words
    key_words = {}
    ticker = ticker_codes[0]
    return ticker, key_words


def _find_ticker_codes(content: str):
    """ Searches for tickers in article that exist as (EXCHANGE:TICKER). """
    ticker_pattern = r"[(][A-Z]+[ ]*[:][ ]*[A-Z]+[)]"
    matches = re.findall(ticker_pattern, content)
    ticker_codes = []
    for match in matches:
        ticker_codes.append(match.replace(" ", ""))
    return ticker_codes

