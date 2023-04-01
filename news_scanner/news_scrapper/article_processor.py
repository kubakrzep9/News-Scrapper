""" Functions to process the contents of a news article. """

from typing import List, Tuple, Dict, NamedTuple
import re
from news_scanner.news_scrapper.target_news_scrapper import ScrappedNewsResult

ALLOWED_EXCHANGES = ["NASDAQ", "NYSE"]   # OTC, pink sheets?


class WordCount(NamedTuple):
    """ Keeps track of the number of occurrences of a word.

    Attrs:
        word: Word to track.
        count: Number of occurrences of a word.

    """
    word: str = "word"
    count: int = 0


class ProcessedNewsResult(NamedTuple):
    """ Result object containing processed article data.

    Attrs:
        article_keywords: Keywords found in article content.
        headline_keywords: Keywords found in headline.
        sentiment: Rating between -1 to 1 indicating whether an article
            is positive or negative.
        article_type: Category indicating the article type.
    """
    article_keywords: List[WordCount] = [WordCount()]
    headline_keywords: List[WordCount] = [WordCount()]
    sentiment: float = 0.01
    article_type: str = "article_type"


def process_articles(
    scrape_results: List[ScrappedNewsResult],
) -> Tuple[
        List[ProcessedNewsResult], List[ScrappedNewsResult],
        List[str], List[str]
]:
    """ Returns the processed article, article, ticker and exchange.

    Articles must refer to a single stock to be accepted.

    Param:
        scrape_results: List of scrapped news content
    """
    processed_results = []
    valid_scrape_results = []
    tickers = []
    exchanges = []
    for scrape_result in scrape_results:
        ticker_code, article_keywords = _process_content(scrape_result.content)
        if ticker_code is not None:
            exchange, ticker = _process_ticker_code(ticker_code)
            if exchange in ALLOWED_EXCHANGES:
                headline_keywords = _process_headline(scrape_result.headline)
                processed_result = ProcessedNewsResult(
                    article_keywords=article_keywords,
                    headline_keywords=headline_keywords,
                )
                processed_results.append(processed_result)
                valid_scrape_results.append(scrape_result)
                tickers.append(ticker)
                exchanges.append(exchange)
    return processed_results, valid_scrape_results, tickers, exchanges


def _process_ticker_code(ticker_code: str) -> Tuple[str, str]:
    """ Returns the exchange and ticker from a ticker_code.

    TODO:
    - handle [<exchange>:<ticker>]

    Params:
        ticker_code: Ticker code found in news raw_processed_articles.
            ex: "(<exchange>:<ticker>)"
    """
    exchange_index = 0
    ticker_index = 1
    tmp = re.sub("[()]", "", ticker_code)
    tmp = tmp.split(":")
    exchange = tmp[exchange_index]
    ticker = tmp[ticker_index]

    return exchange, ticker


def _process_headline(headline: str) -> List[WordCount]:
    """ TO DO

    Param:
        headline: A news raw_processed_articles headline
    """
    # return [WordCount()]  # TODO
    return []  # TODO


def _process_content(content: str) -> Tuple[str, List[WordCount]]:
    """ Returns tickers and key words from article content.

    Note: Currently only accepting raw_processed_articles with 1 ticker
        found from _find_ticker_codes.

    Param:
        content: Contents of article as a str.
    """
    ticker_codes = _find_ticker_codes(content)
    num_tickers = len(ticker_codes)

    # key_words = [WordCount()]  # TODO
    key_words = []  # TODO

    if num_tickers == 1:
    # Find key words
        ticker = ticker_codes[0]
        return ticker, key_words
    # conditional statement checking whether raw_processed_articles include warrants
    elif num_tickers == 2:
        _, ticker = _process_ticker_code(ticker_codes[0])
        _, ticker2 = _process_ticker_code(ticker_codes[1])
        if abs(len(ticker) - len(ticker2)) == 1:
            if len(ticker) > len(ticker2):
                if ticker2 == ticker[:-1] and ticker[-1] == 'W':
                    return ticker_codes[1], key_words
            elif ticker == ticker2[:-1] and ticker2[-1] == 'W':
                return ticker_codes[0], key_words

    return None, None


def _find_ticker_codes(content: str) -> List[str]:
    """ Returns ticker codes found in raw_processed_articles as (EXCHANGE:TICKER).

    Params:
        content: Contents of article as a str.
    """
    ticker_pattern = r"[(][A-Z]+[ ]*[:][ ]*[A-Z]+[)]"
    ticker_pattern2 = r"[[][A-Z]+[ ]*[:][ ]*[A-Z]+[]]"
    ticker_codes = []
    for pattern in [ticker_pattern2, ticker_pattern]:
        matches = re.findall(pattern, content)
        if matches:
            for match in matches:
                ticker_codes.append(match.replace(" ", ""))

    return ticker_codes
