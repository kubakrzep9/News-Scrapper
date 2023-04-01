""" Validates the article processor functions. """

import pytest
import news_scanner.news_scrapper.article_processor as ap


@pytest.mark.parametrize(
    "content, true_tickers",
    [
        ("asdf daf  daf (NYSE:TSLA) daf daf", ["(NYSE:TSLA)"]),
        ("asdf asdf (NASDAQ: INNV) sag asdf ", ["(NASDAQ:INNV)"]),
        ("asdf daf [NYSE:ABBA] daf daf daf", ["[NYSE:ABBA]"]),
    ]
)
def test_find_ticker_codes(content, true_tickers):
    """ Ensures tickers codes can be identified.

    Params:
        content: Mock article content.
        true_tickers: Expected ticker codes.
    """
    tickers = ap._find_ticker_codes(content)
    assert tickers == true_tickers




@pytest.mark.parametrize(
    "content, true_tickers",
    [
        ("asdf daf (NYSE:ABBA) daf (NYSE:TSLA) daf daf", None),
        ("asdf daf (NYSE:ABBA) daf (NYSE:ABBAW) daf daf", "(NYSE:ABBA)"),
        ("asdf daf (NYSE:TSLAW) daf (NYSE:TSLA) daf daf", "(NYSE:TSLA)"),
        ("asdf daf (NYSE:ABBA) daf daf daf", "(NYSE:ABBA)"),
        ("asdf daf daf daf", None),
    ]
)
def test_process_content_tickers(
    content: str, true_tickers: str
):
    """ Ensures raw_processed_articles are processed properly.

    Params:
        content: Mock article content.
        true_tickers: Expected ticker codes.
    """

    ticker, _ = ap._process_content(content)
    #print(ticker)
    #print(true_tickers)
    assert ticker == true_tickers


def test_process_ticker_codes():
    """ Ensures ticker codes can be processed. """
    ticker_code = "(NYSE:TSLA)"
    exchange, ticker = ap._process_ticker_code(ticker_code)
    assert exchange == "NYSE"
    assert ticker == "TSLA"
