import pytest
import news_scanner.news_scrapper.article_processor as msnp

@pytest.mark.parametrize(
    "content, true_tickers",
    [
        ("asdf daf  daf (NYSE:TSLA) daf daf", ["(NYSE:TSLA)"]),
        ("asdf asdf (NASDAQ: INNV) sag asdf ", ["(NASDAQ:INNV)"])
    ]
)
def test_find_tickers(content, true_tickers):
    tickers = msnp._find_ticker_codes(content)
    assert tickers == true_tickers


@pytest.mark.parametrize(
    "content, true_tickers",
    [
        ("asdf daf (NYSE:ABBA) daf (NYSE:TSLA) daf daf", None),
        ("asdf daf (NYSE:ABBA) daf daf daf", "(NYSE:ABBA)"),
        ("asdf daf daf daf", None),
    ]
)
def test_process_content_tickers(
    content: str, true_tickers: str
):
    ticker, _ = msnp._process_content(content)
    assert ticker == true_tickers

def test_process_ticker_codes():
    ticker_code = "(NYSE:TSLA)"
    exchange, ticker = msnp._process_ticker_code(ticker_code)
    assert exchange == "NYSE"
    assert ticker == "TSLA"
