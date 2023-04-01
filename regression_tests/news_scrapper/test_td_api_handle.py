from news_scanner.td_api.td_api_handle import (
    TDApiHandle,
    INVALID_STOCK_DATA
)

from news_scanner.config import Config


def test_retrieving_invalid_ticker_data():
    """ Ensures invalid tickers are set to INVALID_VALUE.
    Allows for invalid/missing data to be identified.
    """
    valid_ticker = "TSLA"
    invalid_ticker = "XASD"  # invalid 2022/11/18
    tickers = [valid_ticker, invalid_ticker]

    config = Config()
    td_api_handle = TDApiHandle(config=config.tda_config)
    stock_data = td_api_handle.get_stock_data(tickers=tickers)
    print(stock_data)
    for ticker in tickers:
        if ticker == valid_ticker:
            assert stock_data[ticker] != INVALID_STOCK_DATA
        else:
            assert stock_data[ticker] == INVALID_STOCK_DATA
