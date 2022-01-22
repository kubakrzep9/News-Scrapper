""" Validates Class, 'TDApiHandle', interfacing td api. """

import selenium.webdriver.chrome.webdriver

from news_scanner.td_api.td_api_handle import (
    TDAPIConfig,
    StockData,
    TDApiHandle,
    _make_webdriver
)
import pytest
from dotenv import load_dotenv
from news_scanner.config import Config
import os
from typing import List


@pytest.fixture(scope="session")
def tdapi_handle() -> TDApiHandle:
    """ Object to interface td api. Built once when test file is run. """
    load_dotenv()
    return TDApiHandle(
        config=TDAPIConfig(
            api_key=os.environ[Config._TDA_API_KEY]
        )
    )


@pytest.mark.parametrize(
    "tickers", [
        [],
        ["GME", "TSLA"]
    ]
)
def test_get_stock_data(
    tdapi_handle: TDApiHandle,
    tickers: List[str]
):
    """ Ensures stock data can be retrieved for a list of tickers.

    Params:
        tdapi_handle: Object to interface td api.
        tickers: List of tickers to retrieve data on.
    """
    stock_data = tdapi_handle.get_stock_data(tickers)

    assert len(stock_data) == len(tickers)
    for ticker in stock_data:
        assert isinstance(stock_data[ticker], StockData)


@pytest.mark.parametrize(
    "tickers", [
        [],
        ["GME", "TSLA"]
    ]
)
def test_get_fundamentals(
    tdapi_handle: TDApiHandle,
    tickers: List[str]
):
    """ Ensures fundamentals data can be retrieved for a list of tickers.

    Params:
        tdapi_handle: Object to interface td api.
        tickers: List of tickers to retrieve data on.
    """
    fundamentals = tdapi_handle._get_fundamentals(tickers)

    assert isinstance(fundamentals, dict)
    assert len(fundamentals) == len(tickers)

    for ticker in fundamentals:
        ticker_fundamentals = fundamentals[ticker][tdapi_handle.FUNDAMENTAL]
        assert ticker in tickers
        assert isinstance(ticker_fundamentals[tdapi_handle.MARKET_CAP], float)
        assert isinstance(
            ticker_fundamentals[tdapi_handle.MARKET_CAP_FLOAT], float
        )
        assert isinstance(
            ticker_fundamentals[tdapi_handle.SHARES_OUTSTANDING], float
        )


@pytest.mark.parametrize(
    "tickers", [
        [],
        ["GME", "TSLA"]
    ]
)
def test_get_quotes(
    tdapi_handle: TDApiHandle,
    tickers: List[str]
):
    """ Ensures quote data can be retrieved for a list of tickers.

    Params:
        tdapi_handle: Object to interface td api.
        tickers: List of tickers to retrieve data on.
    """
    quotes = tdapi_handle._get_quotes(tickers)

    assert isinstance(quotes, dict)
    assert len(quotes) == len(tickers)

    for ticker in quotes:
        assert ticker in tickers
        assert isinstance(quotes[ticker][tdapi_handle.LAST_PRICE], float)


def test_make_webdriver():
    """ Ensures a chrome webdriver is returned. """
    driver = _make_webdriver()
    driver_type = type(driver)
    driver.quit()
    assert driver_type == selenium.webdriver.chrome.webdriver.WebDriver
