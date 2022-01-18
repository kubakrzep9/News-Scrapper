""" Contains config, result object and handle to interface with td api. """

from tda.auth import easy_client
from selenium import webdriver
import atexit
import httpx
from typing import NamedTuple, List


class TDAPIConfig(NamedTuple):
    """ Secrets to access TD API.

    Attrs:
        api_key: TD account api key.
        redirect_uri: Location where to redirect requests from TD api.
        token_path: Path to access token.
    """
    api_key: str
    redirect_uri: str = 'https://localhost:8888'
    token_path: str = '/tmp/token.json'


class StockData(NamedTuple):
    """ Result object containing stock data.

    Attrs:
        market_cap: Total dollar market value of a company's
            outstanding shares.
        market_cap_float: Dollar market value of a company's float (less than
            shares outstanding).
        shares_outstanding: Total number of tradeable and untradeable shares
            registered by a company
        last_price: The last price the stock was registered at.
    """
    market_cap: float = 0
    market_cap_float: float = 0
    shares_outstanding: int = 0
    last_price: float = 0


class TDApiHandle:
    """ Class to interface with td api. """

    def __init__(
        self,
        config: TDAPIConfig
    ):
        """ Initializes handle to td api.

        Params:
            config: Config object that contains secrets to access td api.
        """
        self.client = easy_client(
            api_key=config.api_key,
            redirect_uri=config.redirect_uri,
            token_path=config.token_path,
            webdriver_func=_make_webdriver,
        )

    def get_stock_data(self, tickers: List[str]):
        """ Returns list of specified stock data on tickers from td api.

        Params:
            tickers: List of tickers to retrieve specified stock data on.
        """
        all_fundamentals = self._get_fundamentals(tickers)
        all_quotes = self._get_quotes(tickers)
        stock_data = {}

        for ticker in tickers:
            try:
                fundamentals = all_fundamentals[ticker]["fundamental"]
                quote = all_quotes[ticker]
                stock_data[ticker] = StockData(
                    market_cap=fundamentals["marketCap"],
                    market_cap_float=fundamentals["marketCapFloat"],
                    # stay consistent with market cap units
                    shares_outstanding=fundamentals["sharesOutstanding"] / 1000000,
                    last_price=quote["lastPrice"]
                )
            # stock data not found for ticker
            except KeyError as e:
                stock_data[ticker] = StockData()
        return stock_data

    def _get_fundamentals(self, tickers: List[str]):
        """ Returns list of all fundamental stock data on tickers from td api.

        Params:
            tickers: List of tickers to retrieve all fundamental stock data on.
        """
        resp = self.client.search_instruments(tickers, self.client.Instrument.Projection.FUNDAMENTAL)
        assert resp.status_code == httpx.codes.OK
        # print(resp.json())
        return resp.json()

    def _get_quotes(self, tickers: List[str]):
        """ Returns list of stock quotes (pricing data) on tickers from td api.

        Params:
            tickers: List of tickers to retrieve quotes on.
        """
        resp = self.client.get_quotes(tickers)
        assert resp.status_code == httpx.codes.OK
        # print(resp.json())
        return resp.json()


def _make_webdriver():
    """ Function used to create a webdriver to send requests to td api. """
    driver = webdriver.Chrome()
    atexit.register(lambda: driver.quit())
    return driver
