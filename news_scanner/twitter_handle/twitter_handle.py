""" Contains config and handle to interface with twitter api. """

from twython import Twython, exceptions
from typing import List, NamedTuple
from news_scanner.result_object import NewsReport
from news_scanner import util
from news_scanner.logger.logger import logger


class TwitterHandleConfig(NamedTuple):
    """ Secrets to access twitter API.

    Attrs:
        api_key: Twitter account api key.
        api_secret_key: Twitter account secret api key.
        access_token: Twitter account access token.
        access_token_secret: Twitter account access token secret.
    """
    api_key: str
    api_secret_key: str
    access_token: str
    access_token_secret: str


class TwitterHandle:
    """ Class to interface with twitter api."""

    def __init__(
        self,
        config: TwitterHandleConfig
    ):
        """ Initializes handle to twitter api.

        Params:
            config: Config object that contains secrets to access twitter api.
        """
        print("creating twitter handle")
        self.twitter = Twython(
            config.api_key,
            config.api_secret_key,
            config.access_token,
            config.access_token_secret
        )

    def make_post(self, message: str):
        """ Makes a post on twitter.

        Params:
            message: Message to post on twitter.
        """
        print("message length:", len(message))
        if len(message) > 280:
            print("Too many characters in post")
        self.twitter.update_status(status=message)

    def publish_findings(self, news_reports: List[NewsReport]) -> None:
        """ Outputs NewsReports as alert posts on twitter.

        Can add filter to limit which raw_processed_articles are posted.

        Params:
            news_report: List of NewsReport.
        """
        most_recent_reports = news_reports.copy()
        most_recent_reports.reverse()

        for report in most_recent_reports:
            output = f"{report.scrappedNewsResults.link}\n" \
                f"- published: {report.scrappedNewsResults.publish_date}\n" \
                f"- ticker: {report.nameData.ticker}\n" \
                f"- market cap: ${util.format_millions_unit_to_str(report.stockData.market_cap)}\n" \
                f"- shares outstanding: {util.format_millions_unit_to_str(report.stockData.shares_outstanding)}\n" \
                f"- last price: ${report.stockData.last_price}\n\n"
            try:
                self.make_post(output)
            except exceptions.TwythonError as e:
                logger.error(e.msg)
