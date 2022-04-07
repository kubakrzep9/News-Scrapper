""" Main module to orchestrate system flow. """

import time
from typing import List
import twython.exceptions
from news_scanner.news_scrapper.target_news_scrapper import TargetNewsScrapper
from news_scanner.news_scrapper.article_processor import process_articles
from news_scanner.td_api.td_api_handle import TDApiHandle
from news_scanner import util
from news_scanner.logger.logger import logger
from news_scanner.twitter_handle.twitter_handle import TwitterHandle
from news_scanner.database.database_handles.power_switch_handle import PowerSwitchHandle
from news_scanner.result_object import NewsReport, NameData
from news_scanner.config import Config


FILTER = {
    "market cap": 200,  # millions unit
    "shares outstanding": 200,  # millions unit
    "last price": 20
}


class NewsScanner:
    """ Class to scrape, store and alert on stock news. """
    def __init__(
        self,
        config: Config,
        proxy_on: bool = False,
        twitter_on: bool = False,
    ):
        """ Initializes databases and login to APIs.

        Params:
            config: Config object to set api keys and secrets.
            proxy: Determines if proxy is being used.
            twitter_on: Determines is alerts should be sent to twitter.
        """
        self.twitter_on = twitter_on
        self.news_scrapper = TargetNewsScrapper(
            website_url=config.website_url,
            proxy_on=proxy_on
        )
        self.td_api = TDApiHandle(
            config=config.tda_config
        )
        if twitter_on:
            self.twitter_handle = TwitterHandle(
                config=config.twitter_config
            )
        self.powerswitch_handle = PowerSwitchHandle()

    def run(self):
        """ Main run method to run system. """
        #self.powerswitch_handle.set_power(True)
        #while self.powerswitch_handle.power_on():
        for i in range(0, 1):
            start = time.time()
            logger.info("Getting news")
            news_results = self.news_scrapper.get_news()
            logger.info("Processing news")
            processed_results, scrape_results, tickers, exchanges = process_articles(news_results)
            len_processed_results = len(processed_results)
            logger.info(f"Processed results\n"
                         f"- num_links_found: {self.news_scrapper.num_links_found}\n"
                         f"- num_links_accepted: {self.news_scrapper.num_links_accepted}\n"
                         f"- num_links_processed: {len_processed_results}")

            if len(tickers) > 0:
                logger.info("Getting stock data")
                stock_data = self.td_api.get_stock_data(tickers)
                logger.info("Filtering stocks")
                news_reports = []
                for (processed_result, scrape_result, ticker, exchange) in zip(
                        processed_results, scrape_results, tickers, exchanges
                ):
                    news_report = NewsReport(
                            nameData=NameData(
                                ticker=ticker,
                                exchange=exchange
                            ),
                            scrappedNewsResults=scrape_result,
                            # processedNewsResults=processed_result,
                            stockData=stock_data[ticker]
                        )
                    if _within_filter(news_report):
                        news_reports.append(news_report)

                # store to database
                self.database_handle.insert(news_reports)

                # output results
                num_accepted = len(news_reports)
                num_rejected = len_processed_results - num_accepted
                logger.info("Filtered results\n"
                                 f"- num_accepted: {num_accepted}\n"
                                 f"- num_rejected: {num_rejected}")
                _console_logger_output(news_reports)
                if self.twitter_on:
                    self._twitter_output(news_reports)
            end = time.time()
            logger.info(f"Runtime: {end - start}\n")

    def _twitter_output(self, news_reports: List[NewsReport]) -> None:
        """ Outputs NewsReports as alert posts on twitter.

        Can add filters to limit which articles are posted.

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
                self.twitter_handle.make_post(output)
            except twython.exceptions.TwythonError as e:
                logger.error(e.msg)


def _within_filter(news_report: NewsReport) -> bool:
    """ Returns true if news_report is accepted by filter conditions.

    Params:
        news_report: NewsReport containing data to be compared against.
    """
    if news_report.stockData.market_cap > FILTER["market cap"] or \
            news_report.stockData.shares_outstanding > FILTER["shares outstanding"] or \
            news_report.stockData.last_price > FILTER["last price"]:
        return False
    return True


def _console_logger_output(news_reports: List[NewsReport]) -> None:
    """ Outputs news_reports to console.

    Params:
        news_reports: List of NewsReport.
    """
    output_str = ""
    for report in news_reports:
        output_str += f"News Article\n" \
                       f"- link: {report.scrappedNewsResults.link}\n" \
                       f"- published: {report.scrappedNewsResults.publish_date}\n" \
                       f"- ticker: {report.nameData.ticker}\n" \
                       f"- exchange: {report.nameData.exchange}\n" \
                       f"- market cap: ${util.format_millions_unit_to_str(report.stockData.market_cap)}\n" \
                       f"- market cap float: ${util.format_millions_unit_to_str(report.stockData.market_cap_float)}\n" \
                       f"- shares outstanding: {util.format_millions_unit_to_str(report.stockData.shares_outstanding)}\n" \
                       f"- last price: ${report.stockData.last_price}\n\n"

    print(output_str)
    output_str = output_str[:-2]
    logger.info(f"Results\n {output_str}")
