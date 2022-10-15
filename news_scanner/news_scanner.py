""" Main module to orchestrate system flow. """

import time
import warnings
from news_scanner.news_scrapper.target_news_scrapper import TargetNewsScrapper
from news_scanner.news_scrapper.multithreaded_target_news_scrapper import MultiThreadedTargetNewsScrapper
from news_scanner.news_scrapper.article_processor import process_articles
from news_scanner.td_api.td_api_handle import TDApiHandle
from news_scanner.logger.logger import logger
from news_scanner.twitter_handle.twitter_handle import TwitterHandle
from news_scanner.database.database_handles.power_switch_handle import PowerSwitchHandle
from news_scanner.result_object import NewsReport, NameData
from news_scanner.config import Config
from news_scanner.database.database_handles.newsreport_database_handle import NewsReportDatabaseHandle
from news_scanner.news_scrapper.filter.article_filter import ArticleFilter, FilterCriteria
from news_scanner.output_display_util import output_run_report


class NewsScanner:
    """ Class to scrape, store and alert on stock news. """
    def __init__(
        self,
        filter_criteria: FilterCriteria = FilterCriteria(),
        proxy_on: bool = False,
        twitter_on: bool = False,
        database_on: bool = False,
        multithreaded_on: bool = False,
        keep_alive: bool = False,
        ignore_warnings: bool = False
    ):
        """ Initializes databases and logs in to APIs.

        Params:
            filter_criteria:
            proxy_on: Determines if proxy is being used.
            twitter_on: Determines is alerts should be sent to twitter.
            database_on:
            multithreaded_on:
            keep_alive:
            ignore_warnings:
        """
        config = Config()
        self.article_filter = ArticleFilter(filter_criteria=filter_criteria)
        self.td_api = TDApiHandle(
            config=config.tda_config
        )
        if database_on:
            self.powerswitch_handle = PowerSwitchHandle()
            self.database_handle = NewsReportDatabaseHandle()
        if twitter_on:
            self.twitter_handle = TwitterHandle(
                config=config.twitter_config
            )
        if multithreaded_on:
            self.news_scrapper = MultiThreadedTargetNewsScrapper(
                website_url=config.website_url,
                proxy_on=proxy_on,
                scrapper_api_key=config.scrapper_api_key,
                num_threads=5
            )
        else:
            self.news_scrapper = TargetNewsScrapper(
                website_url=config.website_url,
                proxy_on=proxy_on,
                scrapper_api_key=config.scrapper_api_key
            )
        self.database_on = database_on
        self.twitter_on = twitter_on
        self.multithreaded_on = multithreaded_on
        self.keep_alive = keep_alive

        if ignore_warnings:
            warnings.filterwarnings("ignore")

        print_and_log("Run Configurations:\n"
                      f"- proxy_on: {proxy_on}\n"
                      f"- twitter_on: {self.twitter_on }\n"
                      f"- database_on: {self.database_on}\n"
                      f"- multithreaded_on: {self.multithreaded_on}\n"
                      f"- keep_alive: {self.keep_alive}\n")

    def run(self):
        """ Main run method to run system. """
        # run until manually shut off
        if self.keep_alive:
            self.powerswitch_handle.set_power(True)
            while self.powerswitch_handle.power_on():
                self.scan_news()
        # run set number of times
        else:
            for i in range(0, 3):
                self.scan_news()

    def scan_news(self):
        """ Scans and processes news and outputs results. """
        start = time.time()
        print_and_log("Getting news")
        news_results = self.news_scrapper.get_news()

        print_and_log("Processing news")
        processed_results, scrape_results, tickers, exchanges = process_articles(news_results)

        len_processed_results = len(processed_results)
        print_and_log(f"Processed results\n"
                      f"- num_links_found: {self.news_scrapper.num_links_found}\n"
                      f"- num_links_accepted: {self.news_scrapper.num_new_links}\n"
                      f"- num_links_processed: {len_processed_results}")

        # process raw_processed_articles that contain a ticker
        if tickers:
            print_and_log("Getting stock data")
            stock_data = self.td_api.get_stock_data(tickers)
            print_and_log("Filtering stocks")
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
                    processedNewsResults=processed_result,
                    stockData=stock_data[ticker]
                )
                if self.article_filter.within_filter(news_report):
                    news_reports.append(news_report)

            # store to database
            if self.database_on:
                self.database_handle.insert(news_reports)

            # post results to twitter
            if self.twitter_on:
                self.twitter_handle.publish_findings(news_reports)

            # output results
            output_run_report(
                logger=logger,
                len_processed_results=len_processed_results,
                news_reports=news_reports
            )

        end = time.time()
        print_and_log(f"Runtime: {end - start}\n")


def print_and_log(text: str):
    print(text)
    logger.info(text)
