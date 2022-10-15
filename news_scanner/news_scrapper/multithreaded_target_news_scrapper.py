""" Multithreaded news scrapper. """

import os
import threading
from typing import NamedTuple, List, Dict
from numpy import array_split
from datetime import datetime, timedelta


from news_scanner.news_scrapper.target_news_scrapper import (
    TargetNewsScrapper,
    ScrappedNewsResult,
    _get_latest_link_index
)


class HeadlineData(NamedTuple):
    headline: str
    link: str
    publish_date: datetime


class MultiThreadedTargetNewsScrapper(TargetNewsScrapper):
    """ Scrapes and tracks stock news data."""
    def __init__(
        self,
        website_url: str,
        scrapper_api_key: str,
        proxy_on: bool = False,
        num_threads: int = 5
    ):
        super().__init__(
            website_url=website_url,
            scrapper_api_key=scrapper_api_key,
            proxy_on=proxy_on
        )
        self.num_threads = num_threads
        self.viewed_links: Dict[str, datetime] = {}
        self.write_lock = threading.Lock()

        # resets each run of 'get_news'
        self.results_queue = []  # shared data structure
        self.num_links_found = 0
        self.num_new_links = 0

    def get_news(self) -> List[ScrappedNewsResult]:
        """ Returns the headline, link and content of a each article.

        """
        # resetting tracking attributes each run
        self.results_queue = []
        self.num_links_found = 0
        self.num_new_links = 0

        # getting data (links to be scrapped)
        headlines, links, publish_dates = self._get_headline_data()
        headline_data = []
        for headline, link, publish_date in zip(
            headlines, links, publish_dates
        ):
            headline_data.append(
                HeadlineData(
                    headline=headline,
                    link=link,
                    publish_date=publish_date
                )
            )

        # splitting found links for threads, assumes links[0] is latest
        latest_index = _get_latest_link_index(links, self.viewed_links)
        latest_headline_data = headline_data[0:latest_index]
        split_latest_headline_data = array_split(latest_headline_data, self.num_threads)

        # multi-threaded scrapping
        threads = []
        for split_headline_data, i in zip(
                split_latest_headline_data,
                range(self.num_threads)
        ):
            _split_headline_data = [HeadlineData(*data) for data in split_headline_data]
            t = threading.Thread(
                target=self._get_news,
                args=(_split_headline_data,),
                name=f"t{i}"
            )
            threads.append(t)
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        # sorting results by publish date
        # reverse=True: [0] is latest article
        self.results_queue.sort(key=lambda x: x.publish_date, reverse=True)
        self.num_links_found = len(links)
        self.num_new_links = len(self.results_queue)

        # adding new links to viewed links tracker
        for result in self.results_queue:
            if result.link not in self.viewed_links:
                self.viewed_links[result.link] = result.publish_date

        return self.results_queue

    def _get_news(self, headline_data: List[HeadlineData]):
        """ """
        _links = []
        for hl_data in headline_data:
            _links.append(hl_data.link)

        contents = self._get_article_content(_links)

        results = []
        for hl_data, content in zip(headline_data, contents):
            result = ScrappedNewsResult(
                headline=hl_data.headline,
                link=hl_data.link,
                publish_date=hl_data.publish_date,  #.strftime('%b %d %Y %I:%M %p'),
                content=content
            )
            results.append(result)

        with self.write_lock:
            self.results_queue.extend(results)
