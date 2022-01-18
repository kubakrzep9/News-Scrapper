""" Functions and classes to scrape a target website's news article data. """

import bs4
from selenium import webdriver
import requests
from typing import List, Tuple, NamedTuple
from datetime import datetime, timedelta
from news_scanner.proxy.proxy_constants import PROXY_OPTIONS
from news_scanner.logger.logger import logger

TIME_FORMAT = "%b %d, %Y %I:%M %p %Z"


class ScrappedNewsResult(NamedTuple):
    """ Result object containing data about a scrapped article.

    Attrs:
        headline: Headline of article.
        link: Link to article.
        publish_date: Date and time article was published.
        content: Article content.
    """
    headline: str = "headline"
    link: str = "link"
    publish_date: datetime.date = "publish date"
    content: str = "content"


class TargetNewsScrapper:
    """ Scrapes and tracks stock news data."""
    def __init__(
        self,
        website_url: str,
        proxy_on: bool = False
    ):
        self.website_url = website_url
        self.proxy_on = proxy_on
        if self.proxy_on:
            self._init_web_driver()
        self.viewed_links = []
        self.num_links_found = 0
        self.num_links_accepted = 0

    def __del__(self):
        """ Closes connection to proxy. """
        if self.proxy_on:
            self.browser.close()

    def get_news(self):
        """ Returns the headline, link and content of a each article. """
        headlines, links, publish_dates = self._get_headlines()
        latest_index = _get_latest_link_index(links, self.viewed_links)
        contents = self._get_article_content(links[0:latest_index])

        results = []
        for headline, link, publish_date, content in zip(
            headlines[0:latest_index], links[0:latest_index],
            publish_dates[0:latest_index], contents
        ):
            result = ScrappedNewsResult(
                headline, link, publish_date, content
            )
            results.append(result)
            self.viewed_links.append(link)

        self.num_links_found = len(links)
        self.num_links_accepted = len(results)
        return results

    def _init_web_driver(self):
        self.browser = webdriver.Chrome(options=PROXY_OPTIONS)

    def _get_page_content(self, link: str) -> bs4.BeautifulSoup:
        """ Returns the html contents of a webpage.

        Param:
            link: url to a webpage.
        """
        if self.proxy_on:
            self.browser.get(url=link)
            html_content = bs4.BeautifulSoup(self.browser.page_source, features="html.parser")
        else:
            page = requests.get(url=link)
            html_content = bs4.BeautifulSoup(page.content, features="html.parser")

        return html_content

    def _get_headlines(self) -> Tuple[List[str], List[str], List[datetime]]:
        """ Returns headlines and links of articles of all news outlets. """
        link = self.website_url + "/news"
        page_content = self._get_page_content(link)
        table = page_content.find('section', attrs={'class': "news__results"})
        articles = table.find_all('article')
        headlines = []
        links = []
        publish_dates = []
        for article in articles:
            a_refs = article.find_all('a')
            headline = a_refs[1]
            links.append(self.website_url + headline.get("href"))
            headlines.append(headline.get_text())

            time_tag = article.find("time")
            time_str = time_tag.get_text().replace("\t", "").replace("\n", "")
            publish_dates.append(_to_datetime_cst(time_str))
        return headlines, links, publish_dates

    def _get_article_content(self, links: List[str]) -> List[str]:
        """ Returns article content from each link in links as a list of str.

        Param:
            links: extension paths to corresponding news articles
        """
        contents = []
        for link in links:
            body = ""
            try:
                page_content = self._get_page_content(link)
                article = page_content.find(
                    'div', attrs={"class": "mdc-article-body"}
                )
                paragraphs = article.find_all(
                    "p", attrs={"class": "mdc-article-paragraph"}
                )
                for paragraph in paragraphs:
                    body += (paragraph.get_text() + " ")
            except Exception as e:
                print("Error parsing article contents")
                logger.error(str(e)+f"\n- url: {link}")

            contents.append(body)

        return contents


def _get_latest_link_index(links: List[str], viewed_links: List[str]) -> int:
    """ Returns scrape_results omitting what was read.

    TO DO:
        - Search speed up: start the search from the back of viewed_links
          and the front of links

    Params:
        links: List of links from latest scrape.
        view_links: List of all links seen during runtime.
    """
    latest_index = 0
    for link in links:
        if link in viewed_links:
            break
        latest_index += 1
    return latest_index


def _to_datetime_cst(
    time_str: str,
    time_format: str = TIME_FORMAT,
    utc_offset: int = 6
) -> datetime:
    """ Adhoc method to return datetime obj in specified timezone, default cst.

    Note: Currently in time change.

    Params:
        time_str: str of a date time.
        time_format: datetime format for time_str.
        utc_offset: number of hours to subtract from utc time.
    """
    datetime_utc = datetime.strptime(time_str, time_format)
    datetime_cst = datetime_utc - timedelta(hours=utc_offset)
    return datetime_cst
