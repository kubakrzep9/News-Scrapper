""" Functions and classes to scrape a target website's news article data. """

import bs4
import requests
from typing import List, Tuple, NamedTuple, Union, Dict
from datetime import datetime, timedelta
from news_scanner.logger.logger import logger
from urllib.parse import urlencode

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
        scrapper_api_key: str,
        proxy_on: bool = False
    ):
        self.website_url = website_url
        self.scrapper_api_key = scrapper_api_key
        self.proxy_on = proxy_on
        self.viewed_links = []
        self.num_links_found = 0
        self.num_new_links = 0

    def get_news(self) -> List[ScrappedNewsResult]:
        """ Returns the headline, link and content of a each article.

        Note: [0] of return result is latest link.

        """
        headlines, links, publish_dates = self._get_headline_data()
        latest_index = _get_latest_link_index(links, self.viewed_links)
        contents = self._get_article_content(links[0:latest_index])

        results = []
        for headline, link, publish_date, content in zip(
            headlines[0:latest_index], links[0:latest_index],
            publish_dates[0:latest_index], contents
        ):
            result = ScrappedNewsResult(
                headline=headline,
                link=link,
                publish_date=publish_date.strftime('%b %d %Y %I:%M %p'),
                content=content
            )
            results.append(result)
            self.viewed_links.append(link)

        self.num_links_found = len(links)
        self.num_new_links = len(results)
        return results

    def _get_page_content(self, link: str) -> bs4.BeautifulSoup:
        """ Returns the html contents of a webpage.

        Param:
            link: url to a webpage.
        """
        if self.proxy_on:
            params = {'api_key': self.scrapper_api_key, 'url': link}
            page = requests.get('http://api.scraperapi.com/', params=urlencode(params))
            html_content = bs4.BeautifulSoup(page.content, features="html.parser")
        else:
            page = requests.get(url=link)
            html_content = bs4.BeautifulSoup(page.content, features="html.parser")

        return html_content

    def _get_headline_data(self) -> Tuple[List[str], List[str], List[datetime]]:
        """ Returns parallel lists of headlines, links and publish date. """
        link = self.website_url + "/news"
        page_content = self._get_page_content(link)
        table = page_content.find('section', attrs={'class': "market-news__results"})
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
            links: extension paths to corresponding news raw_processed_articles
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


def _get_latest_link_index(
        links: List[str],
        viewed_links: Union[List[str], Dict[str, datetime]]
) -> int:
    """ Returns scrape_results omitting what was read.

    TODO:
      - Validate it works lol
      - Search speed up: start the search from the back of viewed_links
        and the front of links

    Assumes:
      - links[0] is latest

    Params:
        links: List of links from latest scrape.
        view_links: List of all links seen during runtime.
    """
    latest_index = 0
    for link in links:
        if link in viewed_links:
            break
        latest_index += 1
    return latest_index  # -1?


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
