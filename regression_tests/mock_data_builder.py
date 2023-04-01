import random
import string
from datetime import datetime, timedelta
from typing import List, Dict
from news_scanner.td_api.td_api_handle import StockData
from news_scanner.news_scrapper.target_news_scrapper import TIME_FORMAT

random.seed(10)
ENGLISH_LETTERS = string.ascii_uppercase


class MockDataBuilder:
    def __init__(
            self,
            news_page_url: str,
            iterations: int = 1,
            articles_per_iteration: int = 50,
            new_invalid_articles_per_iteration: int = 10,
            new_valid_articles_per_iteration: int = 2
    ):
        self.news_page_url = news_page_url
        self.iterations = iterations
        self.articles_per_iteration = articles_per_iteration
        self.new_invalid_articles_per_iteration = new_invalid_articles_per_iteration
        self.new_valid_articles_per_iteration = new_valid_articles_per_iteration
        self.viewed_articles: List[Dict] = []
        self.used_tickers = set()
        self.news_page_request_responses = []
        self.article_request_responses = []
        self.all_stock_data = []
        self.start_date = datetime(2022, 10, 18)
        self.old_articles_per_iteration = self.articles_per_iteration - \
            self.new_valid_articles_per_iteration - \
            self.new_invalid_articles_per_iteration

    def build_mock_data(self):
        """ Builds mock news articles and result objects for the system to process.

        Params:
            news_page_url: Home page url used by system.
            iterations: Number of times the system scans for news.
            articles_per_iteration: Number of articles each time system scans
                for news.
            new_articles_per_iteration: Number of new articles each time system
                scans for news.
            new_valid_articles_per_iteration: Number of new articles accepted by
                the system each time it scans for news.
        """

        for i in range(self.iterations):
            if not self.viewed_articles:
                old_articles_per_iteration = 0
            else:
                old_articles_per_iteration = self.old_articles_per_iteration
            new_valid_article_counter = 0
            stock_data_per_iteration = {}
            news_page = """
                <html>
                    <div>
                        <section class="market-news__results">
            """
            news_page_content = []
            for article_i in range(self.articles_per_iteration):
                if article_i < old_articles_per_iteration:
                    # get old article
                    old_article_i = self.old_articles_per_iteration - article_i
                    article_url = self.viewed_articles[-old_article_i]["url"]
                    article_headline = self.viewed_articles[-old_article_i]["headline"]
                    article_time_stamp = self.viewed_articles[-old_article_i]["article_time_stamp"]
                    article_date = self.viewed_articles[-old_article_i]["article_date"]
                else:
                    # create new article
                    article_num = len(self.viewed_articles) + 1
                    article_url = f"/news/l_{article_num}"
                    full_article_url = f"{self.news_page_url}{article_url}"
                    article_headline = f"h_{article_num}"
                    article_datetime_obj = self.start_date + (article_num * timedelta(days=1))  # most recent first
                    article_date = article_datetime_obj.strftime(f"{TIME_FORMAT} CDT")  # "Oct 14, 2022 11:00 PM CDT"
                    article_time_stamp = article_datetime_obj.strftime(
                        "%Y-%m-%dT%H:%M:%S-05:00")  # "2022-10-14T23:00:00-05:00"
                    article_p1 = """<p class="mdc-article-paragraph">p1</p>"""
                    if new_valid_article_counter < self.new_valid_articles_per_iteration:
                        # create new valid article
                        ticker = self._random_ticker_builder()
                        stock_data_per_iteration[ticker] = StockData(
                            market_cap=float(article_num),
                            market_cap_float=float(article_num),
                            shares_outstanding=float(article_num),
                            last_price=float(article_num)
                        )
                        article_p1 = article_p1.replace("p1", f"(NASDAQ:{ticker})")
                        new_valid_article_counter += 1
                        if len(stock_data_per_iteration) == self.new_valid_articles_per_iteration:
                            self.all_stock_data.append(stock_data_per_iteration)
                    self.article_request_responses.append(
                        {
                            "url": full_article_url,
                            "body": f"""
                                <html>
                                    <div class="mdc-article-body">
                                        {article_p1}
                                        <p class="mdc-article-paragraph">p2</p>
                                        <p class="mdc-article-paragraph">p3</p>
                                    </div>
                                </html>
                            """
                        }
                    )

                    self.viewed_articles.append({
                        "url": article_url,
                        "headline": article_headline,
                        "article_time_stamp": article_time_stamp,
                        "article_date": article_date
                    })

                # latest is first
                news_page_content.insert(
                    0,
                    f"""
                                <article>
                                    <a href="not_target">not_target</a>
                                    <a href="{article_url}">{article_headline}</a>
                                    <time datetime="{article_time_stamp}">{article_date}</time>
                                </article>
                    """
                )

            for content in news_page_content:
                news_page += content
            news_page += """
                        </section>
                    </div>
                </html>
            """
            self.news_page_request_responses.append(
                {
                    "url": f"{self.news_page_url}/news",
                    "body": news_page
                }
            )

        # print("news_page_request_responses")
        # for request in self.news_page_request_responses:
        #     print(request)
        # print()
        # print("viewed_articles")
        # for article in self.viewed_articles:
        #     print(article)
        # print()
        # print("article_request_responses")
        # for request in self.article_request_responses:
        #     print(request)
        # print()
        # print("all_stock_data")
        # for stock_data in self.all_stock_data:
        #     print(stock_data)

        return self.news_page_request_responses, self.article_request_responses, self.all_stock_data

    def _random_ticker_builder(self, ticker_len: int = 4) -> str:
        """ Returns a unique ticker and all used tickers.

        Keeps track of all used tickers.

        Params:
            used_tickers: Set of tickers already used.
            ticker_len: Number of characters in a ticker.
        """
        ticker_not_found = True
        while ticker_not_found:
            ticker = random.choices(ENGLISH_LETTERS, k=ticker_len)
            ticker = " ".join(ticker).replace(" ", "")
            if ticker not in self.used_tickers:
                ticker_not_found = False
        self.used_tickers.add(ticker)

        return ticker
