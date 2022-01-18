""" Starts system. Must be executed via command line: 'python main.py'. """

from news_scanner.news_scanner import NewsScanner
from news_scanner.config import Config


if __name__ == "__main__":
    news_scanner = NewsScanner(
        config=Config(),
        proxy_on=False,
        twitter_on=False
    )
    news_scanner.run()
