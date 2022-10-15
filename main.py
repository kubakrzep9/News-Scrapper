""" Starts news scanner system. """

from news_scanner.news_scanner import NewsScanner


if __name__ == "__main__":
    news_scanner = NewsScanner(
        proxy_on=True,
        twitter_on=False,
        database_on=False,
        multithreaded_on=True,
        keep_alive=False,
        ignore_warnings=True
    )
    news_scanner.run()
    print("Success")
