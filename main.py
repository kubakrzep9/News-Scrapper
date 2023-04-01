""" Starts news scanner system. """

from news_scanner.news_scanner import NewsScanner


if __name__ == "__main__":
    news_scanner = NewsScanner(
        proxy_on=False,
        twitter_on=False,
        database_on=False,
        multithreaded_on=True,
        keep_alive=1,
        ignore_warnings=False
    )
    news_scanner.run()
    print("Success")
