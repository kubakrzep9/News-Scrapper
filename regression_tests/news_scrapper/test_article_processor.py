from pathlib import Path
import news_scanner.news_scrapper.article_processor as ap


def test_find_ticker_codes():
    print()
    data_dir = Path(__file__).parent.parent / "data" / "raw_processed_articles"

    for article in data_dir.iterdir():
        print(article)
        with open(article, "r") as file:
            content = file.read()

        ticker_codes = ap._find_ticker_codes(content)
        print(ticker_codes)
        assert len(ticker_codes) > 0

    assert 1 == 2
