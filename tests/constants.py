""" Constants reused in unit tests. """

from pathlib import Path

TEST_DIR = Path(__file__).parent.parent
TEST_DATABASE_DIR = Path(__file__).parent / "test_databases"
TEST_STOCK_DATA_NEWS_DATABASE_PATH = \
    TEST_DATABASE_DIR / "stock_data_processed_news.sqlite"
