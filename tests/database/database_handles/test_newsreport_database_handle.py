
from typing import List
from news_scanner.result_object import (
    NewsReport,
    StockData,
    ScrappedNewsResult,
    NameData,
    ProcessedNewsResult,
    ALLOWED_NAMED_TUPLES,
)
from news_scanner.news_scrapper.article_processor import WordCount
from news_scanner.database.database_handles.newsreport_database_handle import NewsReportDatabaseHandle
from tests.database.test_helper.database_handle_validator import validate_database_handle
from tests.database.test_helper.util import tear_down
from tests.database.conftest import TEST_DB_DIR


def insert_data(
    num_data: int = 5
) -> List:
    data = []
    for i in range(1, num_data+1):
        data.append(
            NewsReport(
                scrappedNewsResults=ScrappedNewsResult(
                    headline=f"headline {i}",
                    link=f"link {i}",
                    publish_date=f"publish date {i}",
                    content=f"content {i}"
                ),
                processedNewsResults=ProcessedNewsResult(
                    article_keywords=[
                        WordCount(word=f"word {i}", count=i),
                        WordCount(word=f"word {i+1}", count=i+1)
                    ],
                    headline_keywords=[
                        WordCount(word=f"word {i}", count=i),
                        WordCount(word=f"word {i + 1}", count=i + 1)
                    ],
                    sentiment=0.01/i,
                    article_type=f"article type {i}"
                ),
                stockData=StockData(
                    market_cap=0.001+i,
                    market_cap_float=0.001+i,
                    shares_outstanding=0.001+i,
                    last_price=0.001+i
                ),
                nameData=NameData(
                    ticker=f"ticker {i}",
                    exchange=f"exchange {i}"
                )
            )
        )

    return data


########
# Add expected extended table indexs to be added to generated tables.
########
def test_newsreport_database_handle():
    tear_down()
    print()
    validate_database_handle(
        database_handle=NewsReportDatabaseHandle(
            db_dir=TEST_DB_DIR
        ),
        insert_data=insert_data(),
        allowed_namedtuples=ALLOWED_NAMED_TUPLES,
    )
