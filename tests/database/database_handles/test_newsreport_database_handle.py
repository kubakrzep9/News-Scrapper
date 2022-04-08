
from typing import List
from news_scanner.result_object import NewsReport, StockData, ScrappedNewsResult, NameData, ALLOWED_NAMED_TUPLES
from news_scanner.database.database_handles.newsreport_database_handle import NewsReportDatabaseHandle
from tests.database.test_helper.database_handle_validator import validate_database_handle


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


def test_newsreport_database_handle():
    print()
    validate_database_handle(
        database_handle=NewsReportDatabaseHandle(),
        insert_data=insert_data(),
        allowed_namedtuples=ALLOWED_NAMED_TUPLES,
    )
