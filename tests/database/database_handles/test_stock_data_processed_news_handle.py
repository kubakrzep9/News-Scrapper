""" Tests functionality of 'StockDataProcessedNewsHandle' """

# from typing import List
# from pathlib import Path
# import pytest
# from news_scanner.database.database_handles.stock_data_processed_news_handle import StockDataProcessedNewsHandle
# from news_scanner.result_object import NewsReport, ScrappedNewsResult, \
#     StockData, NameData, ALLOWED_NAMED_TUPLES
# from tests.database.constants import TEST_DB_DIR
# from tests.database.util import tear_down, compare_complex_nt_obj_to_df
#
#
# @pytest.fixture
# def newsreport_list() -> List[NewsReport]:
#     """ Test data of NewsReport object. """
#     data = []
#     for i in range(3):
#         data.append(NewsReport(
#             scrappedNewsResults=ScrappedNewsResult(
#                 headline=f"headline{str(i)}",
#                 link=f"link{str(i)}",
#                 publish_date=f"publish_date{str(i)}",
#                 content=f"content{str(i)}"
#             ),
#             stockData=StockData(
#                 market_cap=float(i),
#                 market_cap_float=float(i),
#                 shares_outstanding=float(i),
#                 last_price=float(i)
#             ),
#             nameData=NameData(
#                 ticker=f"ticker{str(i)}",
#                 exchange=f"exchange{str(i)}"
#             )
#         ))
#     return data
