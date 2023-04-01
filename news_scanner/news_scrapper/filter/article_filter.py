from news_scanner.result_object import NewsReport
from typing import NamedTuple


class FilterCriteria(NamedTuple):
    """

    Params:
        market_cap: (in millions unit)
        shares_outstanding: (in millions unit)
        last_price:
    """
    market_cap: int = 200
    shares_outstanding: int = 200
    last_price: int = 20


class ArticleFilter:
    """ """
    def __init__(self, filter_criteria: FilterCriteria = FilterCriteria()):
        self.filter_criteria = filter_criteria

    def within_filter(self, news_report: NewsReport):
        """ Returns true if news_report is accepted by filter conditions.

        Params:
            news_report: NewsReport containing data to be compared against.
        """
        if news_report.stockData.market_cap > self.filter_criteria.market_cap or \
                news_report.stockData.shares_outstanding > self.filter_criteria.shares_outstanding or \
                news_report.stockData.last_price > self.filter_criteria.last_price:
            return False
        return True
