""" """

from typing import List
from news_scanner import util
from news_scanner.result_object import NewsReport


def output_run_report(
    logger,
    len_processed_results: int,
    news_reports: List[NewsReport]
):
    num_accepted = len(news_reports)
    num_rejected = len_processed_results - num_accepted
    output_str = "Filtered results\n" \
                 f"- num_accepted: {num_accepted}\n" \
                 f"- num_rejected: {num_rejected}"
    print(output_str)
    logger.info(output_str)

    output_str = ""
    for report in news_reports:
        output_str += f"News Article\n" \
                       f"- link: {report.scrappedNewsResults.link}\n" \
                       f"- published: {report.scrappedNewsResults.publish_date}\n" \
                       f"- ticker: {report.nameData.ticker}\n" \
                       f"- exchange: {report.nameData.exchange}\n" \
                       f"- market cap: ${util.format_millions_unit_to_str(report.stockData.market_cap)}\n" \
                       f"- market cap float: ${util.format_millions_unit_to_str(report.stockData.market_cap_float)}\n" \
                       f"- shares outstanding: {util.format_millions_unit_to_str(report.stockData.shares_outstanding)}\n" \
                       f"- last price: ${report.stockData.last_price}\n\n"

    print(output_str)
    output_str = output_str[:-2]
    logger.info(f"Results\n {output_str}")
