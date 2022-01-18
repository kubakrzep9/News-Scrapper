""" Tests util functions from news_scanner module. """

import pytest
from news_scanner import util


@pytest.mark.parametrize(
    "value, expected",
    [
        (1, "1.0M"),
        (10, "10.0M"),
        (100, "100.0M"),
        (1000, "1.0B"),
        (10000, "10.0B"),
        (100000, "100.0B"),
        (1000000, "1.0T"),
        (10000000, "10.0T"),
        (100000000, "100.0T"),
        (1.7333, "1.73M"),
        (18.999, "19.0M"),
        (173.339, "173.34M"),
        (1773.678, "1.77B"),
        (17736.78, "17.74B"),
        (177367.88, "177.37B"),
        (1324532.3245, "1.32T"),
        (13245323.245, "13.25T"),
        (132453232.45, "132.45T"),
    ]
)
def test_format_millions_unit_to_str(
    value: float, expected: str
):
    """ Ensures float values are formatted in the correct unit as a str.

    Params:
        value: Float value to be formatted.
        expected: Expected formatted str.
    """
    assert util.format_millions_unit_to_str(value) == expected
