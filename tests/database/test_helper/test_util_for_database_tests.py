"""

Testing unit test validator functions.

"""
import pandas as pd
from tests.database.test_helper.util import compare_complex_nt_obj_to_df
from tests.database.conftest import NamedTuple1, ALLOWED_NAMED_TUPLES, ALLOWED_DATA_TYPES


def test_compare_complex_nt_obj_to_df():
    expected_df = pd.DataFrame({
        1: {
            "attr1": "str",
            "attr2": 1,
            "attr3": 0.01
        },
        2: {
            "attr1": "attr1",
            "attr2": 1,
            "attr3": 0.01
        },
    }).T.astype({"attr1": str, "attr2": int, "attr3": float})
    compare_complex_nt_obj_to_df(
        complex_nts={
            1: NamedTuple1(),
            2: NamedTuple1(attr1="attr1")
        },
        df=expected_df,
        allowed_named_tuples=ALLOWED_NAMED_TUPLES,
        allowed_data_types=ALLOWED_DATA_TYPES
    )
