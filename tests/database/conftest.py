from typing import NamedTuple, List
import pytest
from pathlib import Path

TEST_DIR = Path(__file__).parent.parent
TEST_DB_DIR = Path(__file__).parent / "test_databases"



class WrongNamedTuple(NamedTuple):
    attr1: str = ":("


class NamedTuple1(NamedTuple):
    attr1: str = "str"
    attr2: int = 1
    attr3: float = 0.01


class NamedTuple3(NamedTuple):
    attr1: str = "str"
    attr3: float = 0.01


class NamedTuple2(NamedTuple):
    attr2: int = 1
    namedtuple3: NamedTuple3 = NamedTuple3()


class ComplexNT(NamedTuple):
    namedtuple1: NamedTuple1 = NamedTuple1()
    namedtuple2: NamedTuple2 = NamedTuple2()


class NamedTupleDuplicateAttr(NamedTuple):
    attr1: str = "1"
    namedtuple3: NamedTuple3 = NamedTuple3()


class ComplexNTDuplicateAttr(NamedTuple):
    namedtuple: NamedTupleDuplicateAttr = NamedTupleDuplicateAttr()


ALLOWED_DATA_TYPES = [int, float, str]
ALLOWED_NAMED_TUPLES = [NamedTuple1, NamedTuple2, NamedTuple3]


@pytest.fixture
def complexnt_testobjs() -> List[ComplexNT]:
    num_test_objs = 3
    test_objs = []

    for i in range(num_test_objs):
        attr1_val = f"str {str(i)}"
        attr3_val = i*0.01

        test_objs.append(
            ComplexNT(
                namedtuple1=NamedTuple1(
                    attr1=attr1_val,
                    attr2=i,
                    attr3=attr3_val
                ),
                namedtuple2=NamedTuple2(
                    attr2=i,
                    namedtuple3=NamedTuple3(
                        attr1=attr1_val,
                        attr3=attr3_val
                    )
                )
            )
        )

    return test_objs


@pytest.fixture
def complexnt_testobj_unknown_dtype() -> ComplexNT:
    attr1_val = "str"
    attr3_val = 0.01

    return ComplexNT(
        namedtuple1=NamedTuple1(
            attr1=attr1_val,
            attr2=set([1]),  # unknown dtype, should be int
            attr3=attr3_val
        ),
        namedtuple2=NamedTuple2(
            attr2=1,
            namedtuple3=NamedTuple3(
                attr1=attr1_val,
                attr3=attr3_val
            )
        )
    )


@pytest.fixture
def complexnt_testobj_mismatched_attr_dtype() -> ComplexNT:
    attr1_val = "str"
    attr3_val = 0.01

    return ComplexNT(
        namedtuple1=NamedTuple1(
            attr1=attr1_val,
            attr2="1",  # should be int
            attr3=attr3_val
        ),
        namedtuple2=NamedTuple2(
            attr2=1,
            namedtuple3=NamedTuple3(
                attr1=attr1_val,
                attr3=attr3_val
            )
        )
    )
