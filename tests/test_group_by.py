import pytest

# Replace `yourmodule` with the actual import name of your Rust extension
from PyThermite import Index, Indexable, QueryExpr as Q, FilteredIndex

class TestClass(Indexable):
    def some_method(self):
        return "Hello from TestClass"


@pytest.fixture
def indexable():
    # Basic creation of an Indexable object.
    return Indexable()


@pytest.fixture
def index():
    return Index()


def test_non_nested(index: Index):
    for i in range(10):
        index.add_object(TestClass(id=1, num = i//2))

    grouped = index.group_by("num")

    for i in range(4):
        assert len(grouped[i].collect()) == 2

def test_nested_group_by(index: Index):
    for i in range(10):
        nested = TestClass(nest=True, num = i//2)
        index.add_object(TestClass(id=1, nested=nested))

    grouped = index.group_by("nested.num")

    zeros = grouped[0].collect()
    assert len(zeros) == 2
    assert zeros[0].nested.num == 0
    assert zeros[1].nested.num == 0

    grouped = index.group_by("nested.nest")

    all = grouped[True].collect()
    assert len(all) == 10

def test_group_by_one_to_many(index: Index):
    nested = TestClass(nest=True)
    for i in range(10):
        index.add_object(TestClass(id=i, nested=nested))

    grouped = index.group_by("nested.nest")

    all = grouped[True].collect()
    assert len(all) == 10

def test_group_by_one_to_many_deregister(index: Index):
    nested = TestClass(nest=True)
    for i in range(10):
        index.add_object(TestClass(id=i, nested=nested))

    grouped = index.group_by("nested.nest")

    all = grouped[True].collect()
    assert len(all) == 10

    all[0].nested = "test"

    grouped = index.group_by("nested")

    all = grouped[True].collect()
    assert len(all) == 9