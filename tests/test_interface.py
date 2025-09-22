

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


def test_collect(index):
    # Test collecting from an empty index.
    result = index.collect()
    assert isinstance(result, list)
    assert len(result) == 0
    text_obj = TestClass()
    index.add_object(text_obj)
    result = index.collect()
    assert len(result) == 1
    assert result[0] is text_obj


def test_add_object(index):
    text_obj = TestClass(key="value")
    index.add_object(text_obj)
    result = index.collect()
    assert len(result) == 1
    assert result[0] is text_obj


def test_add_object_many(index):
    objs = [TestClass(num=i) for i in range(5)]
    index.add_object_many(objs)
    result = index.collect()
    assert len(result) == 5
    for obj in objs:
        assert obj in result

def test_query(index):
    objs = [TestClass(num=i, active=(i % 2 == 0), score=float(i) * 10.0) for i in range(10)]
    index.add_object_many(objs)

    query = Q.and_(
        Q.eq("active", True),
        Q.gt("score", 50.0)
    )
    result = index.reduced_query(query).collect()
    assert all(obj.active is True and obj.score > 50.0 for obj in result)
    assert len(result) == 2  # Should be objects with num 6 and 8

def test_query_chain(index):
    objs = [TestClass(num=i, active=(i % 2 == 0), score=float(i) * 10.0) for i in range(10)]
    index.add_object_many(objs)

    # First query
    filtered_index = index.reduced(active=True)
    # Second query on the filtered index
    second_filtered_index = filtered_index.reduced_query(Q.gt("score", 50.0))
    res = second_filtered_index.collect()
    assert all(obj.active is True and obj.score > 50.0 for obj in res)
    assert len(res) == 2  # Should be objects with num 6 and 8

    final_filtered_index = second_filtered_index.reduced_query(Q.lt("num", 8))
    final_res = final_filtered_index.collect()
    assert all(obj.active is True and obj.score > 50.0 and obj.num < 8 for obj in final_res)
    assert len(final_res) == 1  # Should be objects with num 6


def test_filtered_index(index):
    objs = [TestClass(num=i, active=(i % 2 == 0), score=float(i) * 10.0) for i in range(10)]
    index.add_object_many(objs)

    # initial filter
    filtered_index = index.reduced(active=True)
    # filter the filtered
    reduced = filtered_index.reduced_query(
        Q.gt("score", 50.0)
    )

    result = reduced.collect()
    assert all(obj.active is True and obj.score > 50.0 for obj in result)
    assert len(result) == 2

def test_and_query(index):
    objs = [TestClass(num=i, active=(i % 2 == 0), score=float(i) * 10.0) for i in range(10)]
    index.add_object_many(objs)

    # Create a nested query
    query = Q.and_(
        Q.eq("active", True),
        Q.or_(
            Q.gt("score", 70.0),
            Q.lt("num", 3)
        )
    )
    result = index.reduced_query(query).collect()
    assert all(obj.active is True and (obj.score > 70.0 or obj.num < 3) for obj in result)
    assert len(result) == 3 # Should be objects with num 0,2,8


def test_nested_object_query(index):
    class NestedTestClass(Indexable):
        pass

    nested_objs = [TestClass(num=i, nested=NestedTestClass(num=i * 10)) for i in range(5)]
    index.add_object_many(nested_objs)

    # Query based on nested object's attribute
    query = Q.eq("nested.num", 20)
    result = index.reduced_query(query).collect()
    assert len(result) == 1
    assert result[0].nested.num == 20

def test_updates_reflect(index):
    obj = TestClass(num=1, active=True)
    index.add_object(obj)

    query = Q.eq("active", True)
    result = index.reduced_query(query).collect()
    assert len(result) == 1

    # Update the object to no longer be active
    obj.active = False

    result = index.reduced_query(query).collect()
    assert len(result) == 0

    # Update back to active
    obj.active = True

    result = index.reduced_query(query).collect()
    assert len(result) == 1
