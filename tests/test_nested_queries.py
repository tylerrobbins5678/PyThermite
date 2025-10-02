
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


def test_string(index):
    objs = [TestClass(name=f"object_{i}") for i in range(5)]
    index.add_object_many(objs)

    query = Q.eq("name", "object_3")
    result = index.reduced_query(query)
    assert len(result.collect()) == 1
    assert result.collect()[0].name == "object_3"

    for r in result.collect():
        r.child = TestClass(name="child_of")

    nested_result = index.reduced_query(Q.eq("child.name", "child_of"))
    assert len(nested_result.collect()) == 1
    assert nested_result.collect()[0].name == "object_3"

def test_nest_before_index(index):
    objs = [TestClass(name=f"object_{i}") for i in range(5)]
    for obj in objs:
        obj.child = TestClass(name="child_of", grandchild=TestClass(name="grandchild_of"))
    index.add_object_many(objs)
    nested_result = index.reduced_query(Q.eq("child.name", "child_of"))
    tripple_nested_result = index.reduced_query(Q.eq("child.grandchild.name", "grandchild_of"))

    assert len(nested_result.collect()) == 5
    assert all(r.child.name == "child_of" for r in nested_result.collect())

    assert len(tripple_nested_result.collect()) == 5
    assert all(r.child.grandchild.name == "grandchild_of" for r in tripple_nested_result.collect())


def test_tripple_nest_after_index(index):
    objs = [TestClass(name=f"object_{i}") for i in range(5)]
    index.add_object_many(objs)

    query = Q.eq("name", "object_3")
    result = index.reduced_query(query)
    assert len(result.collect()) == 1
    assert result.collect()[0].name == "object_3"

    for r in result.collect():
        r.child = TestClass(name="child_of")

    nested_result = index.reduced_query(Q.eq("child.name", "child_of"))
    assert len(nested_result.collect()) == 1
    assert nested_result.collect()[0].name == "object_3"

    nested_result.collect()[0].child.grandchild = TestClass(name="grandchild_of")
    tripple_nested_result = index.reduced_query(Q.eq("child.grandchild.name", "grandchild_of"))
    assert len(tripple_nested_result.collect()) == 1
    assert tripple_nested_result.collect()[0].name == "object_3"


def test_filtered_index_chain(index):
    objs = [TestClass(name=f"object_{i}", common=True) for i in range(5)]
    index.add_object_many(objs)

    query = Q.in_("name", ["object_1", "object_3"])
    result = index.reduced_query(query)
    assert len(result.collect()) == 2
    assert all(r.name in ["object_1", "object_3"] for r in result.collect())

    for r in result.collect():
        r.child = TestClass(name="child_of")

    nested_result = index.reduced_query(Q.eq("child.name", "child_of"))
    assert len(nested_result.collect()) == 2
    assert all(r.child.name == "child_of" for r in nested_result.collect())

    tripple_nested_result = nested_result.reduced_query(Q.eq("common", True))
    assert len(tripple_nested_result.collect()) == 2
    assert all(r.child.name == "child_of" for r in nested_result.collect())


def test_nested_object_query_in(index):
    class NestedTestClass(Indexable):
        pass

    nested_objs = [TestClass(num=i, nested=NestedTestClass(num=i * 10)) for i in range(5)]
    index.add_object_many(nested_objs)

    # Query based on nested object's attribute
    query = Q.in_(
        "nested.num", [ 20, 30, 40 ]
    )
    result = index.reduced_query(query).collect()
    assert len(result) == 3
    assert all(obj.nested.num in [20, 30, 40] for obj in result)


def test_nested_object_query_greater(index):
    class NestedTestClass(Indexable):
        pass

    nested_objs = [TestClass(num=i, nested=NestedTestClass(num=i * 10)) for i in range(11)]
    index.add_object_many(nested_objs)

    # Query based on nested object's attribute
    query = Q.gt(
        "nested.num", 50
    )
    result = index.reduced_query(query).collect()
    assert len(result) == 5
    assert all(obj.nested.num in [60, 70, 80, 90, 100] for obj in result)

        # Query based on nested object's attribute
    query = Q.ge(
        "nested.num", 50
    )
    result = index.reduced_query(query).collect()
    assert len(result) == 6
    assert all(obj.nested.num in [50, 60, 70, 80, 90, 100] for obj in result)


def test_nested_object_query_less(index):
    class NestedTestClass(Indexable):
        pass

    nested_objs = [TestClass(num=i, nested=NestedTestClass(num=i * 10)) for i in range(11)]
    index.add_object_many(nested_objs)

    # Query based on nested object's attribute
    query = Q.lt(
        "nested.num", 50
    )
    result = index.reduced_query(query).collect()
    assert len(result) == 5
    assert all(obj.nested.num in [0, 10, 20, 30, 40] for obj in result)

        # Query based on nested object's attribute
    query = Q.le(
        "nested.num", 50
    )
    result = index.reduced_query(query).collect()
    assert len(result) == 6
    assert all(obj.nested.num in [0, 10, 20, 30, 40, 50] for obj in result)


def test_nested_object_query_between(index):
    class NestedTestClass(Indexable):
        pass

    nested_objs = [TestClass(num=i, nested=NestedTestClass(num=i * 10)) for i in range(11)]
    index.add_object_many(nested_objs)

    # Query based on nested object's attribute
    query = Q.bt(
        "nested.num", 50, 90
    )
    result = index.reduced_query(query).collect()
    assert len(result) == 5
    assert all(obj.nested.num in [50, 60, 70, 80, 90] for obj in result)

def test_recursive_ownership(index):
    class Inner(Indexable):
        pass

    class Outer(Indexable):
        pass

    # Each Outer has an Inner, and each Inner also points back to another Outer (recursive chain).
    outers = []
    for i in range(6):
        inner = Inner(num=i * 10)
        outer = Outer(num=i, inner=inner)
        inner.outer = outer
        outers.append(outer)

    # Add objects (this should also add their nested children)
    index.add_object_many(outers)

    # Query across the recursive Inner objects
    query = Q.bt("inner.num", 20, 50)  # should capture 20, 30, 40, 50
    result = index.reduced_query(query).collect()

    pre_inner = result[0].inner
    result[0].inner = 7
    t_res = index.reduced_query(Q.eq("inner.outer.x", "y")).collect()
    assert len(t_res) == 0

    t_res = index.reduced_query(Q.eq("inner", 7)).collect()
    assert len(t_res) == 1
    result[0].inner_other = pre_inner

    result = index.reduced_query(Q.eq("inner.outer.inner.num", 2)).collect()
    assert len(result) == 0 # do not index children

def test_recursive_ownership_1(index):
    class Inner(Indexable):
        pass

    class Outer(Indexable):
        pass

    # Each Outer has an Inner, and each Inner also points back to another Outer (recursive chain).
    outers = []
    for i in range(6):
        inner = Inner(num=i * 10)
        outer = Outer(num=i, inner=inner)
        inner.outer = outer
        outers.append(outer)

    # Add objects (this should also add their nested children)
    index.add_object_many(outers)

    # Query across the recursive Inner objects
    query = Q.bt("inner.num", 20, 50)  # should capture 20, 30, 40, 50
    result = index.reduced_query(query).collect()

    # Ensure we got the correct number of results
    assert len(result) == 4
    nums = [obj.inner.num for obj in result]
    assert all(n in [20, 30, 40, 50] for n in nums)

    # And check that parent Outer.num values align with expectations
    outer_nums = [obj.num for obj in result]
    assert outer_nums == [2, 3, 4, 5]

    result = index.reduced_query(Q.eq("inner.num", 20)).collect()
    assert len(result) == 1
    assert result[0].num == 2
    assert result[0].inner.num == 20

    r = index.reduced_query(Q.eq("inner.outer.num", 2)).collect()
    assert len(r) == 0 # do not index children

    result[0].inner.outer.x = "y"
    result = index.reduced_query(Q.eq("x", "y")).collect()
    assert len(result) == 1
    assert result[0].x == "y"
    assert result[0].inner.outer.x == "y"

    result[0].inner = 7
    t_res = index.reduced_query(Q.eq("inner", 7)).collect()
    assert len(t_res) == 1
    assert t_res[0].inner == 7


if __name__ == "__main__":
    test_recursive_ownership_1(Index())
