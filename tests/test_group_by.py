import pytest

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

def test_non_nested_many_children(index: Index):
    vals = [0,1,2,3,4,5,6,7,8,9]
    for i in range(10):
        print(vals[i:])
        index.add_object(TestClass(id=1, num = i//2, nested = vals[i:]))

    grouped = index.group_by("nested")

    for i in vals:
        assert len(grouped[i].collect()) == i + 1

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

def test_nested_group_by_with_many_children(index: Index):
    for iteration in range(10):
        t = [i for i in range(iteration)]
        nested = TestClass(nest=True, children=t)
        index.add_object(TestClass(id=1, nested=nested))

    grouped = index.group_by("nested.children")

    for i in grouped:
        assert len(grouped[i].collect()) == 9-i

def test_group_by_one_to_many(index: Index):
    nested = TestClass(nest=True)
    for i in range(10):
        index.add_object(TestClass(id=i, nested=nested))

    grouped = index.group_by("nested.nest")

    all = grouped[True].collect()
    assert len(all) == 10

def test_group_by_many_to_many(index: Index):
    nested = [TestClass(num = i, nest=True, id = i) for i in range(10)]
    for i in range(10):
        index.add_object(TestClass(id=i, nested=nested))


    grouped = index.group_by("nested.num")
    # change any one from nested
    grouped[0].collect()[0].nested[0].idx = 100

    # all changes should reflect
    for ind in index.collect():
        assert ind.nested[0].idx == 100

def test_group_by_one_to_many_deregister(index: Index):
    nested = TestClass(nest=True, num = 100)
    for i in range(10):
        index.add_object(TestClass(id=i, nested=nested))

    grouped = index.group_by("nested.nest")

    all = grouped[True].collect()
    assert len(all) == 10

    all[0].nested = "test"

    grouped = index.group_by("nested.nest")

    all = grouped[True].collect()
    assert len(all) == 9

    grouped = index.group_by("nested.num")
    all = grouped[100].collect()
    assert len(all) == 9

def test_remove_and_reassign_tracked_list(index: Index):
    nested = [TestClass(num=i, id=i) for i in range(3)]
    
    objs = [TestClass(id=i, nested=nested) for i in range(5)]
    for obj in objs:
        index.add_object(obj)

    grouped = index.group_by("nested.num")
    assert len(grouped[0].collect()) == 5

    for obj in objs:
        obj.nested = []

    grouped_after_remove = index.group_by("nested.num")
    assert all(len(v.collect()) == 0 for _, v in grouped_after_remove.items())

    new_nested = [TestClass(num=100 + i, id=10 + i) for i in range(3)]
    for obj in objs:
        obj.nested = new_nested

    grouped_after_reassign = index.group_by("nested.num")
    first_group = grouped_after_reassign[100].collect()
    assert all(o.nested[0].num == 100 for o in first_group)

    new_nested[0].num = 999
    grouped_after_mutation = index.group_by("nested.num")
    first_group = grouped_after_mutation[999].collect()
    assert all(o.nested[0].num == 999 for o in first_group)

def test_nested_group_by_many_to_many(index: Index):
    children = [TestClass(num=i) for i in range(1000)]

    for c in children:
        c.child = [TestClass(nested_num=i) for i in range(3)]

    index.add_object_many(children)

    groups = index.group_by("child.nested_num")

    for i in range(3):
        assert len(groups[i].collect()) == 1000, f"{i} has invalid length"
