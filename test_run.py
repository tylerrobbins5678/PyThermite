
from py_index import Index
from py_index import Indexable


class TestObj:

    def __init__(self, **kwargs):
        for k,v in kwargs.items():
            setattr(self, k, v)

class Test(Indexable):
    def __init__(self,k):
        self.key = k


if __name__ == "__main__":
    import time
    start = time.perf_counter()

    ind = Index()
    for i in range(100_000):
        x = Test(f"val{i}")
        x.y = 0
        ind.add_object(x)
        x.x = 12
        x.z = 13

    end = time.perf_counter()
    print(f"Rust index lookup took {end - start:.6f} seconds")

    start = time.perf_counter()
    for i in range(1_000_000):
        ind.reduced(key="val1", y=0)

    end = time.perf_counter()
    print(f"Rust index lookup took {end - start:.6f} seconds")

    ind.get_by_attribute(key=["val1", "val2"], val="val22", no_exist_val=21, y=12)
    ind.get_by_attribute(key=["val1", "val2"], y=12)
    ind.get_by_attribute(key="val1")
    print(ind)