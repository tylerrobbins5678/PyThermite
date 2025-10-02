import random
import string
import numpy as np
import pytest

from PyThermite import Index
from PyThermite import Indexable
from PyThermite import QueryExpr as Q


import time
import pandas as pd
import psutil
import os

ITERATIONS = 1

class testclass:
    def __init__(self):
        pass

class Record(Indexable):

    def __repr__(self):
        return (
            f"Record(id={self.id}, age={self.age}, score={self.score:.1f}, "
            f"active={self.active}, country='{self.country}', group='{self.group}', tags='{self.tags}')"
        )
    
    def to_dict(self):
        return {
            "id": self.id,
            "age": self.age,
            "score": self.score,
            "active": self.active,
            "country": self.country,
            "group": self.group,
            "tags": self.tags,
        }


def random_str(length=6):
    return ''.join(random.choices(string.ascii_lowercase, k=length))

# Memory usage helper
def mem_usage_mb():
    return psutil.Process(os.getpid()).memory_info().rss / 1024 / 1024

@pytest.fixture
def prep_data_fixture():
    return prep_data()

def prep_data():
    N = 100_000
    random.seed(42)
    np.random.seed(42)

    print("making build")
    make_data = [
        {
            "id": i,
            "age": np.random.randint(18, 80),
            "score": np.random.rand() * 100,
            "active": np.random.choice([True, False]),
            "country": np.random.choice(["US", "CA", "MX", "FR", "DE"]),
            "group": random_str(),
            "tags": np.random.choice(["a", "b", "c", "d"]),
        }
        for i in range(N)
    ]

    return make_data


def test_performance(prep_data_fixture):
    print("creating Objects")
    start = time.perf_counter()
    data = [
        Record(
            **m
        )
        for m in prep_data_fixture
    ]
    object_build_time = time.perf_counter() - start

    py_objects = [row for row in data]

    start = time.perf_counter()
    cols = ["id", "age", "score", "active", "country", "group", "tags"]
    df = pd.DataFrame(prep_data_fixture, columns=cols)

    pandas_build_time = time.perf_counter() - start
    mem_pix = mem_usage_mb()

    print("Starting Pandas")
    # Benchmark: Pandas
    start = time.perf_counter()
    for i in range(ITERATIONS):
        filtered_df = df[
            (
                (df["age"].between(35, 60)) &
                (df["active"]) &
                (df["score"] > 85.0)
            ) |
            (
                (df["country"].isin(["CA", "MX"])) &
                (df["score"].between(50.0, 75.0)) &
                (df["tags"] != "b")
            )
        ]

        filtered_df = filtered_df[
            (filtered_df["group"] != "guest") &
            (filtered_df["age"] < 65) &
            (filtered_df["country"] != "US")
        ]

    filtered_df.to_dict(orient="records")
    duration_df = time.perf_counter() - start
    mem_df = mem_usage_mb()

    print("Starting building index")
    # Benchmark: Your index
    index = Index()
    start = time.perf_counter()
    index.add_object_many(py_objects)
    duration_bix = time.perf_counter() - start
    mem_bix = mem_usage_mb()

    print("Starting index")
    start = time.perf_counter()
    for i in range(ITERATIONS):
        query = Q.and_(
            Q.or_(
                Q.and_(
                    Q.eq("active", True),
                    Q.bt("age", 35, 60),
                    Q.gt("score", 85.0),
                ),
                Q.and_(
                    Q.in_("country", ["CA", "MX"]),
                    Q.bt("score", 50.0, 75.0),
                    Q.ne("tags", "b"),
                )
            ),
            Q.ne("group", "guest"),
            Q.ne("country", "US"),
            Q.lt("age", 65),
        )
        result = index.reduced_query(query)
    #    filtered_ix = index.reduced(b = 2, a = 1000)
    filtered_ix = result.collect()
    duration_ix = time.perf_counter() - start

    mem_ix = mem_usage_mb()

    # Print Results
    print("\n==== Benchmark Results ====")

    test = index.collect()[0]
    test1 = index.collect()[1]
    test.test_val = test1

    nested_test = index.reduced_query(
        query = Q.eq("test_val.age" ,  test1.age)
    ).collect()

    assert len(nested_test) > 0
    assert len(filtered_df) == len(filtered_ix)

    print(f"Object Build:                   {object_build_time:.4f} s")
    print(f"Pandas Filter Build index:      {pandas_build_time:.4f} s | Mem: {mem_pix:.1f} MB")
    print(f"Pandas Filter:                  {duration_df:.6f} s | Mem: {mem_df:.1f} MB | Result size: {len(filtered_df)}")
    print(f"Your Index Filter Build index:  {duration_bix:.4f} s | Mem: {mem_bix:.1f} MB")
    print(f"Your Index Filter:              {duration_ix:.6f} s | Mem: {mem_ix:.1f} MB | Result size: {len(filtered_ix)}")


# run the pytest with print statements visible
if __name__ == "__main__":
    data = prep_data()
    test_performance(data)