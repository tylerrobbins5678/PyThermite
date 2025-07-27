import random
import string
import numpy as np

from py_index import Index
from py_index import Indexable
from py_index import QueryExpr as Q


import time
import pandas as pd
import psutil
import os

# Native Python object
class Record(Indexable):
    def __init__(self, id, age, score, active, country, group, tags):
        self.id = id
        self.age = age
        self.score = score
        self.active = active
        self.country = country
        self.group = group
        self.tags = tags

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
    

def random_str(length=5):
    return ''.join(random.choices(string.ascii_lowercase, k=length))

N = 100_000
ITERATIONS = 1
random.seed(42)
np.random.seed(42)

print("creating Objects")
data = [
    Record(
        id=i,
        age=np.random.randint(18, 80),
        score=np.random.rand() * 100,
        active=np.random.choice([True, False]),
        country=np.random.choice(["US", "CA", "MX", "FR", "DE"]),
        group=random_str(),
        tags=np.random.choice(["a", "b", "c", "d"]),
    )
    for i in range(N)
]


py_objects = [row for row in data]
df = pd.DataFrame([r.to_dict() for r in data])

print("objects created")

# Memory usage helper
def mem_usage_mb():
    return psutil.Process(os.getpid()).memory_info().rss / 1024 / 1024


print("Starting Pandas")
# Benchmark: Pandas
start = time.perf_counter()
for i in range(ITERATIONS):
    filtered_df = df[
        (
            (df["age"].between(35, 60)) &
            (df["score"] > 85.0) &
            (df["active"])
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

duration_df = time.perf_counter() - start
filtered_df.to_dict(orient="records")
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
                Q.bt("age", 35, 60),
                Q.gt("score", 85.0),
                Q.eq("active", True)
            ),
            Q.and_(
                Q.in_("country", ["CA", "MX"]),
                Q.bt("score", 50.0, 75.0),
                Q.ne("tags", "b")
            )
        ),
        Q.ne("group", "guest"),
        Q.lt("age", 65),
        Q.ne("country", "US")
    )
    result = index.reduced_query(query)
#    filtered_ix = index.reduced(b = 2, a = 1000)
duration_ix = time.perf_counter() - start
filtered_ix = result.collect()

mem_ix = mem_usage_mb()

# Print Results
print("\n==== Benchmark Results ====")
#print(f"Python List Comp:   {duration_py:.6f} s | Mem: {mem_py:.1f} MB | Result size: {len(filtered_py)}")
print(f"Pandas Filter:      {duration_df:.6f} s | Mem: {mem_df:.1f} MB | Result size: {len(filtered_df)}")
print(f"Your Index Filter Build index:  {duration_bix:.4f} s | Mem: {mem_bix:.1f} MB")
print(f"Your Index Filter:  {duration_ix:.6f} s | Mem: {mem_ix:.1f} MB | Result size: {len(filtered_ix)}")
