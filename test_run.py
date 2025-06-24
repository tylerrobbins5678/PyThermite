
from py_index import Index
from py_index import Indexable


import time
import random
import pandas as pd
import psutil
import os

# Native Python object
class MyObject(Indexable):
    def __init__(self, a, b, c):
        self.a = a
        self.b = b
        self.c = c


# Dataset size
N = 1_000_000
ITERATIONS = 1

print("creating Objects")
# Generate identical data
data = [(i % 1024, i % 3, f"label_{i}") for i in range(N)]
py_objects = [MyObject(*row) for row in data]
df = pd.DataFrame(data, columns=["a", "b", "c"])

print("objects created")

# Memory usage helper
def mem_usage_mb():
    return psutil.Process(os.getpid()).memory_info().rss / 1024 / 1024

print("Starting Python")
# Benchmark: Python list comprehension
start = time.perf_counter()
for i in range(ITERATIONS):
    filtered_py = [o for o in py_objects if o.a == 1000 and o.b == 2]
duration_py = time.perf_counter() - start
mem_py = mem_usage_mb()

print("Starting Pandas")
# Benchmark: Pandas
start = time.perf_counter()
for i in range(ITERATIONS):
    # filtered_df = df[(df["a"] == 1000) & (df["b"] == 2)]
    filtered_df = df[(df["b"] == 2)]
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
    filtered_ix = index.get_by_attribute(b = 2)
#    filtered_ix = index.reduced(b = 2)
#    filtered_ix = filtered_ix.collect()
duration_ix = time.perf_counter() - start
mem_ix = mem_usage_mb()

# Print Results
print("\n==== Benchmark Results ====")
print(f"Python List Comp:   {duration_py:.6f} s | Mem: {mem_py:.1f} MB | Result size: {len(filtered_py)}")
print(f"Pandas Filter:      {duration_df:.6f} s | Mem: {mem_df:.1f} MB | Result size: {len(filtered_df)}")
print(f"Your Index Filter Build index:  {duration_bix:.4f} s | Mem: {mem_bix:.1f} MB")
print(f"Your Index Filter:  {duration_ix:.6f} s | Mem: {mem_ix:.1f} MB | Result size: {len(filtered_ix)}")


# class TestObj:
# 
#     def __init__(self, **kwargs):
#         for k,v in kwargs.items():
#             setattr(self, k, v)
# 
# class Test(Indexable):
#     def __init__(self,k):
#         self.key = k
# 
# 
# if __name__ == "__main__":
#     import time
#     start = time.perf_counter()
# 
#     ind = Index()
#     for i in range(100_000):
#         x = Test(f"val{i}")
#         x.y = 0
#         ind.add_object(x)
#         x.x = 12
#         x.z = 13
# 
#     end = time.perf_counter()
#     print(f"Rust index lookup took {end - start:.6f} seconds")
# 
#     start = time.perf_counter()
#     for i in range(1_000_000):
#         ind.reduced(key="val1", y=0)
# 
#     end = time.perf_counter()
#     print(f"Rust index lookup took {end - start:.6f} seconds")
# 
#     ind.get_by_attribute(key=["val1", "val2"], val="val22", no_exist_val=21, y=12)
#     ind.get_by_attribute(key=["val1", "val2"], y=12)
#     ind.get_by_attribute(key="val1")
#     print(ind)