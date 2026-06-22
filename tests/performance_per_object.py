from dataclasses import dataclass
from datetime import datetime
from PyThermite.PyThermite import Index, Indexable, PyQueryExpr as Q

index = Index()

class PyThermiteThing(Indexable):
    pass

@dataclass
class PythonThing:
    x: int = 0
    pass

pyth = PyThermiteThing(x = 1)
index.add_object(pyth)
py = PythonThing(x = 1)

start = datetime.now()
for i in range(1_000_000):
    _ = py.x
print(f"native time taken {datetime.now() - start}")

start = datetime.now()
for i in range(1_000_000):
    py.x = i
print(f"native write time taken {datetime.now() - start}")

start = datetime.now()
for i in range(1_000_000):
    _ = pyth.x
print(f"PyThermite time taken {datetime.now() - start}")

start = datetime.now()
for i in range(1_000_000):
    pyth.x = i
print(f"PyThermite write time taken {datetime.now() - start}")

start = datetime.now()
for i in range(1_000_000):
    pass
print(f"loop time taken {datetime.now() - start}")
