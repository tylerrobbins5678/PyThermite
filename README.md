# PyThermite

PyThermite is a **performance-centered, native Python object indexer and graph datastore**.  
It allows you to index arbitrary Python objects, filter them by attributes and nested attributes, navigating graph-style relationships — all with **O(1)** lookups on attributes and **O(log n)** range queries.

---

## Core Design Principles

- **Python-first design**  
  Classes are the best way to enforce types. Class methods are the best way to mutate contents and communicate changes to a third-party datastore.

- **100% dynamic schema**  
  No schema or types are enforced at the datastore or query level. You can load any Python object, indexing all attributes.

- **Indexable schema objects**  
  Data objects are `Indexable` types. Updates made to the object immediately reflect on the `Index`.

- **Constant-time updates**  
  Unlike other data stores, schema updates are **O(1)** with no restructuring needed. This encourages iterative loading, filtering, mutating, and exporting.

---

## Features

- In-memory indexing of arbitrary Python objects
- Filter and query with attribute lookups or composable expressions
- Graph-style traversal from root nodes
- Perfect for ad-hoc migrations, analytics, and breaking down JSON

---

## Basic Usage

<pre> ```
from PyThermite import Index, Indexable, QueryExpr as Q

class Person(Indexable):
    name: str
    age: int
    employer: 'Store'
    wage: int

    def change_salary(self, amount: int):
        self.wage += amount


class Store(Indexable):
    name: str
    address: str
    owner: Person


person_index = Index()

big_python_store = Store(
    name="Big Python Store",
    address="123 Python St",
)

alice = Person(name="Alice", age=30, employer=big_python_store, wage=70000)
bob = Person(name="Bob", age=25, employer=big_python_store, wage=50000)

person_index.add_object_many([bob, alice])

is_30 = person_index.reduced_query(Q.eq("age", 30))
assert len(is_30.collect()) == 1
assert is_30.collect()[0].name == "Alice"
del is_30

high_wage = person_index.reduced_query(Q.gt("wage", 60000))
assert len(high_wage.collect()) == 1
assert high_wage.collect()[0].name == "Alice"
del high_wage

person: Person
for person in person_index.reduced_query(Q.lt("wage", 55_000)).collect():
    print(f"{person.name} works at {person.employer.name} and earns ${person.wage}")
    person.change_salary(10_000)

high_wage = person_index.reduced_query(Q.ge("wage", 60000))
assert len(high_wage.collect()) == 2
assert {p.name for p in high_wage.collect()} == {"Alice", "Bob"}
del high_wage

# nested queries
employees = person_index.reduced_query(
    Q.eq("employer.name", "Big Python Store")
).collect()
assert len(employees) == 2
assert {e.name for e in employees} == {"Alice", "Bob"}
``` </pre>

## Installation

```bash
pip install pythermite
