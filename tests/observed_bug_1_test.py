
from PyThermite import Index, Indexable, QueryExpr as Q, FilteredIndex

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

def test_observed_bug_1():
    
    # root cause was that the internal b tree was not 
    # updating the offset when balancing on removal of a numerical key

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

    employees = person_index.reduced_query(
        Q.eq("employer.name", "Big Python Store")
    ).collect()
    assert len(employees) == 2
    assert {e.name for e in employees} == {"Alice", "Bob"}

