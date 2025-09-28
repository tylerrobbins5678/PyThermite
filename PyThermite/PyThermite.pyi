

class Index:
    '''
    Index is a collection of Indexable objects that can be queried and filtered.
    It supports adding and removing objects, as well as querying via attribute filters or query expressions.
    Experemential Thread safe, but not officially supported yet.
    '''

    def collect() -> list[Indexable]: 
        '''
        collects all valid objects in the index and returns them as a list
        '''
    ...
    def add_object(obj: Indexable): 
        '''
        adds a single object to the index
        '''
    ...
    def add_object_many(objects: list[Indexable]):
        '''
        adds a multiple objects to the index
        '''
    ...
    def reduce(**kwargs): 
        '''
        removes items in place that do not match the given attribute filters
        Note that this modifies the current index in place and is less performant 
        than using a method that returns a filteredIndex such as reduced_query or reduced        
        '''
    ...
    def reduced(**kwargs) -> FilteredIndex:
        '''
        returns a FilteredIndex containing only items that match the given attribute filters
        '''
    ...
    def reduced_query(query: PyQueryExpr) -> FilteredIndex:
        '''
        returns a FilteredIndex containing only items that match the given query expression
        '''
    ...
    def get_by_attribute(**kwargs):
        '''
        a shorthand for reduced(**kwargs).collect()
        more performant than using reduced when you only need the collected results
        and not the FilteredIndex to further query
        '''    
    ...
    def union_with(other: Index):
        '''
        returns a new Index that is the union of this index and another index
        does not mutate the other index
        '''    
    ...

class FilteredIndex:
    '''
    FilteredIndex is a view into an Index with an allow list of items.
    It supports the same querying and filtering operations as Index
    but does not support adding or removing objects.
    '''
    def reduced(**kwargs) -> FilteredIndex:
        '''
        returns a FilteredIndex containing only items that match the given attribute filters
        '''    
    ...
    def reduced_query(query: PyQueryExpr) -> FilteredIndex:
        '''
        returns a FilteredIndex containing only items that match the given query expression
        '''
    ...
    def collect() -> list[Indexable]:
        '''
        collects all valid objects in the FilteredIndex and returns them as a list
        '''
    ...
    def rebase() -> Index:
        '''
        returns a new Index containing only the items in this FilteredIndex
        Only use this when a full Index is needed, as it is much less performant
        '''    
    ...

class PyQueryExpr:
    def and_(queries: list[PyQueryExpr]) -> PyQueryExpr:
        '''
        all subqueries must be true
        '''    
    ...
    def or_(queries: list[PyQueryExpr]) -> PyQueryExpr:
        '''
        at least one subquery must be true
        '''    
    ...
    def not_(queries: PyQueryExpr) -> PyQueryExpr:
        '''
        negates the subquery
        '''    
    ...

    def eq(attr: str, value: any) -> PyQueryExpr:
        '''
        attribute equals value
        '''    
    ...
    def in_(attr: str, values: list[any]) -> PyQueryExpr:
        '''
        attribute is in the list of values
        equivilant to multiple or'd eq queries
        '''
    ...
    def ne(attr: str, value: any) -> PyQueryExpr:
        '''
        attribute does not equal value
        '''    
    ...

    def gt(attr: str, value: int | float) -> PyQueryExpr:
        '''
        attribute is greater than value
        '''
    ...
    def ge(attr: str, value: int | float) -> PyQueryExpr:
        '''
        attribute is greater than or equal to value
        '''    
    ...
    def lt(attr: str, value: int | float) -> PyQueryExpr:
        '''
        attribute is less than value
        '''
    ...
    def le(attr: str, value: int | float) -> PyQueryExpr:
        '''
        attribute is less than or equal to value
        '''    
    ...
    def bt(attr: str, lower: int | float, upper: int | float) -> PyQueryExpr:
        '''
        attribute is between lower and upper, inclusive
        '''    
    ...

class Indexable:
    '''
    Base class for objects that can be indexed.
    All attributes will be indexed unless the attribute name is prefixed with an underscore
    Nested Indexable objects are supported and fully queryable via dot notation.
    '''
...
