
class meta(type):
    def __init__(cls,name,bases,dct):
        def auto__call__init__(self, *a, **kw):
            for base in cls.__bases__:
                base.__init__(self, *a, **kw)
            cls.__init__child_(self, *a, **kw)
        cls.__init__child_ = cls.__init__
        cls.__init__ = auto__call__init__


class Indexable:

    @property
    def _index(self) -> set:
        if not hasattr(self, "_index_obj"):
            self.__dict__["_index_obj"] = set()
        
        return self._index_obj

    def __setattr__(self, name, value):
        if self._index:
            if hasattr(self, name):
                old_val = getattr(self, name)
            else:
                old_val = None
            super().__setattr__(name, value)
            for i in self._index:
                i.update_index(self, name, old_val)
        else:
            super().__setattr__(name, value)
    
    def add_index(self, index: 'Index'):
        self._index.add(index)

    def remove_index(self, index: 'Index'):
        self._index.remove(index)
