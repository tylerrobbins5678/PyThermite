

class _Indexable:

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
    
    def add_index(self, index):
        self._index.add(index)

    def remove_index(self, index):
        self._index.remove(index)
