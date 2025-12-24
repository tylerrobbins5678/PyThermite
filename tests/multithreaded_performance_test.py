

from datetime import datetime
import random
import string

import numpy as np
from PyThermite.PyThermite import Index, Indexable, PyQueryExpr as Q
from threading import Thread

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

def prep_data(size):
    random.seed(42)
    np.random.seed(42)

    print("making build")
    make_data = [
        Record(
            **{
                "id": i,
                "age": np.random.randint(18, 80),
                "score": np.random.rand() * 100,
                "active": np.random.choice([True, False]),
                "country": np.random.choice(["US", "CA", "MX", "FR", "DE"]),
                "group": random_str(),
                "tags": np.random.choice(["a", "b", "c", "d"]),
            }
        )
        for i in range(size)
    ]

    return make_data


def multithreaded_add(thread_num, data):
    index = Index()

    def add_records(start, end):
        index.add_object_many(
            data[start:end]
        )

    threads = []
    records_per_thread = len(data) // thread_num

    for i in range(thread_num):
        start_index = i * records_per_thread
        end_index = (i + 1) * records_per_thread if i != thread_num - 1 else len(data)
        thread = Thread(target=add_records, args=(start_index, end_index))
        threads.append(thread)

    start = datetime.now()

    for thread in threads:
        thread.start()

    for thread in threads:
        thread.join()

    end = datetime.now()
    print(f"Time taken to add {len(data)} records with {thread_num} threads: {end - start}")

    assert len(index.collect()) == len(data)


if __name__ == "__main__":
    data = prep_data(5_000_000)
    multithreaded_add(1, data[:10_000])
    multithreaded_add(2, data[:10_000])

    multithreaded_add(1, data[:100_000])
    multithreaded_add(2, data[:100_000])
    multithreaded_add(4, data[:100_000])

    multithreaded_add(1, data[:500_000])
    multithreaded_add(2, data[:500_000])
    multithreaded_add(4, data[:500_000])

    multithreaded_add(1, data[:1_000_000])
    multithreaded_add(2, data[:1_000_000])
    multithreaded_add(4, data[:1_000_000])
    multithreaded_add(8, data[:1_000_000])

    multithreaded_add(1, data[:2_500_000])
    multithreaded_add(2, data[:2_500_000])
    multithreaded_add(4, data[:2_500_000])
    multithreaded_add(8, data[:2_500_000])

    multithreaded_add(1, data[:5_000_000])
    multithreaded_add(2, data[:5_000_000])
    multithreaded_add(4, data[:5_000_000])
    multithreaded_add(8, data[:5_000_000])
    multithreaded_add(16, data[:5_000_000])
