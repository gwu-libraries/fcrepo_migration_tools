from itertools import islice


class StaggeredQueue:
    """Implements a queue that allows items to be consumed in batches, such that in each batch only items with unique attributes are present, where the attribute can be specified by the caller."""

    def __init__(self, attr_fn):
        self.attr_fn = attr_fn
        self.data = []
        self.seen = []

    def add(self, item):
        self.data.append(item)

    @property
    def not_empty(self):
        return len(self.data) > 0

    def take(self, n):
        "Return first n items of the iterable as a list."
        return list(islice(self, n))

    def __iter__(self):
        self.seen = []
        self.index = 0
        return self

    def __next__(self):
        """Iteration is destructive of the queue -- each item consumed goes into the seen list, which is cleared out on the next iteration."""
        # If we have examined all the items in the list, this iteration stops
        if len(self.data) == 0 or self.index == len(self.data):
            raise StopIteration
        while self.attr_fn(self.data[self.index]) in self.seen:
            self.index += 1
            # Gone past the end of the list with no eligible items
            if self.index == len(self.data):
                raise StopIteration
        # Assuming there's at least one item with a unique attribute, return it and record its attribute
        item = self.data.pop(self.index)
        self.seen.append(self.attr_fn(item))
        return item
