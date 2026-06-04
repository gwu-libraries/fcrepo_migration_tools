from collections import defaultdict
from itertools import islice
from typing import Set


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


class ChildQueue:
    def __init__(self, attr_func):
        self.parents_to_children = defaultdict(list)
        self.children = {}
        self.attr_func = attr_func
        self.counter = 0

    def stored(self, item) -> bool:
        parents = self.attr_func(item)
        if parents:
            self.parents_to_children[tuple(parents)].append(self.counter)
            self.children[self.counter] = item
            self.counter += 1
            return True
        return False

    @property
    def not_empty(self):
        return len(self.children) > 0

    def take(self, parents, n):
        return list(islice(self.get_children(parents), n))

    def get_children(self, parents: Set[str]):
        for parents_, children in self.parents_to_children.items():
            if children and (set(parents_) <= parents):
                while children:
                    item = children.pop()
                    yield self.children[item]
                    del self.children[item]
