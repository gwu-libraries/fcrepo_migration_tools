from random import shuffle

import pytest

from pytools.queue import ChildQueue, StaggeredQueue

ALPHABET = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"


@pytest.fixture()
def all_unique():
    l = list(zip(ALPHABET, list(range(26))))
    shuffle(l)
    return l


@pytest.fixture()
def repeated_attributes():
    items = []
    for letter in ALPHABET:
        for number in range(10):
            items.append((letter, number))
    shuffle(items)
    return items


@pytest.fixture()
def parents():
    return [{"parents": [], "work": a} for a in ALPHABET]


@pytest.fixture()
def works_with_parents():
    return [{"parents": [a], "work": a.lower()} for a in ALPHABET]


def test_queue_unique_items(all_unique):
    q = StaggeredQueue(lambda x: x[0])
    for item in all_unique:
        q.add(item)
    consumed = [item for item in q]
    assert len(consumed) == 26
    assert len({c[0] for c in consumed}) == len(consumed)


def test_queue_repeated_attrs(repeated_attributes):
    all_consumed = []
    q = StaggeredQueue(lambda x: x[0])
    for item in repeated_attributes:
        q.add(item)
    for _ in range(26):
        consumed = [item for item in q.take(10)]
        assert len(consumed) <= 10
        assert len({c[0] for c in consumed}) == len(consumed)
        all_consumed.extend(consumed)
    while q.not_empty:
        for c in q:
            all_consumed.append(c)
    assert len({c for c in all_consumed}) == len(repeated_attributes)


def test_child_queue(parents, works_with_parents):
    q = ChildQueue(attr_func=lambda x: x["parents"])
    for p in parents:
        assert not q.stored(p)
    for w in works_with_parents:
        assert q.stored(w)
    some_parents = {p["work"] for p in parents[:10]}
    children = [w for w in q.get_children(some_parents)]
    assert len(children) == 10
    assert {p for c in children for p in c["parents"]} == some_parents
    assert len(q.children) == 16
    other_parents = {p["work"] for p in parents[10:15]}
    other_children = [w for w in q.take(other_parents, 5)]
    assert {p for c in other_children for p in c["parents"]} == other_parents
