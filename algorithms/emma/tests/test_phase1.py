import pytest
from collections import Counter, defaultdict

from algorithms.emma.phase1_itemset_mining import (
    build_indexDB,
    build_loc_maps,
    build_projected_loclist,
    mine_fima,
)


@pytest.fixture
def simple_flat_data():
    return [
        (1, "A"),
        (1, "C"),
        (1, "F"),
        (3, "B"),
        (3, "D"),
        (4, "A"),
        (4, "C"),
        (4, "F"),
        (5, "B"),
        (5, "D"),
        (5, "E"),
    ]


def test_build_indexDB(simple_flat_data):
    indexDB, item_locs, F1 = build_indexDB(simple_flat_data, min_support=2)
    counts = Counter(item for _, item in simple_flat_data)
    expected_F1 = sorted(i for i, c in counts.items() if c >= 2)
    assert F1 == expected_F1

    filtered = [(tid, item) for tid, item in simple_flat_data if item in F1]
    filtered.sort(key=lambda x: (x[0], x[1]))
    assert [(tid, item) for _, tid, item in indexDB] == filtered
    assert [loc for loc, _, _ in indexDB] == list(range(1, len(filtered) + 1))

    loc_map = defaultdict(list)
    for loc, _, item in indexDB:
        loc_map[item].append(loc)
    assert dict(item_locs) == dict(loc_map)


def test_build_loc_maps(simple_flat_data):
    indexDB, _, _ = build_indexDB(simple_flat_data, min_support=2)
    loc2tid, loc2item = build_loc_maps(indexDB)
    for loc, tid, item in indexDB:
        assert loc2tid[loc] == tid
        assert loc2item[loc] == item


def test_build_projected_list_AC(simple_flat_data):
    indexDB, item_locs, F1 = build_indexDB(simple_flat_data, min_support=2)
    loc2tid, loc2item = build_loc_maps(indexDB)

    # Project from prefix ('A',)
    proj_A = build_projected_loclist(("A",), indexDB)
    items_A = {item for _, _, item in proj_A}
    assert items_A == {"C", "F"}

    # Project from prefix ('A','C')
    proj_AC = build_projected_loclist(("A", "C"), indexDB)
    items_AC = {item for _, _, item in proj_AC}
    assert items_AC == {"F"}


def test_mine_fima_simple(simple_flat_data):
    res = mine_fima(simple_flat_data, min_support=2)
    itemsets = {tuple(v["items"]) for v in res.values()}

    # Singletons
    for s in ["A", "B", "C", "D", "F"]:
        assert (s,) in itemsets

    # Only these multi-item patterns
    assert ("A", "C", "F") in itemsets
    assert ("B", "D") in itemsets

    # Check supports via loc→tid mapping
    loc2tid, _ = build_loc_maps(build_indexDB(simple_flat_data, 2)[0])

    def tids_of(itemset):
        locs = next(v["locs"] for v in res.values() if v["items"] == itemset)
        return {loc2tid[ext] for ext in locs}

    assert tids_of(("A", "C", "F")) == {1, 4}
    assert tids_of(("B", "D")) == {3, 5}
