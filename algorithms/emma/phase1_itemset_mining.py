"""
Phase 1: Frequent Itemset Mining using Memory Anchors(FIMA)
This module implements the first phase of the EMMA algorithm.
It identifies frequent itemsets from a flattened temporal database using
memory anchors and local support thresholds.
"""

from collections import defaultdict, Counter


def build_indexDB(flat_data, min_support):
    # filter frequent 1-items in F1
    counts = Counter(item for tid, item in flat_data)
    F1 = sorted(item for item, c in counts.items() if c >= min_support)

    # create the indexDB based on the locations of frequent items
    filtered = [(tid, item) for tid, item in flat_data if item in F1]
    filtered.sort(key=lambda x: (x[0], x[1]))  # sort by time then item
    indexDB = [(i + 1, tid, item) for i, (tid, item) in enumerate(filtered)]

    # record for each frequent item the list of its locs
    item_locs = defaultdict(list)
    for loc, tid, item in indexDB:
        item_locs[item].append(loc)

    return indexDB, item_locs, F1


def build_loc_maps(indexDB):
    # quick lookups from loc to its transaction (tid) or item
    loc2tid = {loc: tid for loc, tid, _ in indexDB}
    loc2item = {loc: item for loc, _, item in indexDB}
    return loc2tid, loc2item


def build_projected_loclist(prefix, indexDB):
    # group this indexDB by tid
    tid2locs = defaultdict(list)
    for loc, tid, item in indexDB:
        tid2locs[tid].append((loc, item))

    projected = []
    for tid, loc_items in tid2locs.items():
        # all locs in this tid, and those belonging to our prefix
        all_locs = sorted(loc for loc, _ in loc_items)
        p_locs = sorted(loc for loc, itm in loc_items if itm in prefix)
        # require full‐occurrence and exact prefix start
        if len(p_locs) == len(prefix) and p_locs[0] == all_locs[0]:
            last = p_locs[-1]
            for loc, itm in loc_items:
                if loc > last:
                    projected.append((loc, tid, itm))
    return projected


def mine_fima(flat_data, min_support):
    indexDB, item_locs, F1 = build_indexDB(flat_data, min_support)
    loc2tid, loc2item = build_loc_maps(indexDB)

    results = {}
    next_id = 1

    def record(itemset, locs):
        nonlocal next_id
        support = len({loc2tid[ext] for ext in locs})
        results[next_id] = {"items": itemset, "locs": locs, "support": support}
        next_id += 1

    # Seed with all frequent single items
    for itm in F1:
        record((itm,), item_locs[itm])

    # Recursive prefix‐extension
    def fimajoin(prefix):
        proj = build_projected_loclist(prefix, indexDB)

        # group projected locs by candidate item
        local = defaultdict(list)
        for loc, tid, itm in proj:
            local[itm].append(loc)

        last = prefix[-1]
        for itm in sorted(local):
            if itm <= last:
                continue  # keep lexicographic prefix order
            locs = local[itm]
            if len({loc2tid[ext] for ext in locs}) < min_support:
                continue  # prune infrequent extensions
            new_pref = prefix + (itm,)
            record(new_pref, locs)
            fimajoin(new_pref)

    # launch recursion from each singleton
    for itm in F1:
        fimajoin((itm,))

    return results
