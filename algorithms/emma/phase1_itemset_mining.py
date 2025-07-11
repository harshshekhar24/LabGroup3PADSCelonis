"""
Phase 1: Frequent Itemset Mining using Memory Anchors(FIMA)
This module implements the first phase of the EMMA algorithm.
It identifies frequent itemsets from a flattened temporal database using
memory anchors and local support thresholds.
"""

from collections import defaultdict, Counter


def build_indexDB(flat_data, min_support):
    # filter frequent 1-items in F1
    counts = Counter(event for _, event, _, _ in flat_data)
    F1 = sorted(item for item, c in counts.items() if c >= min_support)

    # create the indexDB based on the locations of frequent items
    filtered = [
        (time, event, pid, objs) for time, event, pid, objs in flat_data if event in F1
    ]
    filtered.sort(key=lambda x: (x[0], x[1]))

    indexDB = [
        (i + 1, time, event, pid, objs)
        for i, (time, event, pid, objs) in enumerate(filtered)
    ]

    # record for each frequent item the list of its locs
    item_locs = defaultdict(list)
    item_pids = defaultdict(set)
    item_objs = defaultdict(list)

    for loc, _, event, pid, objs in indexDB:
        item_locs[event].append(loc)
        item_pids[event].add(pid)
        item_objs[event].append(objs)

    return indexDB, item_locs, item_pids, item_objs, F1


def build_projected_loclist(prefix, indexDB):
    # group this indexDB by tid
    tid2locs = defaultdict(list)
    for loc, tid, event, pid, objs in indexDB:
        tid2locs[tid].append((loc, event))

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
    indexDB, item_locs, item_pids, _, F1 = build_indexDB(flat_data, min_support)

    # Mappings
    loc2tid = {loc: tid for loc, tid, _, _, _ in indexDB}
    loc2pid = {loc: pid for loc, _, _, pid, _ in indexDB}

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
            pids = {loc2pid[loc] for loc in locs}
            if len(pids) < min_support:
                continue  # prune infrequent extensions
            new_pref = prefix + (itm,)
            record(new_pref, locs)
            fimajoin(new_pref)

    # launch recursion from each singleton
    for itm in F1:
        fimajoin((itm,))

    return results
