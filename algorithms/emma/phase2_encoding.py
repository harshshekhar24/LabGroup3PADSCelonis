"""
Phase 2: Database Encoding & Bound List Construction
- Creates boundlist based on Tids
- Transforms the frequent itemsets into an encoded format spreading the IDs across time stamps based on the bound list.
"""

from collections import defaultdict

from algorithms.emma.phase1_itemset_mining import build_indexDB, mine_fima


def create_bound_list_from_tids(tid_list):
    return [(tid, tid) for tid in sorted(tid_list)]


def extract_boundlists_from_indexDB(flat_data, min_support):
    """
    Args:
        flat_data (List[Tuple[int, str, str, List[str]]]): List of (timestamp, event, pid, objects)
        min_support (int): Minimum support threshold (based on # of unique pids)

    Returns:
        List[Dict]: Each dict has keys:
            - 'ID': unique pattern ID
            - 'Itemsets': list of events in the pattern
            - 'LocList': list of indexDB positions where the pattern appears
            - 'Boundlist': list of (start, end) tuples (based on time index)
            - 'Objects': list of objects directly involved in the events at the given locs
    """
    results = mine_fima(flat_data, min_support)
    indexDB, _, _, item_objs, _ = build_indexDB(flat_data, min_support)
    # Mapping
    loc2tid = {loc: tid for loc, tid, _, _, _ in indexDB}
    loc2item = {loc: item for loc, _, item, _, _ in indexDB}

    item_locs = defaultdict(list)
    for loc, _, item, _, _ in indexDB:
        item_locs[item].append(loc)

    tuples = []
    for entry in results.values():
        itemset = list(entry["items"])
        locs = sorted(entry["locs"])
        tids = sorted({loc2tid[loc] for loc in locs})
        boundlist = [(tid, tid) for tid in tids]

        # Accumulate the specific objects tied to this pattern's locs only
        objs = []
        for loc in locs:
            item = loc2item[loc]
            index_in_item_locs = item_locs[item].index(loc)
            objs.extend(item_objs[item][index_in_item_locs])

        tuples.append((itemset, locs, boundlist, objs))

    def prepare_itemset_table(raw_tuples):
        itemset_table = []
        for idx, (itemset, locs, boundlist, objects) in enumerate(raw_tuples, start=1):
            itemset_table.append(
                {
                    "ID": idx,
                    "Itemsets": itemset,
                    "LocList": locs,
                    "Boundlist": boundlist,
                    "Objects": list(set(objects)),
                }
            )
        return itemset_table

    frequent_bounded_itemsets = prepare_itemset_table(tuples)

    return frequent_bounded_itemsets


def encode_itemsets_from_table(itemset_table):
    """
    Args:
        itemset_table (List[Dict]): List of dictionaries with keys:
            - 'ID': int, unique identifier for the itemset
            - 'Itemsets': list of str, the itemset
            - 'LocList': list of ints, location where it appears, used to create Boundlist
            - 'Boundlist': list of (start, end) tuples (TIDs)
    Returns:
        List[List[int]]: Encoded database where each row is a time slot and contains itemset IDs
    """
    # Determine the max_time by finding the maximum end time in the Boundlist
    max_time = 0
    for row in itemset_table:
        for start, end in row["Boundlist"]:
            max_time = max(max_time, end)

    encoded_db = defaultdict(list)

    for row in itemset_table:
        itemset_id = row["ID"]
        boundlist = row["Boundlist"]

        for start, end in boundlist:
            # Avoid unnecessary iterations by directly accessing relevant time slots
            for time_index in range(start, end + 1):
                encoded_db[time_index - 1].append(itemset_id)
    return encoded_db
