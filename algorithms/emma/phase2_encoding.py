"""
Phase 2: Database Encoding & Bound List Construction
 - Creates boundlist based on Tids
 - Transforms the frequent itemsets into an encoded format spreading the IDs across time stamps based on the bound list.
"""
from collections import defaultdict

from algorithms.emma.phase1_itemset_mining import build_indexDB, build_loc_maps, mine_fima


def create_bound_list_from_tids(tid_list):
    """
    Args:
        tid_list (List[int]): Sorted or unsorted list of transaction IDs (TIDs)

    Returns:
        bound_list (List[Tuple[int, int]]): List of (start, end) intervals covering consecutive TIDs
    """
    return [(tid, tid) for tid in sorted(tid_list)]


def extract_boundlists_from_indexDB(flat_data, min_support):
    """
    Args:
        flat_data (List[Tuple[int, str]]): Flat transaction data used for mining with TIDs and items
        min_support (int): Minimum support threshold for mining itemsets

    Returns:
        itemset_table (List[Dict]): List of dictionaries with keys:
            - 'ID': int, unique identifier for the itemset
            - 'Itemsets': list of str, the itemset
            - 'LocList': list of ints, location where it appears, used to create Boundlist
            - 'Boundlist': list of (start, end) tuples (TIDs)
    """
    results = mine_fima(flat_data, min_support)
    indexDB, _, _ = build_indexDB(flat_data, min_support)
    loc2tid, _ = build_loc_maps(indexDB)

    tuples = []
    for entry in results.values():
        itemset = list(entry["items"])
        locs = sorted(entry["locs"])
        tids = sorted({loc2tid[loc] for loc in locs})
        boundlist = create_bound_list_from_tids(tids)
        tuples.append((itemset, locs, boundlist))
        
    def prepare_itemset_table(raw_tuples):
        itemset_table = []
        for idx, (itemset, locs, boundlist) in enumerate(raw_tuples, start=1):
            itemset_table.append({
                "ID": idx,
                "Itemsets": itemset,
                "LocList": locs,
                "Boundlist": boundlist
            })
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
