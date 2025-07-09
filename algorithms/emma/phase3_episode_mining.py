"""
Phase 3: Serial Episode Mining
Performs mining of frequent serial episodes using projected bound lists.
Uses the encoded database and bound lists from Phase 2.
"""

from collections import defaultdict
from algorithms.emma.phase2_encoding import encode_itemsets_from_table, extract_boundlists_from_indexDB


def compute_projected_boundlist(boundlist, maxwin, max_time):
    projected = []
    for ts, te in boundlist:
        ts_proj = te + 1
        te_proj = min(ts + maxwin - 1, max_time)
        if ts_proj <= te_proj:
            projected.append((ts_proj, te_proj))
    return projected

def get_local_frequent_ids(pbl, encoded_db, minsup):
    """
    Get frequent IDs appearing within the given projected bound list (1-based indexing) in the encoded database.
    
    Parameters:
        pbl (List[Tuple[int, int]]): List of bounds with (start, end) inclusive, using 1-based indexing.
        encoded_db (List[List[int]]): Encoded database where index 0 corresponds to time slot 1.
        minsup (int): Minimum support threshold.
    
    Returns:
        List[int]: List of frequent item IDs.
    """
    count_dict = defaultdict(int)

    for start, end in pbl:
        for i in range(start, end + 1):
            idx = i - 1  # adjust for 1-based indexing
            if 0 <= idx < len(encoded_db):
                for item_id in encoded_db[idx]:
                    count_dict[item_id] += 1

    return [item_id for item_id, count in count_dict.items() if count >= minsup]


def temporal_join(episode_boundlist, f_boundlist, maxwin):
    """
    Compute the temporal join of an episode's boundlist and a frequent 1-pattern's boundlist.
    
    Parameters:
        episode_boundlist (List[Tuple[int, int]]): Boundlist of current episode.
        f_boundlist (List[Tuple[int, int]]): Boundlist of the frequent 1-pattern (should be (ts', ts') pairs).
        maxwin (int): Maximum window constraint.
    
    Returns:
        List[Tuple[int, int]]: New boundlist from temporal join.
    """
    new_boundlist = []

    for ts_i, te_i in episode_boundlist:
        for ts_f, _ in f_boundlist:  # since f_boundlist is of the form [(ts', ts')]
            if ts_f > te_i and (ts_f - ts_i) < maxwin:
                new_boundlist.append((ts_i, ts_f))
    
    return new_boundlist


def emmajoin(episode, boundlist, maxwin, max_time, encoded_db, minsup, itemset_table, results):
    pbl = compute_projected_boundlist(boundlist, maxwin, max_time)
    LFP = get_local_frequent_ids(pbl, encoded_db, minsup)

    for eid in LFP:
        item = next(item for item in itemset_table if item['ID'] == eid)
        tempBoundlist = temporal_join(boundlist, item['Boundlist'], maxwin)
        temp_pbl = compute_projected_boundlist(tempBoundlist, maxwin, max_time)
        support = len(temp_pbl)
        new_episode = episode + (eid,)
        episode_structured = [
            {
                "activity": item["Itemsets"],
                "objects": item.get("Objects", [])  # default to empty list if missing
            }
            for item in itemset_table if item["ID"] in new_episode
        ]


        results.append({
            "PatternID": new_episode,
            "Episode": episode_structured,
            "Support": len(tempBoundlist)
        })
        if support >= minsup:
            emmajoin(new_episode, tempBoundlist, maxwin, max_time, encoded_db, minsup, itemset_table, results)

def run_emma(flat_data, minsup, maxwin):
    itemset_table = extract_boundlists_from_indexDB(flat_data, minsup)
    encoded_db = encode_itemsets_from_table(itemset_table)
    max_time = len(encoded_db)

    results = []

    for row in itemset_table:
        fid = row['ID']
        boundlist = row['Boundlist']
        pbl = compute_projected_boundlist(boundlist, maxwin, max_time)
        support = len(pbl)

        if support >= minsup:
            episode = (fid,)
            results.append({
                "PatternID": (episode),
                "Episode": [{
                    "activity": row["Itemsets"],
                    "objects": row.get("Objects", [])  # use [] if 'Objects' missing
                }],
                "Support": len(boundlist)
            })
            emmajoin(episode, boundlist, maxwin, max_time, encoded_db, minsup, itemset_table, results)
    return results

