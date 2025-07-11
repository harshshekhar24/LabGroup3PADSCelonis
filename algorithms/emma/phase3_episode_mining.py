"""
Phase 3: Serial Episode Mining
Performs mining of frequent serial episodes using projected bound lists.
Uses the encoded database and bound lists from Phase 2.
"""

from collections import defaultdict
from algorithms.emma.phase2_encoding import (
    encode_itemsets_from_table,
    extract_boundlists_from_indexDB,
)
from prototypes.draft.functions import normalize_timestamps


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
    new_boundlist = []
    for ts_i, te_i in episode_boundlist:
        window_end = ts_i + maxwin - 1
        for ts_f, _ in f_boundlist:
            if te_i < ts_f <= window_end:
                new_boundlist.append((ts_i, ts_f))
    return new_boundlist


def emmajoin(
    episode, boundlist, maxwin, max_time, encoded_db, minsup, itemset_table, results
):
    pbl = compute_projected_boundlist(boundlist, maxwin, max_time)
    LFP = get_local_frequent_ids(pbl, encoded_db, minsup)

    for eid in LFP:
        item = next(item for item in itemset_table if item["ID"] == eid)
        tempBoundlist = temporal_join(boundlist, item["Boundlist"], maxwin)
        temp_pbl = compute_projected_boundlist(tempBoundlist, maxwin, max_time)
        support = len(temp_pbl)
        new_episode = episode + (eid,)
        episode_structured = [
            {
                "activity": item["Itemsets"],
                "objects": item.get("Objects", []),  # default to empty list if missing
            }
            for item in itemset_table
            if item["ID"] in new_episode
        ]

        results.append(
            {
                "PatternID": new_episode,
                "Episode": episode_structured,
                "Support": len(tempBoundlist),
            }
        )
        if support >= minsup:
            emmajoin(
                new_episode,
                tempBoundlist,
                maxwin,
                max_time,
                encoded_db,
                minsup,
                itemset_table,
                results,
            )


def run_emma(flat_data, minsup, maxwin):
    itemset_table = extract_boundlists_from_indexDB(flat_data, minsup)
    encoded_db = encode_itemsets_from_table(itemset_table)
    max_time = len(encoded_db)

    results = []

    for row in itemset_table:
        fid = row["ID"]
        boundlist = row["Boundlist"]
        pbl = compute_projected_boundlist(boundlist, maxwin, max_time)
        support = len(pbl)

        if support >= minsup:
            episode = (fid,)
            results.append(
                {
                    "PatternID": (episode),
                    "Episode": [
                        {
                            "activity": row["Itemsets"],
                            "objects": row.get(
                                "Objects", []
                            ),  # use [] if 'Objects' missing
                        }
                    ],
                    "Support": len(boundlist),
                }
            )
            emmajoin(
                episode,
                boundlist,
                maxwin,
                max_time,
                encoded_db,
                minsup,
                itemset_table,
                results,
            )
    return results


def group_by_pid(flat_data):
    pid_map = defaultdict(list)
    for time, event, pid, objs in flat_data:
        pid_map[pid].append((time, event, pid, objs))
    return pid_map


def run_emma_per_trace(flat_data, minsup, maxwin):
    pid_traces = group_by_pid(flat_data)
    episode_counts = defaultdict(set)
    episode_objects = defaultdict(list)
    global_pattern_registry = {}
    next_id = 1

    # Go through each process execution (trace)
    for pid, trace in pid_traces.items():
        if len(trace) < 2:
            continue

        norm_trace = normalize_timestamps(trace)
        episodes = run_emma(norm_trace, minsup=1, maxwin=maxwin)
        for ep in episodes:
            # Define unique structure key: sorted tuple of sorted activities per step
            structure = tuple(tuple(sorted(step["activity"])) for step in ep["Episode"])

            # Register globally if not yet seen
            if structure not in global_pattern_registry:
                global_pattern_registry[structure] = next_id
                next_id += 1

            # Collect info
            episode_counts[structure].add(pid)
            episode_objects[structure].append(ep["Episode"])

    # Aggregate and return globally frequent episodes
    results = []
    for structure, pids in episode_counts.items():
        if len(pids) >= minsup:
            pattern_id = global_pattern_registry[structure]

            # Merge activities and deduplicate objects
            episode_steps = []
            num_steps = len(structure)
            for i in range(num_steps):
                activities = list(structure[i])
                all_objects = [
                    obj for ep in episode_objects[structure] for obj in ep[i]["objects"]
                ]
                unique_objects = list(set(all_objects))
                episode_steps.append(
                    {"activity": activities, "objects": unique_objects}
                )

            results.append(
                {
                    "PatternID": pattern_id,
                    "Episode": episode_steps,
                    "Support": len(pids),
                }
            )

    return results
