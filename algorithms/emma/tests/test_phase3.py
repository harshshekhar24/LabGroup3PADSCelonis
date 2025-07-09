from algorithms.emma.phase3_episode_mining import mine_serial_episodes


def test_mine_serial_episodes():
    # Frequent itemsets from Phase I
    frequent_itemsets = {
        1: {"items": {"A", "C", "F"}, "times": [1, 4]},
        2: {"items": {"B", "D"}, "times": [3, 5]},
    }
    # Encoded DB from Phase II
    encoded_db = [
        [],
        [1],
        [],
        [2],
        [1],
        [2],
    ]
    minsup = 2
    maxwin = 3

    episodes = mine_serial_episodes(frequent_itemsets, encoded_db, minsup, maxwin)
    expected = {
        (1,): [(1, 3), (4, 5)],
        (2,): [(3, 5), (5, 5)],
        (1, 2): [(3, 3), (5, 5)],
    }

    assert episodes == expected
