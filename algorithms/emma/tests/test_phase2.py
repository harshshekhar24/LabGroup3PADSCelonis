from algorithms.emma import phase2_encoding


def test_encode_itemsets_from_table():
    itemset_table = [
        {
            "ID": 1,
            "Itemsets": ["A"],
            "LocList": [1, 6, 12, 14, 19, 25],
            "Boundlist": [(1, 1), (4, 4), (7, 7), (8, 8), (11, 11), (14, 14)],
        },
        {
            "ID": 2,
            "Itemsets": ["B"],
            "LocList": [4, 10, 17, 22, 29],
            "Boundlist": [(3, 3), (6, 6), (9, 9), (12, 12), (16, 16)],
        },
        {
            "ID": 3,
            "Itemsets": ["C"],
            "LocList": [2, 7, 15, 20, 26, 28],
            "Boundlist": [(1, 1), (4, 4), (8, 8), (11, 11), (14, 14), (15, 15)],
        },
        {
            "ID": 4,
            "Itemset": ["D"],
            "LocList": [5, 9, 11, 18, 23, 24, 30],
            "Boundlist": [(3, 3), (5, 5), (6, 6), (9, 9), (12, 12), (13, 13), (16, 16)],
        },
        {
            "ID": 5,
            "Itemsets": ["F"],
            "LocList": [3, 8, 13, 16, 21, 27],
            "Boundlist": [(1, 1), (4, 4), (7, 7), (8, 8), (11, 11), (14, 14)],
        },
        {
            "ID": 6,
            "Itemsets": ["A", "C"],
            "LocList": [2, 7, 15, 20, 26],
            "Boundlist": [(1, 1), (4, 4), (8, 8), (11, 11), (14, 14)],
        },
        {
            "ID": 7,
            "Itemsets": ["A", "C", "F"],
            "LocList": [3, 8, 16, 21, 27],
            "Boundlist": [(1, 1), (4, 4), (8, 8), (11, 11), (14, 14)],
        },
        {
            "ID": 8,
            "Itemsets": ["A", "F"],
            "LocList": [3, 8, 13, 16, 21, 27],
            "Boundlist": [(1, 1), (4, 4), (7, 7), (8, 8), (11, 11), (14, 14)],
        },
        {
            "ID": 9,
            "Itemsets": ["B", "D"],
            "LocList": [5, 11, 18, 23, 30],
            "Boundlist": [(3, 3), (6, 6), (9, 9), (12, 12), (16, 16)],
        },
    ]
    encoded_db = phase2_encoding.encode_itemsets_from_table(itemset_table)

    expected_encoded_db = [
        [],  # Time 0
        [1, 3, 5, 6, 7, 8],  # Time 1
        [],  # Time 2
        [2, 4, 9],  # Time 3
        [1, 3, 5, 6, 7, 8],  # Time 4
        [4],  # Time 5
        [2, 4, 9],  # Time 6
        [1, 5, 8],  # Time 7
        [1, 3, 5, 6, 7, 8],  # Time 8
        [2, 4, 9],  # Time 9
        [],  # Time 10
        [1, 3, 5, 6, 7, 8],  # Time 11
        [2, 4, 9],  # Time 12
        [4],  # Time 13
        [1, 3, 5, 6, 7, 8],  # Time 14
        [3],  # Time 15
        [2, 4, 9],  # Time 16
    ]

    assert encoded_db == expected_encoded_db


def test_extract_boundlists_from_indexDB():
    flat_data = [
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
    min_support = 2

    actual_output = phase2_encoding.extract_boundlists_from_indexDB(
        flat_data, min_support
    )

    # Expected output (based on item appearance in tids)
    expected = [
        (["A"], [1, 6], [(1, 1), (4, 4)]),
        (["B"], [4, 9], [(3, 3), (5, 5)]),
        (["C"], [2, 7], [(1, 1), (4, 4)]),
        (["D"], [5, 10], [(3, 3), (5, 5)]),
        (["F"], [3, 8], [(1, 1), (4, 4)]),
        (["A", "C"], [2, 7], [(1, 1), (4, 4)]),
        (["A", "C", "F"], [3, 8], [(1, 1), (4, 4)]),
        (["A", "F"], [3, 8], [(1, 1), (4, 4)]),
        (["B", "D"], [5, 10], [(3, 3), (5, 5)]),
    ]

    assert actual_output == expected
