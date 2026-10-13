from __future__ import annotations

from streamplat.reconcile import Layer, compare, expected_ids

LEDGER = {"run_id": "r1", "slots": 6, "poison": {"not_avro": [1], "truncated": [4]}}


def test_expected_ids_exclude_poison():
    assert expected_ids(LEDGER) == {"r1-000000000", "r1-000000002", "r1-000000003", "r1-000000005"}


def test_exact_layer():
    layer = compare(sorted(expected_ids(LEDGER)), expected_ids(LEDGER))
    assert layer == Layer(rows=4, unique=4, missing=0, unexpected=0) and layer.exact


def test_duplicate_loss_and_leak_are_each_caught():
    expected = expected_ids(LEDGER)
    dup = compare([*expected, "r1-000000000"], expected)
    assert (dup.rows, dup.unique, dup.exact) == (5, 4, False)
    lost = compare(sorted(expected)[1:], expected)
    assert (lost.missing, lost.exact) == (1, False)
    leaked = compare([*expected, "r1-000000001"], expected)
    assert (leaked.unexpected, leaked.exact) == (1, False)


def test_tally_matches_compare_on_sequence_numbers():
    import numpy as np

    from streamplat.reconcile import tally

    # seqs 1 and 4 are poison; 0 is duplicated, 3 is missing, 4 leaked, 9 is out of range
    seqs = np.array([0, 0, 2, 4, 5, 9], dtype=np.int64)
    layer = tally(seqs, LEDGER)
    assert layer == Layer(rows=6, unique=5, missing=1, unexpected=2)
    assert not layer.exact
    assert tally(np.array([0, 2, 3, 5], dtype=np.int64), LEDGER).exact
