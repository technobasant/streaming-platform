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
