"""Order-insensitive result equivalence for queries without a top-level ORDER BY.

Real-cluster regression: GROUP BY without ORDER BY returns rows in
nondeterministic order, so a positional compare misreported equivalent
candidates as semantic drift (observed live on Trino 483 / tpch.sf1).
"""
from __future__ import annotations

from genie.skills.trino_query.research import (
    _has_top_level_order_by,
    _results_equivalent,
)

_A = [("1-URGENT", 654989, 25047540935.07), ("3-MEDIUM", 653474, 24996587082.53)]
_B = list(reversed(_A))


def test_order_by_detection():
    assert _has_top_level_order_by("SELECT a FROM t ORDER BY a")
    assert not _has_top_level_order_by("SELECT a, count(*) FROM t GROUP BY a")
    # Subquery ORDER BY does not order the outer result.
    assert not _has_top_level_order_by(
        "SELECT * FROM (SELECT a FROM t ORDER BY a LIMIT 5)")
    # UNION with trailing ORDER BY orders the whole result.
    assert _has_top_level_order_by(
        "SELECT a FROM t UNION ALL SELECT a FROM u ORDER BY a")
    # Fail-closed: unparseable SQL → strict positional compare.
    assert _has_top_level_order_by("NOT SQL ((((")


def test_unordered_same_multiset_is_equivalent():
    equiv, reason = _results_equivalent(_A, _B, ordered=False)
    assert equiv, reason


def test_ordered_positional_still_strict():
    equiv, _ = _results_equivalent(_A, _B, ordered=True)
    assert not equiv


def test_unordered_detects_true_value_drift():
    changed = [_A[0], ("3-MEDIUM", 653474, 0.0)]
    equiv, reason = _results_equivalent(_A, changed, ordered=False)
    assert not equiv
    assert "order-insensitive" in reason


def test_unordered_detects_duplicate_count_drift():
    # Same distinct values but different multiplicities must NOT be equivalent.
    a = [(1,), (1,), (2,)]
    b = [(1,), (2,), (2,)]
    equiv, _ = _results_equivalent(a, b, ordered=False)
    assert not equiv


def test_unordered_handles_unhashable_cells():
    a = [([1, 2], "x"), ([3], "y")]
    b = [([3], "y"), ([1, 2], "x")]
    equiv, _ = _results_equivalent(a, b, ordered=False)
    assert equiv


def test_default_remains_positional():
    equiv, _ = _results_equivalent(_A, _B)
    assert not equiv          # unchanged default for callers that never opt in


def test_unordered_equates_eq_equal_values_like_positional():
    # The multiset compare must never reject rows the positional branch would
    # accept: values equal under == count as the same row even when their
    # reprs differ (decimal scale, int vs float, signed zero).
    from decimal import Decimal
    a = [(Decimal("1.5"), 1, 0.0)]
    b = [(Decimal("1.50"), 1.0, -0.0)]
    assert _results_equivalent(a, b, ordered=True)[0]
    equiv, reason = _results_equivalent(a, b, ordered=False)
    assert equiv, reason


def test_unordered_still_distinguishes_number_from_string():
    equiv, _ = _results_equivalent([(1,)], [("1",)], ordered=False)
    assert not equiv


def test_small_magnitude_float_noise_tolerated():
    # Run-to-run parallel-aggregation noise on small doubles (rates, ratios):
    # the historical round(val, 6) absolute tolerance must survive the
    # significant-digit normalization.
    a = [(0.001234567891,)]
    b = [(0.001234567892,)]
    assert _results_equivalent(a, b, ordered=True)[0]
    assert _results_equivalent(a, b, ordered=False)[0]


def test_large_magnitude_float_noise_tolerated():
    # 1e10-scale sums differ in their last ulps between equivalent runs.
    a = [(24996587082.530001,)]
    b = [(24996587082.529999,)]
    assert _results_equivalent(a, b, ordered=True)[0]
    assert _results_equivalent(a, b, ordered=False)[0]
