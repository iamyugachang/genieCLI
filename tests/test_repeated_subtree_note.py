"""plan_render.repeated_subtree_note — inlined-CTE (duplicate subtree) detection."""
from __future__ import annotations

import json

from genie.skills.trino_query.plan_render import repeated_subtree_note


def _scan_agg_subtree(table: str) -> dict:
    """A 3-node subtree (Aggregate → Filter → TableScan) — big enough to tally."""
    return {
        "name": "Aggregate",
        "descriptor": {"functions": "count(*)"},
        "children": [{
            "name": "Filter",
            "descriptor": {"filterPredicate": "d >= DATE '2026-08-01'"},
            "children": [{
                "name": "TableScan",
                "descriptor": {"table": f"hive.raw.{table}"},
                "children": [],
            }],
        }],
    }


def test_duplicate_subtree_detected():
    plan = {
        "name": "Output",
        "children": [{
            "name": "Join",
            "descriptor": {"type": "INNER"},
            "children": [_scan_agg_subtree("events"), _scan_agg_subtree("events")],
        }],
    }
    note = repeated_subtree_note(plan)
    assert note != ""
    assert "×2" in note
    assert "inlined CTE" in note


def test_unique_subtrees_yield_no_note():
    plan = {
        "name": "Output",
        "children": [{
            "name": "Join",
            "descriptor": {"type": "INNER"},
            "children": [_scan_agg_subtree("events"), _scan_agg_subtree("users")],
        }],
    }
    assert repeated_subtree_note(plan) == ""


def test_string_input_and_garbage_degrade_cleanly():
    plan = {
        "name": "Join",
        "children": [_scan_agg_subtree("t"), _scan_agg_subtree("t")],
    }
    assert repeated_subtree_note(json.dumps(plan)) != ""
    assert repeated_subtree_note("not json {{") == ""
    assert repeated_subtree_note(None) == ""
    assert repeated_subtree_note(12345) == ""


def test_small_subtrees_ignored():
    # Two identical single-node scans — below min_nodes, not worth a note.
    plan = {
        "name": "Join",
        "children": [
            {"name": "TableScan", "descriptor": {"table": "hive.a.t"}, "children": []},
            {"name": "TableScan", "descriptor": {"table": "hive.a.t"}, "children": []},
        ],
    }
    assert repeated_subtree_note(plan) == ""
