"""Tests for genie.skills.trino_query.plan_render — bounded plan skeletons.

The renderer is prompt-facing: these tests lock (a) signal fields surviving
the rendering (op, join type/distribution, criteria, tables, estimates,
aggregation phase), (b) the "" contract for unusable input, (c) the
max_lines budget with fold-then-truncate degradation, (d) determinism.
"""
import json
from pathlib import Path

import pytest

from genie.skills.trino_query.plan_render import (
    DEFAULT_MAX_LINES,
    render_plan_skeleton,
)

FIXTURES = Path(__file__).parent / "fixtures" / "explain_plans"


def _load(name: str) -> dict:
    return json.loads((FIXTURES / name).read_text())


# ---------------------------------------------------------------------------
# Signal preservation
# ---------------------------------------------------------------------------

class TestSignalPreservation:
    def test_join_plan_keeps_type_distribution_criteria_tables(self):
        out = render_plan_skeleton(_load("with_join_partitioned.json"))
        assert "Join[INNER, PARTITIONED]" in out
        assert "on (a.id = b.id)" in out
        assert "TableScan[hive.default.a]" in out
        assert "TableScan[hive.default.b]" in out

    def test_join_plan_keeps_estimates_humanized(self):
        out = render_plan_skeleton(_load("with_join_partitioned.json"))
        # 5_000_000 rows / 524_288_000 bytes on the probe-side scan
        assert "~5.0M rows, 500.0MB" in out
        assert "~10.0K rows" in out

    def test_aggregate_plan_keeps_phase_functions_and_exchange(self):
        out = render_plan_skeleton(_load("with_aggregate.json"))
        assert "Aggregate[FINAL]" in out
        assert "Aggregate[PARTIAL]" in out
        assert "funcs=count(*)" in out
        assert "RemoteExchange" in out
        assert "TableScan[hive.default.events]" in out

    def test_indentation_follows_tree_depth(self):
        out = render_plan_skeleton(_load("with_aggregate.json"))
        lines = out.split("\n")
        scan = next(l for l in lines if "TableScan" in l)
        root = next(l for l in lines if l.startswith("Output"))
        assert len(scan) - len(scan.lstrip()) > 0
        assert len(root) - len(root.lstrip()) == 0

    def test_str_input_equals_dict_input(self):
        raw = (FIXTURES / "with_join_partitioned.json").read_text()
        assert render_plan_skeleton(raw) == render_plan_skeleton(json.loads(raw))

    def test_volatile_descriptor_noise_is_dropped(self):
        plan = {
            "name": "Project[expr_42]",
            "descriptor": {"columnNames": "[expr_42]"},
            "children": [],
        }
        out = render_plan_skeleton(plan)
        assert out == "Project"

    def test_real_trino467_plan_renders_full_signal_set(self):
        """Captured from live Trino 467 (EXPLAIN (FORMAT JSON) on an Iceberg
        join+group-by). Locks the live-output spellings: typed join names
        (InnerJoin), connector-prefixed table ids (iceberg:schema.t$data@snap),
        aggregate phase in the descriptor, fragment-map top level."""
        out = render_plan_skeleton((FIXTURES / "real_trino467_join_group.json").read_text())
        assert "Fragment 0" in out and "Fragment 4" in out
        assert "InnerJoin[REPLICATED] on (id = id_0)" in out
        assert "ScanFilter[iceberg.test_schema.t_g0s_001]" in out
        assert "pred=(id > bigint '100')" in out
        assert "Aggregate[FINAL] keys=[run_id]" in out
        assert "Aggregate[PARTIAL] keys=[run_id]" in out
        assert "RemoteSource from=[4]" in out
        # volatile fields dropped
        assert "$data@" not in out
        assert "dynamicFilters" not in out and "#df_" not in out

    def test_fragment_map_shape_renders_fragment_headers(self):
        plan = {
            "0": _load("simple_select.json"),
            "1": _load("with_aggregate.json"),
        }
        out = render_plan_skeleton(plan)
        assert "Fragment 0" in out
        assert "Fragment 1" in out

    def test_fragment_map_orders_numerically_not_lexicographically(self):
        # String sort renders Fragment 10 between Fragment 1 and Fragment 2,
        # misrepresenting the plan topology in the prompt.
        base = _load("simple_select.json")
        out = render_plan_skeleton({"10": base, "2": base, "1": base})
        assert out.index("Fragment 2") < out.index("Fragment 10")


# ---------------------------------------------------------------------------
# Unusable-input contract: "" (matches format_directions_for_prompt gating)
# ---------------------------------------------------------------------------

class TestUnusableInput:
    @pytest.mark.parametrize("bad", [
        None, "", "not json {", 42, 3.14, True,
        [], [None], {"estimates": "garbage-no-node-keys-no-dict-values"},
    ])
    def test_returns_empty_string(self, bad):
        assert render_plan_skeleton(bad) == ""

    def test_never_raises_on_hostile_nodes(self):
        plan = {
            "name": 123,                       # non-str name
            "descriptor": ["not", "a", "dict"],
            "estimates": [{"outputRowCount": float("nan"),
                           "outputSizeInBytes": True}],
            "children": [None, "junk", {"name": "TableScan[hive.s.t]",
                                        "children": {"name": "Limit"}}],
        }
        out = render_plan_skeleton(plan)
        assert "TableScan[hive.s.t]" in out
        assert "nan" not in out.lower()


# ---------------------------------------------------------------------------
# Budget: fold low-signal nodes first, then hard-truncate with a note
# ---------------------------------------------------------------------------

def _project_chain(leaf: dict, n: int) -> dict:
    node = leaf
    for i in range(n):
        node = {"name": f"Project[p{i}]", "descriptor": {}, "children": [node]}
    return node


class TestBudget:
    def test_within_budget_renders_all_nodes(self):
        plan = _project_chain({"name": "TableScan[hive.d.t]", "children": []}, 5)
        out = render_plan_skeleton(plan, max_lines=20)
        assert out.count("Project") == 5
        assert "folded" not in out

    def test_over_budget_folds_low_signal_nodes(self):
        leaf = {"name": "TableScan[hive.d.t]",
                "descriptor": {"table": "hive.d.t"}, "children": []}
        plan = {
            "name": "Join[INNER]",
            "descriptor": {"type": "INNER", "criteria": "(a.id = b.id)"},
            "children": [_project_chain(leaf, 20), _project_chain(leaf, 20)],
        }
        out = render_plan_skeleton(plan, max_lines=10)
        lines = out.split("\n")
        assert len(lines) <= 10
        assert "Project" not in out
        assert "Join[INNER]" in out
        assert out.count("TableScan[hive.d.t]") == 2
        assert "40 low-signal node(s) folded" in out

    def test_compact_mode_reindents_by_kept_ancestors(self):
        leaf = {"name": "TableScan[hive.d.t]", "children": []}
        plan = {
            "name": "Join[INNER]",
            "descriptor": {"type": "INNER"},
            "children": [_project_chain(leaf, 20), _project_chain(leaf, 20)],
        }
        out = render_plan_skeleton(plan, max_lines=10)
        scans = [l for l in out.split("\n") if "TableScan" in l]
        # Scans sit directly under the join after folding → one indent level.
        assert all(l.startswith("  TableScan") for l in scans)

    def test_still_over_budget_truncates_with_note(self):
        plan = {
            "name": "Union",
            "children": [
                {"name": f"TableScan[hive.d.t{i}]", "children": []}
                for i in range(30)
            ],
        }
        out = render_plan_skeleton(plan, max_lines=10)
        lines = out.split("\n")
        assert len(lines) == 10
        assert "omitted" in lines[-1]

    def test_deep_recursion_is_capped_not_crashed(self):
        plan = _project_chain({"name": "TableScan[hive.d.t]", "children": []}, 200)
        out = render_plan_skeleton(plan, max_lines=500)
        assert out  # renders the top of the tree without RecursionError


# ---------------------------------------------------------------------------
# Determinism
# ---------------------------------------------------------------------------

class TestDeterminism:
    @pytest.mark.parametrize("fixture", [
        "simple_select.json", "with_join_partitioned.json", "with_aggregate.json",
        "with_join_replicated_large_build.json", "with_join_zero_estimates.json",
    ])
    def test_same_input_same_output(self, fixture):
        plan = _load(fixture)
        assert render_plan_skeleton(plan) == render_plan_skeleton(plan)

    def test_default_budget_is_bounded(self):
        plan = {
            "name": "Union",
            "children": [
                {"name": f"TableScan[hive.d.t{i}]", "children": []}
                for i in range(300)
            ],
        }
        out = render_plan_skeleton(plan)
        assert len(out.split("\n")) <= DEFAULT_MAX_LINES
