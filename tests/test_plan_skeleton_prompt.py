"""Plan-skeleton + EXPLAIN ANALYZE hotspot prompt injection.

Locks the wiring that feeds condensed plan evidence to the optimizer LLM:
- `_format_hotspot_stages` renders top-CPU baseline stages (runtime ground truth);
- `_render_mcp_plan_skeleton` / `_render_direct_plan_skeleton` are fail-open;
- the MCP standard loop and plan-cost loop system prompts carry the skeleton
  (and hotspots, where EXPLAIN ANALYZE data exists).
"""
from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock, patch

from genie.skills.mcp_trino import research as mcp_research
from genie.skills.mcp_trino.research import (
    ExplainAnalyzeResult,
    _format_hotspot_stages,
    _format_plan_skeleton_block,
    _run_mcp_plan_cost_loop,
)
from genie.skills.trino_query.research import _render_direct_plan_skeleton
from tests.test_mcp_plan_cost_loop import (
    _explain_runner_factory,
    _llm_provider_with_replies,
    _make_client,
    _make_explain,
    _make_result,
    _no_error_execute,
    _wrap_sql,
)

FIXTURES = Path(__file__).parent / "fixtures" / "explain_plans"
JOIN_PLAN_TEXT = (FIXTURES / "with_join_partitioned.json").read_text()


def _sys_content(provider) -> str:
    request = provider.complete_text.call_args_list[0].args[0]
    parts = []
    for m in request.messages:
        if m["role"] != "system":
            continue
        content = m["content"]
        if isinstance(content, str):
            parts.append(content)
        else:  # new_msg content-block shape: [{"type": "text", "text": ...}]
            parts.extend(b.get("text") or "" for b in content if isinstance(b, dict))
    return "\n".join(parts)


# ---------------------------------------------------------------------------
# _format_hotspot_stages — pure function
# ---------------------------------------------------------------------------

class TestFormatHotspotStages:
    def _explain(self, stages, total_cpu):
        return ExplainAnalyzeResult(
            raw_text="raw", stages=stages, total_cpu_ms=total_cpu, available=True,
        )

    def test_ranks_stages_by_cpu_and_reports_share(self):
        explain = self._explain(
            [
                {"id": 0, "cpu_ms": 100.0, "wall_ms": 50.0,
                 "input_rows": 1000, "output_rows": 10},
                {"id": 1, "cpu_ms": 900.0, "wall_ms": 400.0,
                 "input_rows": 5_000_000, "output_rows": 1000,
                 "memory_bytes": 2 * 1024 * 1024},
            ],
            total_cpu=1000.0,
        )
        block = _format_hotspot_stages(explain)
        assert "hotspots" in block
        assert block.index("Stage 1") < block.index("Stage 0")
        assert "cpu=900ms (90% of total)" in block
        assert "input=5,000,000 rows" in block
        assert "peak_mem=2.0MB" in block

    def test_limit_caps_stage_count(self):
        stages = [{"id": i, "cpu_ms": float(100 - i)} for i in range(6)]
        block = _format_hotspot_stages(self._explain(stages, 500.0), limit=3)
        assert "Stage 0" in block and "Stage 2" in block
        assert "Stage 3" not in block

    def test_unavailable_or_empty_returns_empty(self):
        assert _format_hotspot_stages(None) == ""
        assert _format_hotspot_stages(
            ExplainAnalyzeResult(raw_text="x", available=False)
        ) == ""
        assert _format_hotspot_stages(self._explain([], 0.0)) == ""

    def test_zero_cpu_stages_return_empty(self):
        stages = [{"id": 0, "cpu_ms": 0.0}, {"id": 1}]
        assert _format_hotspot_stages(self._explain(stages, 0.0)) == ""


# ---------------------------------------------------------------------------
# Skeleton fetch helpers — fail-open contract
# ---------------------------------------------------------------------------

class TestSkeletonHelpers:
    def test_block_wraps_nonempty_and_passes_empty_through(self):
        assert _format_plan_skeleton_block("", label="X") == ""
        block = _format_plan_skeleton_block("TableScan[a.b.c]", label="Baseline plan skeleton")
        assert block.startswith("Baseline plan skeleton")
        assert "```text\nTableScan[a.b.c]\n```" in block

    def test_direct_skeleton_from_runner(self):
        skeleton = _render_direct_plan_skeleton(lambda _sql: JOIN_PLAN_TEXT, "SELECT 1")
        assert "Join[INNER, PARTITIONED]" in skeleton
        assert "TableScan[hive.default.a]" in skeleton

    def test_direct_skeleton_fail_open(self):
        assert _render_direct_plan_skeleton(None, "SELECT 1") == ""
        assert _render_direct_plan_skeleton(lambda _sql: None, "SELECT 1") == ""

        def _boom(_sql):
            raise RuntimeError("cluster down")
        assert _render_direct_plan_skeleton(_boom, "SELECT 1") == ""

    def test_mcp_skeleton_uses_explain_runner(self):
        with patch.object(
            mcp_research, "_build_mcp_explain_runner",
            return_value=lambda _sql: JOIN_PLAN_TEXT,
        ):
            skeleton = mcp_research._render_mcp_plan_skeleton(_make_client(), "SELECT 1")
        assert "Join[INNER, PARTITIONED]" in skeleton

    def test_mcp_skeleton_fail_open(self):
        with patch.object(
            mcp_research, "_build_mcp_explain_runner",
            return_value=lambda _sql: None,
        ):
            assert mcp_research._render_mcp_plan_skeleton(_make_client(), "SELECT 1") == ""


# ---------------------------------------------------------------------------
# System-prompt wiring — MCP standard loop
# ---------------------------------------------------------------------------

class TestStandardLoopPromptWiring:
    def _run(self, *, explain_analyze):
        output = MagicMock()
        client = _make_client()
        baseline = _make_result(rows=100, wall_ms=10_000)
        candidate = _make_result(rows=100, wall_ms=5_000)
        cand_sql = "SELECT a FROM t WHERE a > 0"
        runner = _explain_runner_factory({"SELECT * FROM t": JOIN_PLAN_TEXT})
        provider = _llm_provider_with_replies([_wrap_sql(cand_sql)])

        with patch.object(mcp_research, "_build_mcp_explain_runner", return_value=runner), \
             patch.object(mcp_research, "_run_mcp_plan_cost_loop"), \
             patch.object(mcp_research, "_execute_via_mcp", side_effect=_no_error_execute), \
             patch.object(mcp_research, "_fetch_explain_analyze", return_value=explain_analyze), \
             patch.object(mcp_research, "_assemble_mcp_directions", return_value=([], [])), \
             patch.object(mcp_research, "_measure_mcp", side_effect=[baseline, candidate]):
            mcp_research.run_mcp_enhancement(
                client=client, sql="SELECT * FROM t", metric_key="wall_time_ms",
                max_iterations=1, verify_runs=1, provider=provider, model="m",
                reasoning="disable", output=output,
                build_prompt=lambda *a, **k: "SKILL", long_query_opt_in=False,
            )
        return provider

    def test_sys_prompt_carries_baseline_skeleton(self):
        provider = self._run(
            explain_analyze=ExplainAnalyzeResult(raw_text="", available=False),
        )
        sys_content = _sys_content(provider)
        assert "Baseline plan skeleton" in sys_content
        assert "Join[INNER, PARTITIONED]" in sys_content
        assert "TableScan[hive.default.a]" in sys_content

    def test_sys_prompt_carries_hotspots_when_explain_analyze_available(self):
        explain = ExplainAnalyzeResult(
            raw_text="raw",
            stages=[
                {"id": 0, "cpu_ms": 900.0, "input_rows": 1_000_000},
                {"id": 1, "cpu_ms": 100.0},
            ],
            total_cpu_ms=1000.0,
            available=True,
        )
        sys_content = _sys_content(self._run(explain_analyze=explain))
        assert "EXPLAIN ANALYZE hotspots" in sys_content
        assert "Stage 0: cpu=900ms (90% of total)" in sys_content

    def test_hotspots_absent_when_explain_analyze_unavailable(self):
        provider = self._run(
            explain_analyze=ExplainAnalyzeResult(raw_text="", available=False),
        )
        assert "hotspots" not in _sys_content(provider)


# ---------------------------------------------------------------------------
# System-prompt wiring — MCP plan-cost (long-query) loop
# ---------------------------------------------------------------------------

class TestPlanCostLoopPromptWiring:
    def test_sys_prompt_carries_baseline_skeleton(self):
        output = MagicMock()
        client = _make_client()
        baseline = _make_result(rows=100, wall_ms=80_000)
        cand_sql = "SELECT a FROM t WHERE a > 0"
        # Candidate plan structurally identical to baseline → structural
        # reject → loop finishes without any candidate measurement.
        runner = _explain_runner_factory({
            "SELECT * FROM t": _make_explain(rows_est=100, bytes_est=10),
            cand_sql: _make_explain(rows_est=100, bytes_est=10),
        })
        provider = _llm_provider_with_replies([_wrap_sql(cand_sql)])

        with patch.object(mcp_research, "_measure_mcp") as measure:
            _run_mcp_plan_cost_loop(
                client=client, provider=provider, model="m", reasoning="disable",
                original_sql="SELECT * FROM t", metric_key="wall_time_ms",
                max_iterations=1, verify_runs=1, output=output,
                build_prompt=lambda *a, **k: "SKILL", baseline=baseline,
                static_report=None, explain_runner=runner, max_fallbacks=3,
            )

        measure.assert_not_called()
        sys_content = _sys_content(provider)
        assert "Baseline plan skeleton" in sys_content
        assert "TableScan[hive.default.t]" in sys_content
