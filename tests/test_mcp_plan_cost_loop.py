"""MCP plan-cost loop parity tests.

The MCP optimizer should avoid real candidate execution during iteration when
long-query mode has an EXPLAIN runner. Candidates are ranked by EXPLAIN cost and
only the verification phase calls `_measure_mcp`.
"""
from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

from genie.skills.mcp_trino import research as mcp_research
from genie.skills.mcp_trino.research import (
    ExplainAnalyzeResult,
    MeasureResult,
    RunMetrics,
    _run_mcp_plan_cost_loop,
)


def _make_client():
    client = MagicMock()
    client.config.url = "mock://trino"
    return client


def _make_result(rows=100, wall_ms=80_000, samples=None,
                 capture_status="complete", completeness="verified_complete"):
    metrics = RunMetrics(
        query_time_ms=wall_ms,
        cpu_time_ms=wall_ms,
        wall_time_ms=wall_ms,
        processed_rows=rows,
        total_splits=4,
    )
    result_rows = [{"a": i} for i in range(rows)]
    return MeasureResult(
        median_metric=float(wall_ms),
        samples=samples or [float(wall_ms)],
        row_count=rows,
        rows=result_rows,
        columns=["a"],
        metrics=metrics,
        # Default to complete capture provenance — the L3 correctness gate only
        # authorizes semantic comparison for explicitly proven complete
        # captures. Tests exercising the incomplete-rejection path override.
        capture_status=capture_status,
        completeness=completeness,
    )


def _make_explain(rows_est=1000, bytes_est=8000, table="hive.default.t"):
    return json.dumps({
        "name": f"TableScan[{table}]",
        "descriptor": {"table": table},
        "estimates": [{"outputRowCount": rows_est, "outputSizeInBytes": bytes_est}],
        "children": [],
    })


def _make_explain_bytes_only(bytes_est, table="hive.default.t"):
    """EXPLAIN JSON with outputSizeInBytes but NO outputRowCount (partial Trino stats)."""
    return json.dumps({
        "name": f"TableScan[{table}]",
        "descriptor": {"table": table},
        "estimates": [{"outputSizeInBytes": bytes_est}],
        "children": [],
    })


def _make_explain_rows_only(rows_est, table="hive.default.t"):
    """EXPLAIN JSON with outputRowCount but NO outputSizeInBytes."""
    return json.dumps({
        "name": f"TableScan[{table}]",
        "descriptor": {"table": table},
        "estimates": [{"outputRowCount": rows_est}],
        "children": [],
    })


def _make_explain_no_estimates(table="hive.default.t"):
    """EXPLAIN JSON with empty estimates list (no usable cost data)."""
    return json.dumps({
        "name": f"TableScan[{table}]",
        "descriptor": {"table": table},
        "estimates": [],
        "children": [],
    })


def _explain_runner_factory(plans_by_sql):
    def _runner(sql):
        return plans_by_sql.get(
            sql,
            _make_explain(rows_est=999, bytes_est=9999, table="hive.default.unknown"),
        )
    return _runner


def _llm_provider_with_replies(replies):
    provider = MagicMock()
    provider.complete_text.side_effect = list(replies)
    return provider


def _wrap_sql(sql):
    return f"Here is the rewrite:\n```sql\n{sql}\n```"


def _no_error_execute(*_args, **_kwargs):
    return {
        "rows": [],
        "columns": [],
        "row_count": 0,
        "metrics": RunMetrics(),
        "error": None,
        "raw": "",
    }


def test_mcp_dispatches_to_plan_cost_and_does_not_measure_iterations():
    output = MagicMock()
    client = _make_client()
    baseline = _make_result(rows=100, wall_ms=80_000)
    cand_a = "SELECT a FROM t WHERE a > 0"
    cand_b = "SELECT a FROM t WHERE a > 100"
    runner = _explain_runner_factory({
        "SELECT * FROM t": _make_explain(rows_est=100, bytes_est=10),
        cand_a: _make_explain(rows_est=20, bytes_est=10),
        cand_b: _make_explain(rows_est=10, bytes_est=10),
    })
    provider = _llm_provider_with_replies([_wrap_sql(cand_a), _wrap_sql(cand_b)])
    verified = _make_result(rows=100, wall_ms=20_000)

    with patch.object(mcp_research, "_build_mcp_explain_runner", return_value=runner), \
         patch.object(mcp_research, "_execute_via_mcp", side_effect=_no_error_execute), \
         patch.object(mcp_research, "_fetch_explain_analyze") as explain_analyze, \
         patch.object(mcp_research, "_measure_mcp", side_effect=[baseline, verified]) as measure:
        report = mcp_research.run_mcp_enhancement(
            client=client,
            sql="SELECT * FROM t",
            metric_key="wall_time_ms",
            max_iterations=2,
            verify_runs=1,
            provider=provider,
            model="m",
            reasoning="disable",
            output=output,
            build_prompt=lambda *a, **k: "SKILL",
            long_query_opt_in=True,
        )

    assert report.enhanced_sql == cand_b
    assert report.best_value == 20_000.0
    explain_analyze.assert_not_called()
    labels = [call.kwargs.get("label") for call in measure.call_args_list]
    assert labels == ["baseline", "verify iter 2"]
    assert all("candidate" not in str(label) for label in labels)


def test_mcp_plan_cost_loop_picks_lowest_cost_candidate():
    output = MagicMock()
    client = _make_client()
    baseline = _make_result(rows=100, wall_ms=80_000)
    cand_a = "SELECT a FROM t WHERE a > 0"
    cand_b = "SELECT a FROM t WHERE a > 100"
    runner = _explain_runner_factory({
        "SELECT * FROM t": _make_explain(rows_est=100, bytes_est=10),
        cand_a: _make_explain(rows_est=20, bytes_est=10),
        cand_b: _make_explain(rows_est=10, bytes_est=10),
    })
    provider = _llm_provider_with_replies([_wrap_sql(cand_a), _wrap_sql(cand_b)])

    with patch.object(mcp_research, "_measure_mcp", return_value=_make_result(rows=100, wall_ms=20_000)) as measure:
        report = _run_mcp_plan_cost_loop(
            client=client,
            provider=provider,
            model="m",
            reasoning="disable",
            original_sql="SELECT * FROM t",
            metric_key="wall_time_ms",
            max_iterations=2,
            verify_runs=1,
            output=output,
            build_prompt=lambda *a, **k: "SKILL",
            baseline=baseline,
            static_report=None,
            explain_runner=runner,
            max_fallbacks=3,
        )

    assert measure.call_args.args[1] == cand_b
    assert report.enhanced_sql == cand_b
    assert report.best_value == 20_000.0
    assert [r.status for r in report.iterations] == ["plan_cost_better", "improved"]


def test_mcp_plan_cost_loop_no_verifiable_keeps_original_sql():
    output = MagicMock()
    client = _make_client()
    baseline = _make_result(rows=100, wall_ms=80_000)
    cand = "SELECT a FROM t LIMIT 10000"
    runner = _explain_runner_factory({
        "SELECT * FROM t": _make_explain(rows_est=10, bytes_est=10),
        cand: _make_explain(rows_est=100, bytes_est=10),
    })
    provider = _llm_provider_with_replies([_wrap_sql(cand)])

    with patch.object(mcp_research, "_measure_mcp") as measure:
        report = _run_mcp_plan_cost_loop(
            client=client,
            provider=provider,
            model="m",
            reasoning="disable",
            original_sql="SELECT * FROM t",
            metric_key="wall_time_ms",
            max_iterations=1,
            verify_runs=1,
            output=output,
            build_prompt=lambda *a, **k: "SKILL",
            baseline=baseline,
            static_report=None,
            explain_runner=runner,
            max_fallbacks=3,
        )

    measure.assert_not_called()
    assert report.enhanced_sql == "SELECT * FROM t"
    assert report.best_value == baseline.median_metric
    assert report.data_consistent is True


def test_mcp_plan_cost_loop_l1_rejects_structural_divergence_without_verifying():
    output = MagicMock()
    client = _make_client()
    baseline = _make_result(rows=100, wall_ms=80_000)
    cand = "SELECT a FROM other_table"
    runner = _explain_runner_factory({
        "SELECT * FROM t": _make_explain(rows_est=100, bytes_est=10, table="hive.default.t"),
        cand: _make_explain(rows_est=10, bytes_est=10, table="hive.default.other_table"),
    })
    provider = _llm_provider_with_replies([_wrap_sql(cand)])

    with patch.object(mcp_research, "_measure_mcp") as measure:
        report = _run_mcp_plan_cost_loop(
            client=client,
            provider=provider,
            model="m",
            reasoning="disable",
            original_sql="SELECT * FROM t",
            metric_key="wall_time_ms",
            max_iterations=1,
            verify_runs=1,
            output=output,
            build_prompt=lambda *a, **k: "SKILL",
            baseline=baseline,
            static_report=None,
            explain_runner=runner,
            max_fallbacks=3,
        )

    measure.assert_not_called()
    assert report.enhanced_sql == "SELECT * FROM t"
    assert [r.status for r in report.iterations] == ["structural_reject"]


def test_mcp_plan_cost_loop_k_retry_falls_through_on_l3_failure():
    output = MagicMock()
    client = _make_client()
    baseline = _make_result(rows=100, wall_ms=80_000)
    cand_a = "SELECT a FROM t WHERE id > 100"
    cand_b = "SELECT a FROM t WHERE id > 50"
    runner = _explain_runner_factory({
        "SELECT * FROM t": _make_explain(rows_est=100, bytes_est=10),
        cand_a: _make_explain(rows_est=10, bytes_est=10),
        cand_b: _make_explain(rows_est=20, bytes_est=10),
    })
    provider = _llm_provider_with_replies([_wrap_sql(cand_a), _wrap_sql(cand_b)])
    bad = _make_result(rows=50, wall_ms=10_000)
    good = _make_result(rows=100, wall_ms=20_000)

    with patch.object(mcp_research, "_measure_mcp", side_effect=[bad, good]) as measure:
        report = _run_mcp_plan_cost_loop(
            client=client,
            provider=provider,
            model="m",
            reasoning="disable",
            original_sql="SELECT * FROM t",
            metric_key="wall_time_ms",
            max_iterations=2,
            verify_runs=1,
            output=output,
            build_prompt=lambda *a, **k: "SKILL",
            baseline=baseline,
            static_report=None,
            explain_runner=runner,
            max_fallbacks=3,
        )

    assert [call.args[1] for call in measure.call_args_list] == [cand_a, cand_b]
    assert report.enhanced_sql == cand_b
    assert [r.status for r in report.iterations] == ["plan_cost_better", "improved"]


def test_mcp_standard_loop_runs_when_long_query_opt_in_false():
    output = MagicMock()
    client = _make_client()
    baseline = _make_result(rows=100, wall_ms=10_000)
    candidate = _make_result(rows=100, wall_ms=5_000)
    cand_sql = "SELECT a FROM t WHERE a > 0"
    runner = _explain_runner_factory({
        "SELECT * FROM t": _make_explain(rows_est=100, bytes_est=10),
        cand_sql: _make_explain(rows_est=10, bytes_est=10),
    })
    provider = _llm_provider_with_replies([_wrap_sql(cand_sql)])
    explain = ExplainAnalyzeResult(raw_text="", available=False)

    with patch.object(mcp_research, "_build_mcp_explain_runner", return_value=runner), \
         patch.object(mcp_research, "_run_mcp_plan_cost_loop") as plan_loop, \
         patch.object(mcp_research, "_execute_via_mcp", side_effect=_no_error_execute), \
         patch.object(mcp_research, "_fetch_explain_analyze", return_value=explain), \
         patch.object(mcp_research, "_assemble_mcp_directions", return_value=([], [])), \
         patch.object(mcp_research, "_measure_mcp", side_effect=[baseline, candidate]) as measure:
        report = mcp_research.run_mcp_enhancement(
            client=client,
            sql="SELECT * FROM t",
            metric_key="wall_time_ms",
            max_iterations=1,
            verify_runs=1,
            provider=provider,
            model="m",
            reasoning="disable",
            output=output,
            build_prompt=lambda *a, **k: "SKILL",
            long_query_opt_in=False,
        )

    plan_loop.assert_not_called()
    labels = [call.kwargs.get("label") for call in measure.call_args_list]
    assert labels == ["baseline", "iter 1 candidate"]
    assert report.enhanced_sql == cand_sql


def test_mcp_standard_loop_does_not_remeasure_rejected_duplicate_candidate():
    """A rejected candidate is fed back and an exact retry consumes no Trino run."""
    output = MagicMock()
    client = _make_client()
    # Deliberately incomplete provenance: this test locks the
    # incomplete-result rejection path + duplicate-candidate dedup.
    baseline = _make_result(rows=100, wall_ms=10_000,
                            capture_status="not_captured",
                            completeness="not_captured")
    candidate = _make_result(rows=100, wall_ms=5_000,
                             capture_status="not_captured",
                             completeness="not_captured")
    cand_sql = "SELECT a FROM t WHERE a > 0"
    provider = _llm_provider_with_replies([_wrap_sql(cand_sql), _wrap_sql(cand_sql)])
    explain = ExplainAnalyzeResult(raw_text="", available=False)

    with patch.object(mcp_research, "_build_mcp_explain_runner", return_value=lambda _sql: None), \
         patch.object(mcp_research, "_run_mcp_plan_cost_loop") as plan_loop, \
         patch.object(mcp_research, "_execute_via_mcp", side_effect=_no_error_execute), \
         patch.object(mcp_research, "_fetch_explain_analyze", return_value=explain), \
         patch.object(mcp_research, "_assemble_mcp_directions", return_value=([], [])), \
         patch.object(mcp_research, "_measure_mcp", side_effect=[baseline, candidate]) as measure:
        report = mcp_research.run_mcp_enhancement(
            client=client, sql="SELECT * FROM t", metric_key="wall_time_ms",
            max_iterations=2, verify_runs=1, provider=provider, model="m",
            reasoning="disable", output=output,
            build_prompt=lambda *a, **k: "SKILL", long_query_opt_in=False,
        )

    plan_loop.assert_not_called()
    assert [call.kwargs.get("label") for call in measure.call_args_list] == [
        "baseline", "iter 1 candidate",
    ]
    assert [it.status for it in report.iterations] == [
        "equivalence_unverified_incomplete_result", "duplicate_candidate",
    ]
    second_request = provider.complete_text.call_args_list[1].args[0]
    prompt_text = str(second_request.messages)
    assert "Previously attempted changes" in prompt_text
    rejection_reason = report.iterations[0].rejection_reason
    assert rejection_reason is not None
    assert rejection_reason in prompt_text


def test_mcp_falls_back_to_standard_loop_when_explain_has_no_estimates():
    # A cluster without table statistics returns a plan but NO row/byte estimates.
    # Plan-cost ranking is impossible there (every candidate would skip), so the
    # optimizer must fall back to the standard measure loop, not silently do
    # nothing. Regression test for the real-cluster no-stats case.
    output = MagicMock()
    client = _make_client()
    baseline = _make_result(rows=100, wall_ms=10_000)
    candidate = _make_result(rows=100, wall_ms=5_000)
    cand_sql = "SELECT a FROM t WHERE a > 0"
    no_estimates_plan = json.dumps({
        "name": "TableScan[hive.default.t]",
        "descriptor": {"table": "hive.default.t"},
        "estimates": [],  # plan present, but NO cost estimates
        "children": [],
    })

    def _runner(_sql):
        return no_estimates_plan

    provider = _llm_provider_with_replies([_wrap_sql(cand_sql)])
    explain = ExplainAnalyzeResult(raw_text="", available=False)

    with patch.object(mcp_research, "_build_mcp_explain_runner", return_value=_runner), \
         patch.object(mcp_research, "_run_mcp_plan_cost_loop") as plan_loop, \
         patch.object(mcp_research, "_execute_via_mcp", side_effect=_no_error_execute), \
         patch.object(mcp_research, "_fetch_explain_analyze", return_value=explain), \
         patch.object(mcp_research, "_assemble_mcp_directions", return_value=([], [])), \
         patch.object(mcp_research, "_measure_mcp", side_effect=[baseline, candidate]) as measure:
        report = mcp_research.run_mcp_enhancement(
            client=client,
            sql="SELECT * FROM t",
            metric_key="wall_time_ms",
            max_iterations=1,
            verify_runs=1,
            provider=provider,
            model="m",
            reasoning="disable",
            output=output,
            build_prompt=lambda *a, **k: "SKILL",
            long_query_opt_in=True,  # opt-in TRUE but no estimates → must NOT use plan-cost
        )

    plan_loop.assert_not_called()
    labels = [call.kwargs.get("label") for call in measure.call_args_list]
    assert labels == ["baseline", "iter 1 candidate"]
    assert report.enhanced_sql == cand_sql
    printed = " ".join(
        str(c.args[0]) for c in output.progress.call_args_list if c.args
    )
    assert "Plan-cost mode unavailable" in printed


# ── Bugfix: bytes-only / rows-only / None-baseline cost ranking (MCP parity) ─

def test_mcp_bytes_only_candidate_with_fewer_bytes_wins():
    """MCP path: bytes-only candidate (rows=None) with bytes < baseline_cost wins correctly.

    With the bug: cand_cost = 0 → always falsely ranked first.
    After fix:   cand_cost = 5_000_000 < 8_000_000_000_000 → correct win.
    """
    output = MagicMock()
    client = _make_client()
    baseline = _make_result(rows=100, wall_ms=80_000)
    cand = "SELECT a FROM t WHERE a > 0"
    runner = _explain_runner_factory({
        "SELECT * FROM t": _make_explain(rows_est=1_000_000, bytes_est=8_000_000),
        cand: _make_explain_bytes_only(bytes_est=5_000_000),  # rows=None, bytes=5M
    })
    provider = _llm_provider_with_replies([_wrap_sql(cand)])
    verified = _make_result(rows=100, wall_ms=20_000)

    with patch.object(mcp_research, "_measure_mcp", return_value=verified):
        report = _run_mcp_plan_cost_loop(
            client=client,
            provider=provider,
            model="m",
            reasoning="disable",
            original_sql="SELECT * FROM t",
            metric_key="wall_time_ms",
            max_iterations=1,
            verify_runs=1,
            output=output,
            build_prompt=lambda *a, **k: "SKILL",
            baseline=baseline,
            static_report=None,
            explain_runner=runner,
            max_fallbacks=3,
        )

    assert report.enhanced_sql == cand
    assert report.best_value == 20_000.0


def test_mcp_bytes_only_candidate_with_more_bytes_does_not_win():
    """MCP path: bytes-only candidate with bytes > baseline_cost must not win.

    The old bug produced cand_cost=0 → always beat any positive baseline.
    After fix: cand_cost = 90_000 > 80_000 → correctly rejected, no verification.
    """
    output = MagicMock()
    client = _make_client()
    baseline = _make_result(rows=100, wall_ms=80_000)
    cand = "SELECT a FROM t WHERE a > 0"
    runner = _explain_runner_factory({
        "SELECT * FROM t": _make_explain(rows_est=100, bytes_est=800),  # cost = 80_000
        cand: _make_explain_bytes_only(bytes_est=90_000),                # cand_cost = 90_000 > 80_000
    })
    provider = _llm_provider_with_replies([_wrap_sql(cand)])

    with patch.object(mcp_research, "_measure_mcp") as m:
        report = _run_mcp_plan_cost_loop(
            client=client,
            provider=provider,
            model="m",
            reasoning="disable",
            original_sql="SELECT * FROM t",
            metric_key="wall_time_ms",
            max_iterations=1,
            verify_runs=1,
            output=output,
            build_prompt=lambda *a, **k: "SKILL",
            baseline=baseline,
            static_report=None,
            explain_runner=runner,
            max_fallbacks=3,
        )
        m.assert_not_called()

    assert report.enhanced_sql == "SELECT * FROM t"


def test_mcp_rows_only_candidate_handled():
    """MCP path: rows-only candidate (bytes=None) ranks without sentinel distortion."""
    output = MagicMock()
    client = _make_client()
    baseline = _make_result(rows=100, wall_ms=80_000)
    cand = "SELECT a FROM t WHERE a > 0"
    runner = _explain_runner_factory({
        "SELECT * FROM t": _make_explain(rows_est=1_000, bytes_est=8_000),  # cost = 8_000_000
        cand: _make_explain_rows_only(rows_est=900),                         # cand_cost = 900
    })
    provider = _llm_provider_with_replies([_wrap_sql(cand)])
    verified = _make_result(rows=100, wall_ms=20_000)

    with patch.object(mcp_research, "_measure_mcp", return_value=verified):
        report = _run_mcp_plan_cost_loop(
            client=client,
            provider=provider,
            model="m",
            reasoning="disable",
            original_sql="SELECT * FROM t",
            metric_key="wall_time_ms",
            max_iterations=1,
            verify_runs=1,
            output=output,
            build_prompt=lambda *a, **k: "SKILL",
            baseline=baseline,
            static_report=None,
            explain_runner=runner,
            max_fallbacks=3,
        )

    assert report.enhanced_sql == cand


def test_mcp_both_missing_candidate_skipped_no_crash():
    """MCP path: candidate with no estimates is skipped without crash."""
    output = MagicMock()
    client = _make_client()
    baseline = _make_result(rows=100, wall_ms=80_000)
    cand = "SELECT a FROM t"
    runner = _explain_runner_factory({
        "SELECT * FROM t": _make_explain(rows_est=100, bytes_est=10),
        cand: _make_explain_no_estimates(),
    })
    provider = _llm_provider_with_replies([_wrap_sql(cand)])

    with patch.object(mcp_research, "_measure_mcp") as m:
        report = _run_mcp_plan_cost_loop(
            client=client,
            provider=provider,
            model="m",
            reasoning="disable",
            original_sql="SELECT * FROM t",
            metric_key="wall_time_ms",
            max_iterations=1,
            verify_runs=1,
            output=output,
            build_prompt=lambda *a, **k: "SKILL",
            baseline=baseline,
            static_report=None,
            explain_runner=runner,
            max_fallbacks=3,
        )
        m.assert_not_called()

    assert report.enhanced_sql == "SELECT * FROM t"


def test_mcp_baseline_none_cost_does_not_crash():
    """MCP path: baseline with both estimates=None must not raise TypeError.

    OI-01 fix: baseline_cost=None → each candidate skipped (not falsely promoted).
    """
    output = MagicMock()
    client = _make_client()
    baseline = _make_result(rows=100, wall_ms=80_000)
    cand = "SELECT a FROM t WHERE a > 0"
    runner = _explain_runner_factory({
        "SELECT * FROM t": _make_explain_no_estimates(),  # baseline → (None, None)
        cand: _make_explain(rows_est=50, bytes_est=5),    # candidate has cost
    })
    provider = _llm_provider_with_replies([_wrap_sql(cand)])

    with patch.object(mcp_research, "_measure_mcp") as m:
        # Must not raise TypeError
        report = _run_mcp_plan_cost_loop(
            client=client,
            provider=provider,
            model="m",
            reasoning="disable",
            original_sql="SELECT * FROM t",
            metric_key="wall_time_ms",
            max_iterations=1,
            verify_runs=1,
            output=output,
            build_prompt=lambda *a, **k: "SKILL",
            baseline=baseline,
            static_report=None,
            explain_runner=runner,
            max_fallbacks=3,
        )
        m.assert_not_called()

    assert report.enhanced_sql == "SELECT * FROM t"
    statuses = [r.status for r in report.iterations]
    assert "plan_cost_better" not in statuses
