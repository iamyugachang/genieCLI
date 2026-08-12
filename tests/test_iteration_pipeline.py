"""iteration_pipeline — Step A/B/C prompt composition, degradation, recording.

Also locks the loop-level contract: GENIE_STEPWISE_ITERATION=0 restores the
legacy single-call-per-iteration behavior exactly.
"""
from __future__ import annotations

from unittest.mock import MagicMock, patch

from genie.skills.trino_query.iteration_pipeline import (
    IterationEvidence,
    StepwiseResult,
    compose_rewrite_message,
    parse_hypothesis,
    record_fields,
    run_diagnose_step,
    run_stepwise_prelude,
    stepwise_enabled,
)


def _evidence(**overrides) -> IterationEvidence:
    base = dict(
        metric_key="cpu_time_ms",
        baseline_metric=1000.0,
        best_metric=800.0,
        last_result_line="improved (metric=800.0, delta=-200.0)",
        iteration=2,
        max_iterations=5,
        best_sql="SELECT a FROM t",
        hotspot_block="[Runtime stage hotspots]\nS1 cpu 60%",
        landscape_block="[Table landscape]\nhive.raw.t — 1.2B rows",
        skeleton_block="[Current plan skeleton]\nTableScan[hive.raw.t]",
        dup_subtree_note="",
        static_block="",
        directions_block="Pre-execution diagnosis:\n1. fix-select-star",
    )
    base.update(overrides)
    return IterationEvidence(**base)


def _flatten(content) -> str:
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        return " ".join(
            (p.get("text") or "") if isinstance(p, dict) else str(p) for p in content
        )
    return str(content)


class _RecordingProvider:
    """Snapshots each call's flattened user text AT CALL TIME — the loop keeps
    mutating the live session history list after the request is issued."""

    def __init__(self, replies=None):
        self.user_texts: list[str] = []
        self._replies = list(replies or [])

    def complete_text(self, req):
        users = [m for m in req.messages if m.get("role") == "user"]
        self.user_texts.append(_flatten(users[-1]["content"]) if users else "")
        return self._replies.pop(0) if self._replies else "STRATEGY: P7 skinny-join\nTARGET: t\nHYPOTHESIS: project only needed columns\nRATIONALE: narrow shuffle"


# ── Flag ──────────────────────────────────────────────────────────────────────

def test_stepwise_enabled_by_default(monkeypatch):
    monkeypatch.delenv("GENIE_STEPWISE_ITERATION", raising=False)
    assert stepwise_enabled()
    monkeypatch.setenv("GENIE_STEPWISE_ITERATION", "0")
    assert not stepwise_enabled()


# ── Step A ────────────────────────────────────────────────────────────────────

def test_diagnose_skipped_without_evidence_blocks():
    provider = _RecordingProvider()
    ev = _evidence(hotspot_block="", landscape_block="", skeleton_block="",
                   dup_subtree_note="")
    assert run_diagnose_step(provider, "m", "disable", ev) == ""
    assert provider.user_texts == []        # no call — nothing to cite


def test_diagnose_prompt_carries_all_evidence():
    provider = _RecordingProvider(replies=["BOTTLENECK: S1 scan"])
    ev = _evidence()
    out = run_diagnose_step(provider, "m", "disable", ev)
    assert out == "BOTTLENECK: S1 scan"
    user_msg = provider.user_texts[0]
    assert "Runtime stage hotspots" in user_msg
    assert "Table landscape" in user_msg
    assert "plan skeleton" in user_msg
    # Diagnosis sees evidence, not the fix menu or the SQL rewrite request.
    assert "P1" not in user_msg


# ── Step B / hypothesis ───────────────────────────────────────────────────────

def test_parse_hypothesis_extracts_line():
    reply = "STRATEGY: P5 pushdown\nTARGET: events\nHYPOTHESIS: push date filter into scan\nRATIONALE: x"
    assert parse_hypothesis(reply) == "push date filter into scan"
    assert parse_hypothesis("no structured output") == ""


def test_prelude_full_flow_produces_lean_rewrite_message():
    provider = _RecordingProvider(replies=[
        "BOTTLENECK: S1 dominates\nEVIDENCE: cpu 60%\nSECONDARY: none",
        "STRATEGY: P7 skinny-join\nTARGET: t\nHYPOTHESIS: narrow the join\nRATIONALE: y",
    ])
    result = run_stepwise_prelude(provider, "m", "disable", _evidence())
    assert result.calls_used == 2
    assert result.hypothesis == "narrow the join"
    # Step B saw the strategy menu and directions.
    step_b_msg = provider.user_texts[1]
    assert "P7" in step_b_msg and "fix-select-star" in step_b_msg
    # Step C message is conclusions-only: no raw evidence blocks.
    assert "Diagnosis:" in result.rewrite_user_msg
    assert "Chosen strategy:" in result.rewrite_user_msg
    assert "Runtime stage hotspots" not in result.rewrite_user_msg
    assert "SELECT a FROM t" in result.rewrite_user_msg


def test_prelude_degrades_to_legacy_context_when_llm_fails():
    class _DeadProvider:
        def complete_text(self, req):
            raise RuntimeError("provider down")
    result = run_stepwise_prelude(_DeadProvider(), "m", "disable", _evidence())
    assert result.diagnosis == "" and result.strategy == ""
    # Legacy fallback: evidence rides along with the rewrite request itself.
    assert "fix-select-star" in result.rewrite_user_msg
    assert "COMPLETE optimized SQL" in result.rewrite_user_msg


def test_record_fields_caps_length():
    result = StepwiseResult(diagnosis="d" * 1000, strategy="s" * 1000)
    fields = record_fields(result)
    assert len(fields["diagnosis"]) == 400
    assert len(fields["strategy"]) == 400
    assert record_fields(StepwiseResult()) == {}


def test_compose_rewrite_message_legacy_includes_skeleton():
    msg = compose_rewrite_message(_evidence(), diagnosis="", strategy="")
    assert "plan skeleton" in msg          # evidence falls through when A/B absent


# ── Loop-level: flag off → exactly one provider call per iteration ────────────

def test_loop_with_stepwise_disabled_is_single_call(monkeypatch):
    from genie.skills.trino_query import QueryMetrics
    from genie.skills.trino_query import research as direct_research

    monkeypatch.setenv("GENIE_STEPWISE_ITERATION", "0")
    monkeypatch.setenv("GENIE_V48_SEED_DECOMPOSE", "0")

    def _measure_side_effect(sql, *args, **kwargs):
        return {
            "median": 100.0, "samples": [100.0], "row_count": 1,
            "observed_row_count": 1, "rows": [(1,)], "captured_row_count": 1,
            "max_capture_rows": 100_000, "capture_status": "complete",
            "completeness": "verified_complete",
            "metrics": QueryMetrics(wall_time_ms=100, peak_memory_bytes=0),
        }

    provider = _RecordingProvider(replies=["nothing\n```sql\nSELECT a FROM t\n```"])
    with patch.object(direct_research, "_measure", side_effect=_measure_side_effect):
        direct_research._run_optimization_loop(
            provider=provider,
            model="test-model",
            reasoning="disable",
            original_sql="SELECT a FROM t",
            metric_key="wall_time_ms",
            max_iterations=1,
            verify_runs=1,
            output=MagicMock(),
            build_prompt=lambda *a, **k: "",
            explain_runner=None,
        )
    assert len(provider.user_texts) == 1
    assert "Return the COMPLETE optimized SQL" in provider.user_texts[0]


def test_compose_partial_step_failure_keeps_evidence():
    ev = _evidence(static_block="Static analysis findings:\n- avoid SELECT *")
    # Step B failed, Step A succeeded → static findings + ranked directions
    # must still reach the rewrite call (the legacy loop always sent them).
    msg = compose_rewrite_message(ev, "BOTTLENECK: scan-heavy stage 1", "")
    assert "fix-select-star" in msg
    assert "avoid SELECT *" in msg
    # Step A failed, Step B succeeded → the plan skeleton must still flow.
    msg = compose_rewrite_message(ev, "", "STRATEGY: P7 skinny-join")
    assert "TableScan[hive.raw.t]" in msg
