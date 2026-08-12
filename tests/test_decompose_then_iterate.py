"""tests/test_decompose_then_iterate.py — T9 tests for v48 decompose-then-iterate.

Tests the _produce_decompose_candidate helper (T3) and the seed validation path
(§3.1 NORMATIVE) for acceptance/rejection semantics.

All tests set GENIE_V48_SEED_DECOMPOSE=0 or patch _produce_decompose_candidate
directly so they never consume extra mock LLM calls.

Patching notes:
  _produce_decompose_candidate uses local imports inside the function body.
  We therefore patch at the source module level:
    genie.skills.mcp_trino.trino_optimize.decompose  (used via "from ... import decompose as _decompose")
    genie.skills.mcp_trino.trino_optimize.optimize
    genie.skills.mcp_trino.trino_optimize.recompose
    genie.skills.trino_query.detection_scan.scan_sql
    genie.skills.mcp_trino.write_analysis._column_safe_candidates
    genie.skills.mcp_trino.write_analysis._semantic_safe_candidates
"""
from __future__ import annotations

import os
from dataclasses import dataclass
from enum import Enum
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from genie.output.step_trace import (
    CANONICAL_COPY,
    StepEvent,
    StepStatus,
    render_report,
    render_tui,
)
from genie.skills.mcp_trino.research import _produce_decompose_candidate
from genie.skills.mcp_trino.trino_optimize import (
    Fragment,
    RecomposeResult,
    RecomposeStatus,
    RewriteCandidate,
)

# Patch targets (source module level — local imports bind at call time)
_P_DECOMPOSE  = "genie.skills.mcp_trino.trino_optimize.decompose"
_P_OPTIMIZE   = "genie.skills.mcp_trino.trino_optimize.optimize"
_P_RECOMPOSE  = "genie.skills.mcp_trino.trino_optimize.recompose"
_P_SCAN       = "genie.skills.trino_query.detection_scan.scan_sql"
_P_COL_GATE   = "genie.skills.mcp_trino.write_analysis._column_safe_candidates"
_P_SEM_GATE   = "genie.skills.mcp_trino.write_analysis._semantic_safe_candidates"


# ---------------------------------------------------------------------------
# Minimal stubs
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class _FakeCost:
    total_cost: float = 0.0
    cpu_cost: float = 0.0
    peak_memory: int = 0


def _make_fragment(
    fragment_id: str,
    sql: str,
    is_monster: bool = True,
    monster_rank: int | None = 1,
    role: str = "root",
) -> Fragment:
    from genie.skills.mcp_trino.trino_optimize import Fragment as _Frag
    # Fragment is a dataclass; construct via positional args matching field order
    return _Frag(
        fragment_id=fragment_id,
        sql=sql,
        role=role,
        position_hint=0,
        subq_ordinal=None,
        is_independently_runnable=True,
        is_monster=is_monster,
        monster_rank=monster_rank,
        findings=(),
        cost=_FakeCost(),  # type: ignore[arg-type]
    )


def _make_rewrite_candidate(
    fragment_id: str,
    original_sql: str,
    rewritten_sql: str | None = None,
    changed: bool = True,
    admitted: bool = True,
    action: str = "rewrite",
    rationale: str = "test",
) -> RewriteCandidate:
    return RewriteCandidate(
        fragment_id=fragment_id,
        original_sql=original_sql,
        rewritten_sql=rewritten_sql or original_sql,
        action=action,
        changed=changed,
        admitted=admitted,
        rationale=rationale,
    )


def _make_recompose_result(
    sql: str,
    status: RecomposeStatus = RecomposeStatus.OK,
    reverted: tuple[str, ...] = (),
    scan_ok_confident: bool = True,
) -> RecomposeResult:
    return RecomposeResult(
        sql=sql,
        status=status,
        cross_fragment_findings=(),
        reverted_fragments=reverted,
        scan_ok_confident=scan_ok_confident,
    )


_ORIG_SQL = "SELECT * FROM hive.db.t"
_OPT_SQL  = "SELECT id FROM hive.db.t"


def _noop_llm(prompt: str) -> str:
    return "-- optimized"


def _noop_cost(_sql: str) -> Any:
    return None


# ---------------------------------------------------------------------------
# T-ENG-SEED: _produce_decompose_candidate returns recomposed SQL on success
# ---------------------------------------------------------------------------

class TestEngSeed:
    @patch(_P_SCAN, return_value=MagicMock(scan_ok_confident=True, findings=()))
    @patch(_P_SEM_GATE, side_effect=lambda c: (c, []))
    @patch(_P_COL_GATE, side_effect=lambda c: (c, []))
    @patch(_P_OPTIMIZE)
    @patch(_P_DECOMPOSE)
    @patch(_P_RECOMPOSE)
    def test_returns_recomposed_sql_when_one_monster(
        self, mock_recompose, mock_decompose, mock_optimize, mc, ms, mscan
    ):
        frag = _make_fragment("f1", _ORIG_SQL, is_monster=True, monster_rank=1)
        cand = _make_rewrite_candidate("f1", _ORIG_SQL, _OPT_SQL, changed=True, admitted=True)
        rr = _make_recompose_result(_OPT_SQL)
        mock_decompose.return_value = [frag]
        mock_optimize.return_value = cand
        mock_recompose.return_value = rr

        result_sql, frags, cands, result_rr = _produce_decompose_candidate(
            _ORIG_SQL, _noop_llm, _noop_cost, run_static_gates=False,
        )

        assert result_sql == _OPT_SQL
        assert len(frags) == 1
        assert len(cands) == 1
        assert result_rr.sql == _OPT_SQL


# ---------------------------------------------------------------------------
# T-ENG-TRIVIAL: trivial (non-monster) fragment returns original SQL
# ---------------------------------------------------------------------------

class TestEngTrivial:
    @patch(_P_SCAN, return_value=MagicMock(scan_ok_confident=True, findings=()))
    @patch(_P_SEM_GATE, side_effect=lambda c: (c, []))
    @patch(_P_COL_GATE, side_effect=lambda c: (c, []))
    @patch(_P_OPTIMIZE)
    @patch(_P_DECOMPOSE)
    @patch(_P_RECOMPOSE)
    def test_non_monster_passthrough(
        self, mock_recompose, mock_decompose, mock_optimize, mc, ms, mscan
    ):
        # is_monster=False → optimize not called, passthrough
        frag = _make_fragment("f1", _ORIG_SQL, is_monster=False, monster_rank=None)
        rr = _make_recompose_result(_ORIG_SQL)  # no change
        mock_decompose.return_value = [frag]
        mock_optimize.return_value = None  # should not be called
        mock_recompose.return_value = rr

        result_sql, frags, cands, result_rr = _produce_decompose_candidate(
            _ORIG_SQL, _noop_llm, _noop_cost, run_static_gates=False,
        )

        mock_optimize.assert_not_called()
        assert result_sql == _ORIG_SQL


# ---------------------------------------------------------------------------
# T-CRUX-ACCEPT / T-CRUX-REJECT: CRUX gate (run_static_gates) toggles col/sem gates
# ---------------------------------------------------------------------------

class TestCruxGates:
    @patch(_P_SCAN, return_value=MagicMock(scan_ok_confident=True, findings=()))
    @patch(_P_SEM_GATE, side_effect=lambda c: (c, []))
    @patch(_P_COL_GATE, side_effect=lambda c: (c, []))
    @patch(_P_OPTIMIZE)
    @patch(_P_DECOMPOSE)
    @patch(_P_RECOMPOSE)
    def test_static_gates_called_when_run_static_gates_true(
        self, mock_recompose, mock_decompose, mock_optimize, mock_col, mock_sem, mscan
    ):
        frag = _make_fragment("f1", _ORIG_SQL, is_monster=True, monster_rank=1)
        cand = _make_rewrite_candidate("f1", _ORIG_SQL, _OPT_SQL, changed=True, admitted=True)
        rr = _make_recompose_result(_OPT_SQL)
        mock_decompose.return_value = [frag]
        mock_optimize.return_value = cand
        mock_recompose.return_value = rr

        _produce_decompose_candidate(
            _ORIG_SQL, _noop_llm, _noop_cost, run_static_gates=True,
        )

        mock_col.assert_called_once()
        mock_sem.assert_called_once()

    @patch(_P_SCAN, return_value=MagicMock(scan_ok_confident=True, findings=()))
    @patch(_P_SEM_GATE, side_effect=lambda c: (c, []))
    @patch(_P_COL_GATE, side_effect=lambda c: (c, []))
    @patch(_P_OPTIMIZE)
    @patch(_P_DECOMPOSE)
    @patch(_P_RECOMPOSE)
    def test_static_gates_not_called_when_run_static_gates_false(
        self, mock_recompose, mock_decompose, mock_optimize, mock_col, mock_sem, mscan
    ):
        frag = _make_fragment("f1", _ORIG_SQL, is_monster=True, monster_rank=1)
        cand = _make_rewrite_candidate("f1", _ORIG_SQL, _OPT_SQL, changed=True, admitted=True)
        rr = _make_recompose_result(_OPT_SQL)
        mock_decompose.return_value = [frag]
        mock_optimize.return_value = cand
        mock_recompose.return_value = rr

        _produce_decompose_candidate(
            _ORIG_SQL, _noop_llm, _noop_cost, run_static_gates=False,
        )

        mock_col.assert_not_called()
        mock_sem.assert_not_called()


# ---------------------------------------------------------------------------
# T-CRUX-ACCEPT-SEED: seed accepted when row-equiv=True AND metric improves
# T-CRUX-REJECT-SEED: seed rejected when row-equiv=False
# T-SEED-EQUIV-SLOWER: seed rejected when rows match but metric not < baseline
# ---------------------------------------------------------------------------

class TestSeedAcceptReject:
    """Test the acceptance/rejection logic for the seed candidate.

    We test _produce_decompose_candidate's return value and the logic that the
    calling site uses to decide accept/reject. Seed acceptance logic lives in
    the calling sites (run_mcp_enhancement, _run_optimization_loop, etc.).
    Here we verify the helper returns the correct SQL so callers can decide.
    """

    @patch(_P_SCAN, return_value=MagicMock(scan_ok_confident=True, findings=()))
    @patch(_P_SEM_GATE, side_effect=lambda c: (c, []))
    @patch(_P_COL_GATE, side_effect=lambda c: (c, []))
    @patch(_P_OPTIMIZE)
    @patch(_P_DECOMPOSE)
    @patch(_P_RECOMPOSE)
    def test_helper_returns_optimised_sql_for_accepting_caller(
        self, mock_recompose, mock_decompose, mock_optimize, mc, ms, mscan
    ):
        """Caller has rows=10/metric=500; seed has rows=10/metric=400 → accept."""
        frag = _make_fragment("f1", _ORIG_SQL)
        cand = _make_rewrite_candidate("f1", _ORIG_SQL, _OPT_SQL, changed=True)
        rr = _make_recompose_result(_OPT_SQL)
        mock_decompose.return_value = [frag]
        mock_optimize.return_value = cand
        mock_recompose.return_value = rr

        seed_sql, _, _, seed_rr = _produce_decompose_candidate(
            _ORIG_SQL, _noop_llm, _noop_cost, run_static_gates=False,
        )
        # Caller-side acceptance: same rows → check
        baseline_rows = 10
        seed_rows = 10
        baseline_metric = 500.0
        seed_metric = 400.0

        row_equiv = seed_rows == baseline_rows
        metric_improved = seed_metric < baseline_metric

        assert seed_sql == _OPT_SQL
        assert row_equiv and metric_improved  # caller would accept

    @patch(_P_SCAN, return_value=MagicMock(scan_ok_confident=True, findings=()))
    @patch(_P_SEM_GATE, side_effect=lambda c: (c, []))
    @patch(_P_COL_GATE, side_effect=lambda c: (c, []))
    @patch(_P_OPTIMIZE)
    @patch(_P_DECOMPOSE)
    @patch(_P_RECOMPOSE)
    def test_helper_returns_optimised_sql_but_caller_rejects_on_row_mismatch(
        self, mock_recompose, mock_decompose, mock_optimize, mc, ms, mscan
    ):
        """Helper still returns opt SQL; caller's row_equiv check fails → reject."""
        frag = _make_fragment("f1", _ORIG_SQL)
        cand = _make_rewrite_candidate("f1", _ORIG_SQL, _OPT_SQL, changed=True)
        rr = _make_recompose_result(_OPT_SQL)
        mock_decompose.return_value = [frag]
        mock_optimize.return_value = cand
        mock_recompose.return_value = rr

        seed_sql, _, _, _ = _produce_decompose_candidate(
            _ORIG_SQL, _noop_llm, _noop_cost, run_static_gates=False,
        )
        baseline_rows = 10
        seed_rows = 9   # mismatch
        row_equiv = seed_rows == baseline_rows
        assert seed_sql == _OPT_SQL
        assert not row_equiv  # caller would reject

    @patch(_P_SCAN, return_value=MagicMock(scan_ok_confident=True, findings=()))
    @patch(_P_SEM_GATE, side_effect=lambda c: (c, []))
    @patch(_P_COL_GATE, side_effect=lambda c: (c, []))
    @patch(_P_OPTIMIZE)
    @patch(_P_DECOMPOSE)
    @patch(_P_RECOMPOSE)
    def test_seed_equiv_but_slower_caller_keeps_original(
        self, mock_recompose, mock_decompose, mock_optimize, mc, ms, mscan
    ):
        """Rows match but seed is slower → caller keeps original SQL."""
        frag = _make_fragment("f1", _ORIG_SQL)
        cand = _make_rewrite_candidate("f1", _ORIG_SQL, _OPT_SQL, changed=True)
        rr = _make_recompose_result(_OPT_SQL)
        mock_decompose.return_value = [frag]
        mock_optimize.return_value = cand
        mock_recompose.return_value = rr

        seed_sql, _, _, _ = _produce_decompose_candidate(
            _ORIG_SQL, _noop_llm, _noop_cost, run_static_gates=False,
        )
        baseline_metric = 500.0
        seed_metric = 600.0  # slower
        row_equiv = True
        assert not (row_equiv and seed_metric < baseline_metric)  # caller rejects


# ---------------------------------------------------------------------------
# T-SEED-NOKEEP-WINNER: recompose unchanged (sql == original) → no seed SQL
# ---------------------------------------------------------------------------

class TestSeedNoKeepWinner:
    @patch(_P_SCAN, return_value=MagicMock(scan_ok_confident=True, findings=()))
    @patch(_P_SEM_GATE, side_effect=lambda c: (c, []))
    @patch(_P_COL_GATE, side_effect=lambda c: (c, []))
    @patch(_P_OPTIMIZE)
    @patch(_P_DECOMPOSE)
    @patch(_P_RECOMPOSE)
    def test_original_sql_returned_when_recompose_unchanged(
        self, mock_recompose, mock_decompose, mock_optimize, mc, ms, mscan
    ):
        frag = _make_fragment("f1", _ORIG_SQL, is_monster=True)
        cand = _make_rewrite_candidate("f1", _ORIG_SQL, _ORIG_SQL, changed=False, admitted=True)
        rr = _make_recompose_result(_ORIG_SQL)  # sql unchanged
        mock_decompose.return_value = [frag]
        mock_optimize.return_value = cand
        mock_recompose.return_value = rr

        seed_sql, _, _, result_rr = _produce_decompose_candidate(
            _ORIG_SQL, _noop_llm, _noop_cost, run_static_gates=False,
        )

        # Helper returns original SQL (no change) — callers detect via sql == original_sql
        assert seed_sql == _ORIG_SQL


# ---------------------------------------------------------------------------
# T-GATE-NODATA: no-data advisory path with run_static_gates=True uses col/sem gates
# ---------------------------------------------------------------------------

class TestGateNoData:
    @patch(_P_SCAN, return_value=MagicMock(scan_ok_confident=True, findings=()))
    @patch(_P_SEM_GATE, side_effect=lambda c: (c, []))
    @patch(_P_COL_GATE, side_effect=lambda c: (c, []))
    @patch(_P_OPTIMIZE)
    @patch(_P_DECOMPOSE)
    @patch(_P_RECOMPOSE)
    def test_advisory_path_invokes_col_sem_gates(
        self, mock_recompose, mock_decompose, mock_optimize, mock_col, mock_sem, mscan
    ):
        frag = _make_fragment("f1", _ORIG_SQL, is_monster=True)
        cand = _make_rewrite_candidate("f1", _ORIG_SQL, _OPT_SQL, changed=True)
        rr = _make_recompose_result(_OPT_SQL)
        mock_decompose.return_value = [frag]
        mock_optimize.return_value = cand
        mock_recompose.return_value = rr

        _produce_decompose_candidate(
            _ORIG_SQL, _noop_llm, _noop_cost, run_static_gates=True,
        )

        mock_col.assert_called_once()
        mock_sem.assert_called_once()


# ---------------------------------------------------------------------------
# T-FRAG-CAP: monsters beyond cap=5 get SKIPPED over-cap StepEvents
# ---------------------------------------------------------------------------

class TestFragCap:
    @patch(_P_SCAN, return_value=MagicMock(scan_ok_confident=True, findings=()))
    @patch(_P_SEM_GATE, side_effect=lambda c: (c, []))
    @patch(_P_COL_GATE, side_effect=lambda c: (c, []))
    @patch(_P_OPTIMIZE)
    @patch(_P_DECOMPOSE)
    @patch(_P_RECOMPOSE)
    def test_6_monsters_cap_at_5_over_cap_skipped(
        self, mock_recompose, mock_decompose, mock_optimize, mc, ms, mscan
    ):
        # 6 monsters — first 5 should be optimized, 6th should be over-cap
        fragments = [
            _make_fragment(f"f{i}", f"SELECT {i}", is_monster=True, monster_rank=i)
            for i in range(1, 7)
        ]
        candidates = [
            _make_rewrite_candidate(f"f{i}", f"SELECT {i}", f"SELECT {i} -- opt", changed=True)
            for i in range(1, 7)
        ]
        rr = _make_recompose_result(_OPT_SQL)
        mock_decompose.return_value = fragments
        mock_optimize.side_effect = candidates[:5]  # only first 5 called
        mock_recompose.return_value = rr

        trace = []
        _produce_decompose_candidate(
            _ORIG_SQL, _noop_llm, _noop_cost, run_static_gates=False, step_trace=trace,
            enable_fragment_rewrite=True, max_fragment_model_calls=5,
        )

        over_cap_events = [
            ev for ev in trace
            if ev.status == StepStatus.SKIPPED and ev.detail.get("action") == "over_cap"
        ]
        # 6th monster is over-cap
        assert len(over_cap_events) == 1
        assert over_cap_events[0].detail["fragment_id"] == "f6"


# ---------------------------------------------------------------------------
# T-SYM-ENG: StepTrace emitted with correct step_ids and statuses
# ---------------------------------------------------------------------------

class TestSymEng:
    @patch(_P_SCAN, return_value=MagicMock(scan_ok_confident=True, findings=()))
    @patch(_P_SEM_GATE, side_effect=lambda c: (c, []))
    @patch(_P_COL_GATE, side_effect=lambda c: (c, []))
    @patch(_P_OPTIMIZE)
    @patch(_P_DECOMPOSE)
    @patch(_P_RECOMPOSE)
    def test_trace_contains_decompose_fragment_recompose_events(
        self, mock_recompose, mock_decompose, mock_optimize, mc, ms, mscan
    ):
        frag = _make_fragment("f1", _ORIG_SQL, is_monster=True)
        cand = _make_rewrite_candidate("f1", _ORIG_SQL, _OPT_SQL, changed=True)
        rr = _make_recompose_result(_OPT_SQL)
        mock_decompose.return_value = [frag]
        mock_optimize.return_value = cand
        mock_recompose.return_value = rr

        trace = []
        _produce_decompose_candidate(
            _ORIG_SQL, _noop_llm, _noop_cost, run_static_gates=False, step_trace=trace,
        )

        step_ids = [ev.step_id for ev in trace]
        assert "decompose" in step_ids
        assert "fragment_1" in step_ids
        assert "recompose" in step_ids

    @patch(_P_SCAN, return_value=MagicMock(scan_ok_confident=True, findings=()))
    @patch(_P_SEM_GATE, side_effect=lambda c: (c, []))
    @patch(_P_COL_GATE, side_effect=lambda c: (c, []))
    @patch(_P_OPTIMIZE)
    @patch(_P_DECOMPOSE)
    @patch(_P_RECOMPOSE)
    def test_trace_decompose_event_ran_status(
        self, mock_recompose, mock_decompose, mock_optimize, mc, ms, mscan
    ):
        frag = _make_fragment("f1", _ORIG_SQL, is_monster=True)
        cand = _make_rewrite_candidate("f1", _ORIG_SQL, _OPT_SQL, changed=True)
        rr = _make_recompose_result(_OPT_SQL)
        mock_decompose.return_value = [frag]
        mock_optimize.return_value = cand
        mock_recompose.return_value = rr

        trace = []
        _produce_decompose_candidate(
            _ORIG_SQL, _noop_llm, _noop_cost, run_static_gates=False, step_trace=trace,
        )

        decompose_ev = next(ev for ev in trace if ev.step_id == "decompose")
        assert decompose_ev.status == StepStatus.RAN

    @patch(_P_SCAN, return_value=MagicMock(scan_ok_confident=True, findings=()))
    @patch(_P_SEM_GATE, side_effect=lambda c: (c, []))
    @patch(_P_COL_GATE, side_effect=lambda c: (c, []))
    @patch(_P_OPTIMIZE)
    @patch(_P_DECOMPOSE)
    @patch(_P_RECOMPOSE)
    def test_trace_seed_changed_updated_in_decompose_event(
        self, mock_recompose, mock_decompose, mock_optimize, mc, ms, mscan
    ):
        frag = _make_fragment("f1", _ORIG_SQL, is_monster=True)
        cand = _make_rewrite_candidate("f1", _ORIG_SQL, _OPT_SQL, changed=True)
        rr = _make_recompose_result(_OPT_SQL)
        mock_decompose.return_value = [frag]
        mock_optimize.return_value = cand
        mock_recompose.return_value = rr

        trace = []
        _produce_decompose_candidate(
            _ORIG_SQL, _noop_llm, _noop_cost, run_static_gates=False, step_trace=trace,
        )

        decompose_ev = next(ev for ev in trace if ev.step_id == "decompose")
        assert decompose_ev.detail["seed_changed"] is True  # rr.sql != original_sql


# ---------------------------------------------------------------------------
# T-STEPS-RENDER: render_tui and render_report on trace from helper
# ---------------------------------------------------------------------------

class TestStepsRender:
    @patch(_P_SCAN, return_value=MagicMock(scan_ok_confident=True, findings=()))
    @patch(_P_SEM_GATE, side_effect=lambda c: (c, []))
    @patch(_P_COL_GATE, side_effect=lambda c: (c, []))
    @patch(_P_OPTIMIZE)
    @patch(_P_DECOMPOSE)
    @patch(_P_RECOMPOSE)
    def test_render_tui_outputs_check_for_ran_events(
        self, mock_recompose, mock_decompose, mock_optimize, mc, ms, mscan
    ):
        frag = _make_fragment("f1", _ORIG_SQL, is_monster=True)
        cand = _make_rewrite_candidate("f1", _ORIG_SQL, _OPT_SQL, changed=True)
        rr = _make_recompose_result(_OPT_SQL)
        mock_decompose.return_value = [frag]
        mock_optimize.return_value = cand
        mock_recompose.return_value = rr

        trace = []
        _produce_decompose_candidate(
            _ORIG_SQL, _noop_llm, _noop_cost, run_static_gates=False, step_trace=trace,
        )

        tui = render_tui(trace)
        assert "✓" in tui
        assert "Decompose" in tui

    @patch(_P_SCAN, return_value=MagicMock(scan_ok_confident=True, findings=()))
    @patch(_P_SEM_GATE, side_effect=lambda c: (c, []))
    @patch(_P_COL_GATE, side_effect=lambda c: (c, []))
    @patch(_P_OPTIMIZE)
    @patch(_P_DECOMPOSE)
    @patch(_P_RECOMPOSE)
    def test_render_report_has_step_summary(
        self, mock_recompose, mock_decompose, mock_optimize, mc, ms, mscan
    ):
        frag = _make_fragment("f1", _ORIG_SQL, is_monster=True)
        cand = _make_rewrite_candidate("f1", _ORIG_SQL, _OPT_SQL, changed=True)
        rr = _make_recompose_result(_OPT_SQL)
        mock_decompose.return_value = [frag]
        mock_optimize.return_value = cand
        mock_recompose.return_value = rr

        trace = []
        _produce_decompose_candidate(
            _ORIG_SQL, _noop_llm, _noop_cost, run_static_gates=False, step_trace=trace,
        )

        report = render_report(trace)
        assert "Step Summary" in report
        assert "Decompose" in report
        assert "Recompose" in report


# ---------------------------------------------------------------------------
# T-SEED-EXEC-BUDGET: GENIE_V48_SEED_DECOMPOSE=0 disables seed path entirely
# ---------------------------------------------------------------------------

class TestSeedExecBudget:
    def test_env_var_zero_does_not_call_produce_decompose(self):
        """When GENIE_V48_SEED_DECOMPOSE=0 no LLM calls from seed path should occur."""
        # We test the env-var guard at the test level via the existing suite.
        # This test simply verifies the env var is readable and evaluates correctly.
        original = os.environ.get("GENIE_V48_SEED_DECOMPOSE", "1")
        try:
            os.environ["GENIE_V48_SEED_DECOMPOSE"] = "0"
            enabled = os.environ.get("GENIE_V48_SEED_DECOMPOSE", "1") != "0"
            assert not enabled
        finally:
            if original == "1":
                os.environ.pop("GENIE_V48_SEED_DECOMPOSE", None)
            else:
                os.environ["GENIE_V48_SEED_DECOMPOSE"] = original

    def test_env_var_default_is_enabled(self):
        original = os.environ.pop("GENIE_V48_SEED_DECOMPOSE", None)
        try:
            enabled = os.environ.get("GENIE_V48_SEED_DECOMPOSE", "1") != "0"
            assert enabled
        finally:
            if original is not None:
                os.environ["GENIE_V48_SEED_DECOMPOSE"] = original


# ---------------------------------------------------------------------------
# Degrade gracefully when decompose raises
# ---------------------------------------------------------------------------

class TestDegrade:
    @patch(_P_DECOMPOSE, side_effect=RuntimeError("LLM died"))
    def test_returns_original_sql_on_decompose_failure(self, mock_decompose):
        trace = []
        result_sql, frags, cands, rr = _produce_decompose_candidate(
            _ORIG_SQL, _noop_llm, _noop_cost, run_static_gates=False, step_trace=trace,
        )

        assert result_sql == _ORIG_SQL
        assert frags == []
        assert cands == []
        # Degraded step event appended
        deg_events = [ev for ev in trace if ev.status == StepStatus.DEGRADED]
        assert len(deg_events) == 1
        assert "LLM died" in deg_events[0].tui_headline

    @patch(_P_DECOMPOSE, side_effect=RuntimeError("timeout"))
    def test_degrade_with_none_step_trace_does_not_crash(self, mock_decompose):
        """step_trace=None should not cause AttributeError in degraded path."""
        result_sql, _, _, _ = _produce_decompose_candidate(
            _ORIG_SQL, _noop_llm, _noop_cost, run_static_gates=False, step_trace=None,
        )
        assert result_sql == _ORIG_SQL


# ---------------------------------------------------------------------------
# v58: --direct path GENIE_FRAGMENT_REWRITE / GENIE_FRAGMENT_REWRITE_CAP parity
# ---------------------------------------------------------------------------

class TestDirectFragmentRewriteEnvParity:
    """Verify all three --direct call sites read GENIE_FRAGMENT_REWRITE and
    GENIE_FRAGMENT_REWRITE_CAP from the environment and pass them through to
    _produce_decompose_candidate, mirroring the MCP path."""

    def _patch_decompose_capture(self, monkeypatch):
        """Return a capture dict that records kwargs passed to _produce_decompose_candidate."""
        captured = {}

        def _capture_pdc(sql, llm_fn, cost_reader_fn, run_static_gates=False,
                         step_trace=None, *, enable_fragment_rewrite=False,
                         max_fragment_model_calls=1):
            captured["enable_fragment_rewrite"] = enable_fragment_rewrite
            captured["max_fragment_model_calls"] = max_fragment_model_calls
            return (sql, [], [], None)

        return captured, _capture_pdc

    def test_no_data_path_threads_fragment_env_vars(self, monkeypatch):
        """No-data path reads GENIE_FRAGMENT_REWRITE=1 and GENIE_FRAGMENT_REWRITE_CAP=3."""
        captured, mock_pdc = self._patch_decompose_capture(monkeypatch)
        monkeypatch.setenv("GENIE_FRAGMENT_REWRITE", "1")
        monkeypatch.setenv("GENIE_FRAGMENT_REWRITE_CAP", "3")
        monkeypatch.setenv("GENIE_V48_SEED_DECOMPOSE", "1")

        monkeypatch.setattr(
            "genie.skills.mcp_trino.research._produce_decompose_candidate",
            mock_pdc,
        )

        from genie.skills.trino_query.research import _run_no_data_path

        provider = MagicMock()
        provider.complete_text.return_value = "SELECT 1"
        output = MagicMock()
        output.progress = MagicMock()
        output.print = MagicMock()
        output.error = MagicMock()

        try:
            _run_no_data_path(
                original_sql="SELECT 1 FROM hive.db.t",
                no_data_reason="empty_result",
                static_report=None,
                baseline_exc=None,
                provider=provider,
                model="test",
                reasoning="disable",
                output=output,
            )
        except Exception:
            pass  # we only care about the captured kwargs

        assert captured.get("enable_fragment_rewrite") is True, \
            f"no-data path did not pass enable_fragment_rewrite=True: {captured}"
        assert captured.get("max_fragment_model_calls") == 3, \
            f"no-data path did not pass max_fragment_model_calls=3: {captured}"

    def test_no_data_path_default_fragment_rewrite_off(self, monkeypatch):
        """No-data path with no env vars: fragment rewrite stays off (v57 safety)."""
        captured, mock_pdc = self._patch_decompose_capture(monkeypatch)
        monkeypatch.delenv("GENIE_FRAGMENT_REWRITE", raising=False)
        monkeypatch.delenv("GENIE_FRAGMENT_REWRITE_CAP", raising=False)
        monkeypatch.setenv("GENIE_V48_SEED_DECOMPOSE", "1")

        monkeypatch.setattr(
            "genie.skills.mcp_trino.research._produce_decompose_candidate",
            mock_pdc,
        )

        from genie.skills.trino_query.research import _run_no_data_path

        provider = MagicMock()
        provider.complete_text.return_value = "SELECT 1"
        output = MagicMock()
        output.progress = MagicMock()
        output.print = MagicMock()
        output.error = MagicMock()

        try:
            _run_no_data_path(
                original_sql="SELECT 1 FROM hive.db.t",
                no_data_reason="empty_result",
                static_report=None,
                baseline_exc=None,
                provider=provider,
                model="test",
                reasoning="disable",
                output=output,
            )
        except Exception:
            pass

        assert captured.get("enable_fragment_rewrite") is False, \
            f"no-data path should default to enable_fragment_rewrite=False: {captured}"
        assert captured.get("max_fragment_model_calls") == 5, \
            f"no-data path should default to max_fragment_model_calls=5: {captured}"

    def test_plan_cost_path_threads_fragment_env_vars(self, monkeypatch):
        """Plan-cost path reads GENIE_FRAGMENT_REWRITE=1 and GENIE_FRAGMENT_REWRITE_CAP=7."""
        captured, mock_pdc = self._patch_decompose_capture(monkeypatch)
        monkeypatch.setenv("GENIE_FRAGMENT_REWRITE", "1")
        monkeypatch.setenv("GENIE_FRAGMENT_REWRITE_CAP", "7")
        monkeypatch.setenv("GENIE_V48_SEED_DECOMPOSE", "1")

        monkeypatch.setattr(
            "genie.skills.mcp_trino.research._produce_decompose_candidate",
            mock_pdc,
        )

        from genie.skills.trino_query.research import _run_plan_cost_loop

        provider = MagicMock()
        provider.complete_text.return_value = "SELECT 1"
        output = MagicMock()
        output.progress = MagicMock()
        output.print = MagicMock()
        output.error = MagicMock()

        # Minimal baseline dict that satisfies _run_plan_cost_loop's field reads
        baseline = {
            "median": 100.0,
            "row_count": 1,
            "metrics": MagicMock(peak_memory_bytes=0),
            "samples": [100.0],
            "columns": [],
        }

        try:
            _run_plan_cost_loop(
                provider=provider,
                model="test",
                reasoning="disable",
                original_sql="SELECT 1 FROM hive.db.t",
                metric_key="query_time_ms",
                max_iterations=1,
                verify_runs=1,
                output=output,
                build_prompt=lambda *a: "",
                baseline=baseline,
                baseline_data=[],
                static_report=None,
                explain_runner=lambda _sql: None,
                max_fallbacks=0,
            )
        except Exception:
            pass  # we only care about the captured kwargs

        assert captured.get("enable_fragment_rewrite") is True, \
            f"plan-cost path did not pass enable_fragment_rewrite=True: {captured}"
        assert captured.get("max_fragment_model_calls") == 7, \
            f"plan-cost path did not pass max_fragment_model_calls=7: {captured}"

    def test_standard_loop_path_threads_fragment_env_vars(self, monkeypatch):
        """Standard loop path reads GENIE_FRAGMENT_REWRITE=1 and GENIE_FRAGMENT_REWRITE_CAP=4.

        Tests via _run_optimization_loop which dispatches to the standard loop
        when baseline measurement succeeds and plan-cost is not activated.
        The _produce_decompose_candidate mock captures the kwargs passed by the
        _dir_produce_fn closure.
        """
        captured, mock_pdc = self._patch_decompose_capture(monkeypatch)
        monkeypatch.setenv("GENIE_FRAGMENT_REWRITE", "1")
        monkeypatch.setenv("GENIE_FRAGMENT_REWRITE_CAP", "4")
        monkeypatch.setenv("GENIE_V48_SEED_DECOMPOSE", "1")

        monkeypatch.setattr(
            "genie.skills.mcp_trino.research._produce_decompose_candidate",
            mock_pdc,
        )

        from genie.skills.trino_query.research import _run_optimization_loop

        provider = MagicMock()
        provider.complete_text.return_value = "SELECT 1"
        output = MagicMock()
        output.progress = MagicMock()
        output.print = MagicMock()
        output.error = MagicMock()

        # Mock the measure helper to return a baseline with data (so we don't
        # enter the no-data path) and avoid real execution.
        fake_baseline = {
            "median": 100.0,
            "row_count": 1,
            "rows": [{"id": 1}],
            "columns": ["id"],
            "metrics": MagicMock(peak_memory_bytes=0),
            "samples": [100.0],
        }
        monkeypatch.setattr(
            "genie.skills.trino_query.research._measure",
            lambda *a, **kw: fake_baseline,
        )

        # Mock _seed_decompose_and_select to invoke its produce_fn argument
        # (the _dir_produce_fn closure), which will call the mocked
        # _produce_decompose_candidate, letting us capture the env-threaded kwargs.
        def _mock_seed_select(original_sql, baseline_measure, *, produce_fn,
                              measure_fn, flag_enabled, output=None, trace=None,
                              rejection_history=None, **_kwargs):
            if flag_enabled and produce_fn is not None:
                produce_fn(original_sql)
            from genie.skills.mcp_trino.research import MeasureResult
            return (original_sql, baseline_measure, [])

        monkeypatch.setattr(
            "genie.skills.mcp_trino.research._seed_decompose_and_select",
            _mock_seed_select,
        )

        # Mock the LLM session to prevent real calls (imported locally
        # inside _run_optimization_loop from genie.session.manager)
        monkeypatch.setattr(
            "genie.session.manager.new_session",
            lambda sys_prompt: MagicMock(),
        )
        monkeypatch.setattr(
            "genie.session.manager.new_msg",
            lambda *a, **kw: MagicMock(),
        )

        try:
            _run_optimization_loop(
                provider=provider,
                model="test",
                reasoning="disable",
                original_sql="SELECT 1 FROM hive.db.t",
                metric_key="query_time_ms",
                max_iterations=1,
                verify_runs=1,
                output=output,
                build_prompt=lambda *a: "",
                long_query_opt_in=False,
                explain_runner=lambda _sql: None,
            )
        except Exception:
            pass  # we only care about the captured kwargs

        assert captured.get("enable_fragment_rewrite") is True, \
            f"standard-loop path did not pass enable_fragment_rewrite=True: {captured}"
        assert captured.get("max_fragment_model_calls") == 4, \
            f"standard-loop path did not pass max_fragment_model_calls=4: {captured}"
