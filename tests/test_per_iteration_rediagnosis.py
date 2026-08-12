"""v32 T1: per-iteration re-diagnosis.

The pre-execution diagnosis injected into the system prompt describes the
ORIGINAL query. Once an improvement changes ``best_sql`` those directions are
stale, so the loop must re-diagnose the current ``best_sql`` (zero query cost)
and feed fresh directions into that iteration's model-visible context.

Strategy: drive the real ``--direct`` optimization loop for two iterations with
a mocked provider + ``_measure``. Iteration 1 returns a rewrite (``SELECT *``)
that the static analyzer flags (``select-star``) — which the ORIGINAL query did
not. The test asserts that after the rewrite is kept, a fresh ``select-star``
direction reaches the model in some iteration-2 prompt.

Stepwise note: with the Step A/B/C pipeline (GENIE_STEPWISE_ITERATION, default
on) one iteration spans several provider calls, and directions flow into the
Step B (strategize) prompt rather than the rewrite call. The assertions
therefore split calls at the first rewrite-step call (its marker text) instead
of assuming one call per iteration.
"""
from __future__ import annotations

from unittest.mock import MagicMock, patch

from genie.skills.trino_query import QueryMetrics
from genie.skills.trino_query import research as direct_research

_ORIGINAL = "SELECT a FROM t"          # no static findings
_REWRITE = "SELECT * FROM t"           # triggers r2 select-star

# Marker unique to the Step C / legacy rewrite request (Step B says
# "Do NOT write the SQL yet" instead).
_REWRITE_CALL_MARKER = "Return the COMPLETE optimized SQL"


def _measure_side_effect(sql, *args, **kwargs):
    """Baseline (original) is slow; the rewrite is faster + result-equivalent."""
    improved = sql.strip() != _ORIGINAL
    return {
        "median": 50.0 if improved else 100.0,
        "samples": [50.0 if improved else 100.0],
        "row_count": 1,
        "observed_row_count": 1,
        "rows": [(1,)],
        "captured_row_count": 1,
        "max_capture_rows": 100_000,
        "capture_status": "complete",
        "completeness": "verified_complete",
        "metrics": QueryMetrics(wall_time_ms=50 if improved else 100, peak_memory_bytes=0),
    }


class _SequencedProvider:
    """Returns the rewrite on every call; records each call's messages."""

    def __init__(self):
        self.calls: list[list[dict]] = []

    def complete_text(self, req):
        self.calls.append([dict(m) for m in req.messages])
        return f"Improve projection\n```sql\n{_REWRITE}\n```"


def _text(content):
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        return " ".join(
            (p.get("text") or "") if isinstance(p, dict) else str(p) for p in content
        )
    return str(content)


def _last_user(messages):
    users = [m for m in messages if m.get("role") == "user"]
    return _text(users[-1]["content"]) if users else ""


def test_direct_loop_reinjects_fresh_directions_after_best_sql_changes():
    provider = _SequencedProvider()

    with patch.object(direct_research, "_measure", side_effect=_measure_side_effect):
        direct_research._run_optimization_loop(
            provider=provider,
            model="test-model",
            reasoning="disable",
            original_sql=_ORIGINAL,
            metric_key="wall_time_ms",
            max_iterations=2,
            verify_runs=1,
            output=MagicMock(),
            build_prompt=lambda *a, **k: "SKILL_PROMPT_TEXT",
            explain_runner=None,
        )

    contexts = [_last_user(c) for c in provider.calls]

    # Iteration boundary: everything up to and including the FIRST rewrite-step
    # call belongs to iteration 1; everything after it is iteration ≥ 2.
    first_rewrite_idx = next(
        (i for i, ctx in enumerate(contexts) if _REWRITE_CALL_MARKER in ctx), None
    )
    assert first_rewrite_idx is not None, "no rewrite-step call observed"
    iter1_ctxs = contexts[: first_rewrite_idx + 1]
    later_ctxs = contexts[first_rewrite_idx + 1:]
    assert later_ctxs, "loop did not reach a second iteration"

    # Iteration 1 ran against the original (no findings) → no select-star direction.
    assert all("fix-select-star" not in ctx for ctx in iter1_ctxs)

    # Iteration 2 ran against the rewritten best_sql → a fresh re-diagnosis of
    # SELECT * surfaced the select-star direction in a model-visible prompt,
    # alongside the rewritten SQL as the current best.
    fresh = [ctx for ctx in later_ctxs if "fix-select-star" in ctx]
    assert fresh, (
        "no iteration-2 prompt carried fresh directions for the changed best_sql"
    )
    assert any(_REWRITE in ctx for ctx in fresh)
