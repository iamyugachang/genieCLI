"""Step A/B/C iteration pipeline — staged reasoning for the --direct loop.

Instead of one prompt that must read metrics, find the bottleneck, choose a
strategy, and emit SQL in a single shot, each iteration runs three focused
calls (map-reduce over evidence: large facts in, small conclusions forward):

  Step A (diagnose)   runtime stage hotspots + plan skeleton + table
                      landscape + this-vs-last metrics → one primary
                      bottleneck, with numbers cited
  Step B (strategize) current SQL + A's verdict + the P1–P9 fix menu +
                      ranked directions → ONE named strategy + hypothesis
  Step C (rewrite)    current SQL + B's hypothesis → complete candidate SQL
                      (runs on the loop's session history so revert feedback
                      and the lean-trim mechanism keep working)

Steps A and B are stateless one-shots; their outputs are recorded in the
iteration history and the report. Any step failing degrades gracefully — an
empty diagnosis/hypothesis simply reduces Step C to today's single-call
behavior. Enabled by default; set GENIE_STEPWISE_ITERATION=0 to fall back to
the legacy single-call loop.
"""
from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import Optional

MAX_RECORDED_CHARS = 400  # history/report cap for step outputs


def stepwise_enabled() -> bool:
    return os.environ.get("GENIE_STEPWISE_ITERATION", "1") != "0"


@dataclass
class IterationEvidence:
    """Everything the staged calls may cite. Empty blocks are omitted."""
    metric_key: str
    baseline_metric: float
    best_metric: float
    last_result_line: str = ""       # e.g. "improved (metric=9800.0, delta=-2700.0)"
    iteration: int = 1
    max_iterations: int = 1
    best_sql: str = ""
    hotspot_block: str = ""          # runtime stage hotspots (measured best run)
    landscape_block: str = ""        # SHOW STATS table landscape
    skeleton_block: str = ""         # current plan skeleton
    dup_subtree_note: str = ""       # repeated-subtree (inlined CTE) note
    static_block: str = ""           # sqlglot findings (iteration 1)
    directions_block: str = ""       # ranked directions + rule gate


@dataclass
class StepwiseResult:
    diagnosis: str = ""              # Step A raw output ("" = unavailable)
    strategy: str = ""               # Step B raw output ("" = unavailable)
    hypothesis: str = ""             # one-line hypothesis parsed from Step B
    rewrite_user_msg: str = ""       # composed user message for Step C
    calls_used: int = 0


def _complete_oneshot(provider, model, reasoning, system: str, user: str) -> str:
    """Stateless two-message completion; '' on any failure (fail-open)."""
    from genie.core.provider import CompletionRequest
    from genie.session.manager import new_msg
    try:
        req = CompletionRequest(
            messages=[new_msg("system", system), new_msg("user", user)],
            model=model, reasoning=reasoning,
        )
        return (provider.complete_text(req) or "").strip()
    except Exception:
        return ""


# ── Step A: diagnose ──────────────────────────────────────────────────────────

_DIAGNOSE_SYSTEM = """\
You are a Trino query performance analyst. You are given measured runtime
evidence for the CURRENT BEST version of a query inside an optimization loop.

Identify the PRIMARY bottleneck. Rules:
- Cite exact numbers from the evidence (stage ids, CPU shares, row counts,
  skew ratios, NDV values). No number, no claim.
- Runtime measurements outrank planner estimates when they disagree; a large
  estimate-vs-measured gap usually means stale or missing table stats.
- Do NOT propose SQL or rewrite strategies. Diagnosis only.

Answer in exactly this format:
BOTTLENECK: <one sentence — where and what>
EVIDENCE: <the specific numbers backing it>
SECONDARY: <next-biggest issue, or "none">
"""


def run_diagnose_step(provider, model, reasoning, ev: IterationEvidence) -> str:
    blocks = [
        f"Target metric: {ev.metric_key} (lower is better)\n"
        f"Baseline: {ev.baseline_metric}\n"
        f"Current best: {ev.best_metric}\n"
        f"Last iteration: {ev.last_result_line or 'N/A (first iteration)'}",
    ]
    for b in (ev.hotspot_block, ev.skeleton_block, ev.dup_subtree_note,
              ev.landscape_block):
        if b:
            blocks.append(b)
    if len(blocks) == 1:
        # No evidence beyond bare metrics — a diagnosis call would only
        # hallucinate structure. Skip; Step B runs from directions alone.
        return ""
    return _complete_oneshot(provider, model, reasoning,
                             _DIAGNOSE_SYSTEM, "\n\n".join(blocks))


# ── Step B: strategize ────────────────────────────────────────────────────────

_STRATEGIZE_SYSTEM = """\
You are a Trino SQL optimization strategist. Given a diagnosis and the current
SQL, choose the single next rewrite to attempt.

Rules:
- Choose exactly ONE strategy: a named P-strategy from the menu below, or
  "stats-refresh" (recommend ANALYZE; no rewrite possible), or "other" with a
  short name if no menu entry fits.
- DANGEROUS-tier strategies may not be chosen — they are advisory-only.
- Prefer the strategy that attacks the diagnosed PRIMARY bottleneck.
- The rewrite must preserve the exact result set; this loop is read-only
  (no CTAS / materialization statements).
- Do NOT write the SQL yet.

Answer in exactly this format:
STRATEGY: <P# name | stats-refresh | other:<name>>
TARGET: <table / join / fragment it applies to>
HYPOTHESIS: <one line, ≤80 chars — becomes the iteration history row>
RATIONALE: <≤2 sentences tying strategy to the diagnosis>
"""


def run_strategize_step(provider, model, reasoning, ev: IterationEvidence,
                        diagnosis: str) -> str:
    try:
        from genie.skills.mcp_trino.p_strategies import render_menu
        menu = render_menu()
    except Exception:
        menu = ""
    blocks = []
    if diagnosis:
        blocks.append(f"Diagnosis (from measured evidence):\n{diagnosis}")
    if ev.directions_block:
        blocks.append(ev.directions_block)
    if ev.static_block:
        blocks.append(ev.static_block)
    if menu:
        blocks.append(menu)
    blocks.append(f"Current SQL:\n```sql\n{ev.best_sql}\n```")
    return _complete_oneshot(provider, model, reasoning,
                             _STRATEGIZE_SYSTEM, "\n\n".join(blocks))


def parse_hypothesis(strategy_reply: str) -> str:
    """HYPOTHESIS line from Step B output; '' when absent."""
    for line in strategy_reply.splitlines():
        stripped = line.strip()
        if stripped.upper().startswith("HYPOTHESIS:"):
            return stripped.split(":", 1)[1].strip()
    return ""


# ── Step C: compose the rewrite request ───────────────────────────────────────

def compose_rewrite_message(ev: IterationEvidence, diagnosis: str,
                            strategy: str) -> str:
    """User message for the rewrite call (runs on the loop's session history).

    With A/B output present the message is lean — conclusions, not evidence.
    With both absent it degrades to the legacy single-call context so the
    loop never gets worse than the pre-pipeline behavior.
    """
    header = (
        f"[Trino Query Optimization — Iteration {ev.iteration}/{ev.max_iterations}]\n"
        f"Target metric: {ev.metric_key} (lower is better)\n"
        f"Baseline: {ev.baseline_metric}\n"
        f"Current best: {ev.best_metric}\n"
        f"Last iteration: {ev.last_result_line or 'N/A (first iteration)'}\n"
    )
    parts = [header]
    if diagnosis:
        parts.append(f"Diagnosis:\n{diagnosis}\n")
    if strategy:
        parts.append(f"Chosen strategy:\n{strategy}\n")
    # Per-step fallback: whatever a failed step would have digested goes to
    # the rewrite call directly, so a single transient step failure never
    # strips evidence the legacy single-call loop always supplied. With both
    # steps failed this degrades to exactly the legacy context.
    if not strategy:
        # Step B digests static findings + ranked directions.
        for b in (ev.static_block, ev.directions_block):
            if b:
                parts.append(b + "\n")
    if not diagnosis and ev.skeleton_block:
        # Step A digests the plan evidence.
        parts.append(ev.skeleton_block + "\n")
    parts.append(f"Current SQL:\n```sql\n{ev.best_sql}\n```\n")
    parts.append(
        "Apply the chosen strategy as ONE focused change. "
        "Return the COMPLETE optimized SQL in a ```sql block. "
        "Keep the EXACT same result set. Do NOT include a trailing semicolon."
    )
    return "\n".join(parts)


# ── Orchestration ─────────────────────────────────────────────────────────────

def run_stepwise_prelude(provider, model, reasoning, ev: IterationEvidence,
                         output=None) -> StepwiseResult:
    """Run Steps A and B and compose the Step C user message.

    The caller owns the Step C call (it lives on the loop's session history so
    revert feedback keeps flowing) plus all guards and measurement.
    """
    result = StepwiseResult()

    if output:
        output.progress("  [Step A] diagnosing measured evidence...")
    result.diagnosis = run_diagnose_step(provider, model, reasoning, ev)
    if result.diagnosis:
        result.calls_used += 1
        if output:
            first = result.diagnosis.splitlines()[0][:100]
            output.progress(f"  [Step A] {first}")
    elif output:
        output.progress("  [Step A] no runtime evidence available — skipped")

    if output:
        output.progress("  [Step B] choosing strategy...")
    result.strategy = run_strategize_step(provider, model, reasoning, ev,
                                          result.diagnosis)
    if result.strategy:
        result.calls_used += 1
        result.hypothesis = parse_hypothesis(result.strategy)
        if output and result.hypothesis:
            output.progress(f"  [Step B] {result.hypothesis[:100]}")

    result.rewrite_user_msg = compose_rewrite_message(
        ev, result.diagnosis, result.strategy)
    return result


def record_fields(result: StepwiseResult) -> dict:
    """Capped step outputs for the iteration-history entry / report."""
    fields = {}
    if result.diagnosis:
        fields["diagnosis"] = result.diagnosis[:MAX_RECORDED_CHARS]
    if result.strategy:
        fields["strategy"] = result.strategy[:MAX_RECORDED_CHARS]
    return fields
