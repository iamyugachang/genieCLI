"""trino-research — Autoresearch mode for SQL query optimization.

Architecture (v2 — 2026-04-03):
- AI returns COMPLETE SQL each iteration (no file_patch dependency)
- Verify measures median of N runs to reduce cache noise
- Row-count guard rejects semantically-wrong optimizations
- Supports both interactive and non-interactive (parameterized) entry

Usage in CLI:
  Interactive:
    /trino-research
  Non-interactive:
    /trino-research --file query.sql --metric cpu_time_ms --iterations 5 --runs 3
"""
from __future__ import annotations

import re
import statistics
import threading
import time
from pathlib import Path
from typing import Callable, Optional, NamedTuple

from genie.core.sql_extraction import extract_sql_from_reply
from genie.skills.mcp_trino.preflight import (
    CandidateTimeoutError, ExecutionPolicy, ReadOnlyViolationError,
    _assert_executable_read_only, _execution_sql_for, check_read_only,
    make_candidate_timeout_ms, validate_safe_limit,
)
from genie.skills.trino_query.connection import get_active_profile
from genie.skills.trino_query import QueryMetrics, _extract_metrics


# ---------------------------------------------------------------------------
# Measurement helpers (run in-process, no subprocess/verify.py needed)
# ---------------------------------------------------------------------------

DEFAULT_FETCH_BATCH_SIZE = 1_000


class _RowCapture(NamedTuple):
    observed_row_count: int
    rows: list
    captured_row_count: int
    capture_status: str
    completeness: str


def _validate_positive_non_bool_int(name: str, value: object) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ValueError(f"{name} must be a positive integer")
    return value


def _read_rows_bounded(cursor, *, capture_rows: bool, max_capture_rows: int,
                       batch_size: int = DEFAULT_FETCH_BATCH_SIZE) -> _RowCapture:
    """Drain a cursor with ``fetchmany`` and optionally retain a bounded prefix.

    Args and return contract: ``max_capture_rows`` and ``batch_size`` are positive
    non-bool integers and ``batch_size <= max_capture_rows``.  On successful EOF,
    the returned capture reports observed rows, retained rows, capture status, and
    completeness.

    For a compliant cursor:
        retained rows <= max_capture_rows
        current fetched batch <= batch_size
        batch_size <= max_capture_rows

        retained rows + one current batch
        <= max_capture_rows + batch_size
        <= 2 * max_capture_rows

    This is only a row-reference bound. It is not a byte-memory, row-payload-size,
    driver-prefetch/internal-buffer, oversized-batch-before-detection,
    network-transfer, EOF-drain-transfer, MCP-envelope, or Trino-server-resource
    bound.
    """
    cap = _validate_positive_non_bool_int("max_capture_rows", max_capture_rows)
    batch = _validate_positive_non_bool_int("batch_size", batch_size)
    if batch > cap:
        raise ValueError("batch_size must be less than or equal to max_capture_rows")

    observed = 0
    retained: list = []
    while True:
        rows = cursor.fetchmany(batch)
        if rows is None:
            rows = []
        if len(rows) > batch:
            raise RuntimeError("cursor returned more rows than requested fetchmany batch_size")
        if not rows:
            break
        observed += len(rows)
        if capture_rows and len(retained) < cap:
            retained.extend(rows[:cap - len(retained)])

    if not capture_rows:
        return _RowCapture(observed, [], 0, "not_captured", "not_captured")
    if observed > cap:
        return _RowCapture(observed, retained, len(retained), "truncated", "direct_truncated")
    return _RowCapture(observed, retained, len(retained), "complete", "verified_complete")


def _execute_sql_sync(sql: str, capture_rows: bool = False, *,
                      max_capture_rows: int = 100_000,
                      batch_size: int = DEFAULT_FETCH_BATCH_SIZE) -> tuple[int, QueryMetrics, list]:
    """Execute SQL on Trino, return (row_count, metrics, rows).

    When capture_rows=True, actual row data is returned for equivalence checks.
    """
    cfg = get_active_profile()
    conn = cfg.connect()
    cur = conn.cursor()
    try:
        cur.execute(sql)
        captured = _read_rows_bounded(
            cur, capture_rows=capture_rows, max_capture_rows=max_capture_rows,
            batch_size=batch_size,
        )
        stats = getattr(cur, "stats", {}) or {}
        metrics = _extract_metrics(stats)
        metrics.query_id = getattr(cur, "query_id", "") or ""
        return captured.observed_row_count, metrics, captured.rows
    finally:
        conn.close()


def _execute_sql(
    sql: str,
    capture_rows: bool = False,
    timeout_ms: Optional[float] = None,
    label: str = "candidate",
    *,
    max_capture_rows: int = 100_000,
    batch_size: int = DEFAULT_FETCH_BATCH_SIZE,
) -> tuple[int, QueryMetrics, list]:
    """Execute SQL with an optional wall-clock timeout.

    The Trino Python cursor exposes ``cancel()``, so candidate timeouts can
    stop the server-side query instead of waiting for the full driver request.
    """
    if timeout_ms is None or timeout_ms <= 0:
        return _execute_sql_sync(
            sql, capture_rows=capture_rows, max_capture_rows=max_capture_rows,
            batch_size=batch_size,
        )

    result: dict[str, tuple[int, QueryMetrics, list]] = {}
    error: dict[str, BaseException] = {}
    state: dict[str, object] = {}

    def runner() -> None:
        conn = None
        cur = None
        try:
            cfg = get_active_profile()
            conn = cfg.connect()
            state["conn"] = conn
            cur = conn.cursor()
            state["cur"] = cur
            cur.execute(sql)
            captured = _read_rows_bounded(
                cur, capture_rows=capture_rows, max_capture_rows=max_capture_rows,
                batch_size=batch_size,
            )
            stats = getattr(cur, "stats", {}) or {}
            metrics = _extract_metrics(stats)
            metrics.query_id = getattr(cur, "query_id", "") or ""
            result["value"] = (captured.observed_row_count, metrics, captured.rows)
        except BaseException as exc:
            error["exc"] = exc
        finally:
            if conn is not None:
                try:
                    conn.close()
                except Exception:
                    pass

    thread = threading.Thread(target=runner, daemon=True)
    thread.start()
    thread.join(timeout_ms / 1000.0)
    if thread.is_alive():
        cur = state.get("cur")
        if cur is not None and hasattr(cur, "cancel"):
            try:
                cur.cancel()
            except Exception:
                pass
        conn = state.get("conn")
        if conn is not None and hasattr(conn, "close"):
            try:
                conn.close()
            except Exception:
                pass
        thread.join(timeout=0.2)
        raise CandidateTimeoutError(timeout_ms, label)

    if "exc" in error:
        raise error["exc"]
    if "value" not in result:
        raise RuntimeError("Trino query finished without returning a result")
    return result["value"]


def _measure(
    sql: str,
    metric_key: str,
    runs: int,
    capture_rows: bool = False,
    output=None,
    label: str = "query",
    timeout_ms: Optional[float] = None,
    max_capture_rows: int = 100_000,
    batch_size: int = DEFAULT_FETCH_BATCH_SIZE,
) -> dict:
    """Run SQL `runs` times, return median metric + row_count + all samples.

    When capture_rows=True, the rows from the LAST run are included for
    equivalence checking.
    """
    samples = []
    all_metrics = []
    row_count = 0
    last_rows = []

    for i in range(runs):
        # Capture rows only on last run to avoid memory waste
        capture = capture_rows and (i == runs - 1)
        run_label = f"{label}: run {i + 1}/{runs}"
        if timeout_ms is not None:
            run_label = f"{run_label} limit={timeout_ms / 1000.0:.1f}s"
        if output and hasattr(output, "status"):
            with output.status(run_label):
                rc, m, rows = _execute_sql(
                    sql, capture_rows=capture,
                    timeout_ms=timeout_ms, label=label,
                    max_capture_rows=max_capture_rows, batch_size=batch_size,
                )
        else:
            rc, m, rows = _execute_sql(
                sql, capture_rows=capture,
                timeout_ms=timeout_ms, label=label,
                max_capture_rows=max_capture_rows, batch_size=batch_size,
            )
        row_count = rc
        if capture:
            last_rows = rows
        value = float(getattr(m, metric_key, 0) or 0)
        samples.append(value)
        all_metrics.append(m)

    median_val = statistics.median(samples)
    # Pick the run closest to median for full metrics display
    median_idx = min(range(len(samples)), key=lambda i: abs(samples[i] - median_val))

    if not capture_rows:
        capture_status, completeness = "not_captured", "not_captured"
    elif row_count > max_capture_rows:
        capture_status, completeness = "truncated", "direct_truncated"
    else:
        capture_status, completeness = "complete", "verified_complete"
    return {
        "median": median_val,
        "samples": samples,
        "row_count": row_count,  # compatibility alias for observed_row_count
        "observed_row_count": row_count,
        "rows": last_rows,
        "captured_row_count": len(last_rows),
        "max_capture_rows": max_capture_rows,
        "capture_status": capture_status,
        "completeness": completeness,
        "metrics": all_metrics[median_idx],
    }


def _measure_logical_sql(logical_sql: str, metric_key: str, runs: int, *,
                         policy: ExecutionPolicy, **kwargs) -> dict:
    """Enforce read-only logical SQL immediately before direct measurement."""
    _assert_executable_read_only(logical_sql)
    execution_sql = _execution_sql_for(logical_sql, policy)
    result = _measure(execution_sql, metric_key, runs, **kwargs)
    result.update({
        "logical_sql": logical_sql,
        "execution_sql": execution_sql,
        "safe_limit": policy.safe_limit,
    })
    return result


def _direct_measure_to_measure_result(measurement: dict):
    """Convert a direct measurement without upgrading absent provenance to proof."""
    from genie.skills.mcp_trino.research import MeasureResult
    rows = measurement.get("rows", [])
    median = measurement["median"]
    return MeasureResult(
        median_metric=median,
        samples=measurement.get("samples", [median]),
        row_count=measurement["row_count"],
        observed_row_count=measurement.get("observed_row_count", measurement["row_count"]),
        rows=rows,
        captured_row_count=measurement.get("captured_row_count", len(rows)),
        max_capture_rows=measurement.get("max_capture_rows", 100_000),
        capture_status=measurement.get("capture_status", "not_captured"),
        completeness=measurement.get("completeness", "not_captured"),
        columns=measurement.get("columns", []),
        metrics=measurement["metrics"],
    )


def _correctness_authorized(baseline: dict, candidate: dict) -> bool:
    """Authorize only explicitly proven complete direct captures.

    Missing provenance is incomplete provenance, not legacy-compatible proof.
    """
    return (
        baseline.get("capture_status") == candidate.get("capture_status") == "complete"
        and baseline.get("completeness") == candidate.get("completeness") == "verified_complete"
    )


def _incomplete_history(*, iteration: int, baseline: dict, candidate: dict,
                        candidate_sql: str, base_sql: str, metric: float,
                        delta: float) -> dict:
    """Build the sole persisted authorization-failure history representation."""
    return {
        "iteration": iteration,
        "status": "equivalence_unverified_incomplete_result",
        "rejection_reason": _incomplete_rejection_reason(baseline, candidate),
        "metric": metric,
        "delta": delta,
        "base_sql": base_sql,
        "candidate_sql": candidate_sql,
        "baseline_capture_status": baseline.get("capture_status"),
        "candidate_capture_status": candidate.get("capture_status"),
        "baseline_completeness": baseline.get("completeness"),
        "candidate_completeness": candidate.get("completeness"),
    }


def _incomplete_rejection_reason(baseline, candidate) -> str:
    """Return one of the complete, canonical incomplete-result reasons.

    A complete verified-direct side is deliberately classified separately so an
    incomplete peer gets its required baseline/candidate-specific reason.
    """
    def kind(item):
        if item.get("completeness") == "direct_truncated":
            return "direct_truncated"
        if item.get("capture_status") == "not_captured":
            return "capture_not_captured"
        if item.get("capture_status") == "truncated":
            return "capture_truncated"
        if item.get("completeness") == "unverified_received_envelope":
            return "upstream_completeness_unverified"
        if item.get("capture_status") == "complete" and item.get("completeness") == "verified_complete":
            return "verified"
        return "unknown"

    left, right = kind(baseline), kind(candidate)
    both = {
        "direct_truncated": "both_direct_truncated",
        "capture_not_captured": "both_captures_not_captured",
        "capture_truncated": "both_captures_truncated",
        "upstream_completeness_unverified": "both_upstream_completeness_unverified",
    }
    if left == right and left in both:
        return both[left]
    if right == "verified" and left in both:
        return f"baseline_{left}"
    if left == "verified" and right in both:
        return f"candidate_{right}"
    return "mixed_incomplete_result"


def _baseline_wall_ms(metrics) -> float:
    """Best available wall-clock duration from Trino metrics.

    Takes the LARGEST available numeric measure so the per-candidate kill-timeout
    basis never under-estimates baseline (the EXPLAIN-stage wall_time is often
    0/tiny). Non-numeric attribute values are treated as 0.
    """
    def _num(v) -> float:
        return float(v) if isinstance(v, (int, float)) and not isinstance(v, bool) else 0.0

    return float(
        max(
            _num(getattr(metrics, "query_time_ms", 0)),
            _num(getattr(metrics, "wall_time_ms", 0)),
            _num(getattr(metrics, "elapsed_time_ms", 0)),
        )
    )


def _normalize_row(row: tuple) -> tuple:
    """Normalize a row for comparison (handle float precision, None, etc).

    Floats normalize to 12 significant digits AND at most 6 decimal places:
    parallel aggregation over doubles is order-nondeterministic in its last
    ulps. The significant-digit cap absorbs that noise at large magnitudes
    (e.g. 1e10-scale sums, where a fixed 6-decimal round demands ~17
    significant digits — beyond double precision itself); the decimal-place
    cap keeps the historical ~5e-7 absolute tolerance for small-magnitude
    values (rates, ratios), where 12 significant digits alone would flag
    run-to-run noise as semantic drift.
    """
    result = []
    for val in row:
        if isinstance(val, float):
            result.append(round(float(f"{val:.12g}"), 6))
        else:
            result.append(val)
    return tuple(result)


def _multiset_key(val) -> str:
    """Canonical key for the order-insensitive (multiset) compare.

    Values that compare equal under == must map to the same key — mirroring
    the positional branch's tuple-== semantics: Decimal('1.5') vs
    Decimal('1.50'), 1 vs 1.0, 0.0 vs -0.0 all count as the same value.
    Numbers canonicalize through Fraction (exact, cross-type); everything
    else falls back to repr, which keeps unhashable values (arrays/maps)
    countable and preserves the 1 vs '1' type distinction.
    """
    from decimal import Decimal
    from fractions import Fraction
    if isinstance(val, (int, float, Decimal)):
        # bool included deliberately: True == 1 under ==, so the positional
        # branch equates them too.
        try:
            return str(Fraction(val))
        except (ValueError, OverflowError):  # NaN / inf
            return repr(val)
    return repr(val)


def _has_top_level_order_by(sql: str) -> bool:
    """True when the statement's outermost query carries an ORDER BY.

    Decides whether result comparison must be positional. Fail-closed: any
    parse failure → True (strict positional compare — the conservative gate).
    """
    try:
        import sqlglot
        stmt = sqlglot.parse_one(sql, read="trino")
        if stmt is None:
            return True
        return stmt.args.get("order") is not None
    except Exception:
        return True


def _results_equivalent(rows_a: list, rows_b: list, *,
                        ordered: bool = True) -> tuple[bool, str]:
    """Check if two result sets are equivalent.

    ordered=True → same rows in the same order (queries with a top-level
    ORDER BY, where order is part of the semantics).
    ordered=False → same multiset of rows. SQL without a top-level ORDER BY
    has no guaranteed result order, so a positional compare would misreport
    equivalent candidates as semantic drift whenever the cluster returns the
    same groups in a different order.

    Returns (equivalent, reason).
    """
    if len(rows_a) != len(rows_b):
        return False, f"row count differs: {len(rows_a)} vs {len(rows_b)}"

    if not rows_a:
        return True, "both empty"

    # Compare column count
    if len(rows_a[0]) != len(rows_b[0]):
        return False, f"column count differs: {len(rows_a[0])} vs {len(rows_b[0])}"

    if not ordered:
        from collections import Counter
        # _multiset_key canonicalizes ==-equal values to the same key (so this
        # branch never rejects rows the positional branch would accept) while
        # keeping unhashable values (arrays/maps) countable via repr.
        ca = Counter(tuple(_multiset_key(v) for v in _normalize_row(r)) for r in rows_a)
        cb = Counter(tuple(_multiset_key(v) for v in _normalize_row(r)) for r in rows_b)
        if ca == cb:
            return True, "exact match (order-insensitive)"
        only_a = ca - cb
        mismatches = sum(only_a.values())
        sample = next(iter(only_a))
        return False, (
            f"{mismatches} row(s) differ (order-insensitive); "
            f"e.g. baseline-only row: {sample}"
        )

    # Normalize and compare row by row
    mismatches = 0
    first_mismatch = None
    for i, (a, b) in enumerate(zip(rows_a, rows_b)):
        na = _normalize_row(a)
        nb = _normalize_row(b)
        if na != nb:
            mismatches += 1
            if first_mismatch is None:
                first_mismatch = f"row {i}: {na} vs {nb}"

    if mismatches == 0:
        return True, "exact match"

    return False, f"{mismatches} row(s) differ; first: {first_mismatch}"


def _lint_sql(sql: str) -> tuple[bool, str]:
    """Lint SQL, return (passed, message). F with parse error = fail."""
    try:
        from genie.core.lint_analyzer import analyze
        result = analyze(sql)
        if result.score == "F" and result.parse_error:
            return False, f"parse error: {result.parse_error}"
        return True, f"lint score={result.score}"
    except Exception as e:
        return False, f"lint error: {e}"


def _format_static_findings(report) -> str:
    """Render a sql_static report as a compact bullet list for prompt injection."""
    if report is None or not report.findings:
        return ""
    lines = []
    for f in report.findings:
        lines.append(f"  - [{f.severity}] {f.rule_id} (line {f.line}): {f.message}")
        lines.append(f"      → {f.suggestion}")
    return "\n".join(lines)


def _no_data_report(
    *,
    sql: str,
    reason: str,
    static_report,
    llm_finishing: Optional[str],
    model: str,
    optimized_sql: Optional[str] = None,
) -> str:
    """Render the no-data path report (sticky warning + L1 findings + L3 finishing)."""
    from datetime import datetime

    reason_human = {
        "table_not_found": "referenced table/schema/catalog does not exist",
        "empty_result": "query ran but returned 0 rows",
    }.get(reason, reason)

    lines = [
        "# Trino Query Static Analysis Report (no-data path)",
        "",
        f"**Date:** {datetime.now().strftime('%Y-%m-%d %H:%M')}",
        f"**Model:** {model}",
        f"**Mode:** static — iteration loop skipped because **{reason_human}**",
        "",
        "## Why this report instead of an iteration run",
        "",
        f"- {reason_human}",
        "- Without measurable rows there is nothing to optimize empirically.",
        "- Static analysis still surfaces query-shape issues that hold regardless of data.",
        "",
        "## Original SQL",
        "",
        "```sql",
        sql.rstrip(),
        "```",
        "",
    ]

    # Dedicated, copy-paste-ready optimized SQL (advisory — extracted from the LLM
    # finishing pass). Surfaced as its own block so the user does not have to dig the
    # rewrite out of the prose below.
    if optimized_sql:
        lines += [
            "## Optimized SQL (advisory — UNVERIFIED, no data to validate)",
            "",
            "Static-analysis-driven rewrite. It was **not** executed, EXPLAINed, "
            "benchmarked, or row-equivalence verified (the table is missing/empty). "
            "Review it, then re-run `/trino-research` once real data is available.",
            "",
            "```sql",
            optimized_sql.rstrip(),
            "```",
            "",
        ]

    lines += [
        "## Static analysis findings",
        "",
    ]

    if static_report is None or static_report.parse_error:
        err = (static_report.parse_error if static_report else "analyzer unavailable")
        lines += [f"_Parse failed: {err}_", ""]
    elif not static_report.findings:
        lines += [
            "_No structural issues detected._",
            "",
            "If the query is correct, the next step is to confirm the table name / "
            "catalog / schema, or re-check the partition filter.",
            "",
        ]
    else:
        lines += [f"**Summary:** {static_report.summary}", ""]
        lines += ["| # | Severity | Rule | Line | Message | Suggestion |",
                  "|---|---|---|---|---|---|"]
        for i, f in enumerate(static_report.findings, 1):
            msg = f.message.replace("|", "\\|")
            sug = f.suggestion.replace("|", "\\|")
            lines.append(f"| {i} | {f.severity} | {f.rule_id} | {f.line} | {msg} | {sug} |")
        lines.append("")

    if llm_finishing:
        lines += ["## LLM finishing pass", "", llm_finishing.rstrip(), ""]

    lines += ["## Next steps", "",
              "1. Verify the referenced tables exist and contain data.",
              "2. Apply the highest-severity findings above.",
              "3. Re-run `/trino-research` against the corrected query.", ""]
    return "\n".join(lines)




# ---------------------------------------------------------------------------
# No-data path (v28 T9 + T10)
# ---------------------------------------------------------------------------

def _run_no_data_path(
    *,
    provider,
    model: str,
    reasoning: str,
    original_sql: str,
    no_data_reason: str,
    static_report,
    baseline_exc: Optional[BaseException],
    output,
    step_trace=None,
) -> dict:
    """Single-call static analysis + optional LLM finishing — no iteration loop.

    Triggered when the baseline either raised a table-not-found-shaped error
    or returned 0 rows. Cost: ≤1 LLM call vs N for the iteration path.
    """
    reason_human = {
        "table_not_found": "table/schema/catalog not found",
        "empty_result": "baseline returned 0 rows",
    }.get(no_data_reason, no_data_reason)

    output.print("")
    output.error(f"  [no-data] {reason_human} — switching to static analysis mode")
    if baseline_exc is not None:
        output.print(f"  [dim]baseline error: {baseline_exc}[/dim]")

    if static_report is None:
        output.error("  Static analyzer unavailable — emitting bare report")
    elif static_report.parse_error:
        output.error(f"  Static parse failed: {static_report.parse_error}")
    else:
        output.progress(f"  Static analysis: {static_report.summary}")
        for f in static_report.findings:
            output.print(f"    [{f.severity[0].upper()}] {f.rule_id}: {f.message}")

    # ── v48 T7: Advisory decompose (run_static_gates=True, no live execution) ──
    # Guard: disabled by GENIE_V48_SEED_DECOMPOSE=0 for test/debugging isolation.
    import os as _os_v48_nd
    _v48_nd_seed_enabled = _os_v48_nd.environ.get("GENIE_V48_SEED_DECOMPOSE", "1") != "0"

    # v58: fragment rewrite opt-in (mirroring MCP path env vars)
    _v58_nd_frag_rewrite = _os_v48_nd.environ.get("GENIE_FRAGMENT_REWRITE", "0") == "1"
    _v58_nd_frag_cap = max(1, int(_os_v48_nd.environ.get("GENIE_FRAGMENT_REWRITE_CAP", "5")))

    from genie.output.step_trace import StepTrace as _ND_StepTrace
    _nd_trace: _ND_StepTrace = step_trace if step_trace is not None else []
    _nd_advisory_sql = original_sql
    if _v48_nd_seed_enabled and provider is not None:
        try:
            from genie.skills.mcp_trino.write_analysis import _make_advisory_llm_fn as _nd_llm_fn_factory
            from genie.skills.mcp_trino.write_analysis import _advisory_cost_reader as _nd_cost_reader
            from genie.skills.mcp_trino.research import _produce_decompose_candidate as _nd_decompose
            _nd_llm_fn = _nd_llm_fn_factory(provider, model, reasoning)
            _nd_recomposed, _nd_frags, _nd_cands, _nd_rr = _nd_decompose(
                original_sql, _nd_llm_fn, _nd_cost_reader,
                run_static_gates=True, step_trace=_nd_trace,
                enable_fragment_rewrite=_v58_nd_frag_rewrite,
                max_fragment_model_calls=_v58_nd_frag_cap,
            )
            if _nd_recomposed != original_sql:
                _nd_advisory_sql = _nd_recomposed
                if output:
                    output.progress("  Advisory decompose→recompose produced candidate (ADVISORY — UNVERIFIED)")
        except Exception as _nd_exc:
            if output:
                output.progress(f"  [warn] advisory decompose failed (degraded): {_nd_exc}")

    # Finishing pass: ask the model to synthesise an optimized rewrite. Single
    # call — no iteration, no measurement. In the no-data path genieCLI acts as a
    # pure static optimizer, so this runs whenever a provider exists — NOT only
    # when the static analyzer happened to flag findings. A clean query still
    # deserves an advisory rewrite (or an explicit "already well-shaped" verdict)
    # and the live spinner that comes with it.
    llm_finishing: Optional[str] = None
    has_findings = bool(static_report and static_report.findings)
    if provider is not None:
        try:
            from genie.core.provider import CompletionRequest
            from genie.session.manager import new_msg

            if has_findings:
                findings_text = _format_static_findings(static_report)
                sys_prompt = (
                    "You are a Trino SQL reviewer. The user's query could not be benchmarked "
                    "(table missing or empty), but a static analyzer found structural issues. "
                    "Combine the findings below with the SQL and write a concise rewrite "
                    "recommendation. Return: (1) a one-paragraph diagnosis, (2) a single "
                    "rewritten SQL block, (3) a short list of any further checks the user "
                    "should perform before re-running."
                )
                user_prompt = (
                    f"Original SQL:\n```sql\n{original_sql.rstrip()}\n```\n\n"
                    f"Static findings:\n{findings_text}\n\n"
                    f"Reason for no-data path: {reason_human}."
                )
            else:
                # No static findings — the optimizer still reviews the SQL shape
                # directly (join order, predicate pushdown, CTE materialization,
                # projection pruning) so the user always gets a copy-paste rewrite.
                sys_prompt = (
                    "You are a Trino SQL reviewer. The user's query could not be benchmarked "
                    "(table missing or empty) and a static analyzer found no structural issues. "
                    "Review the SQL directly for Trino-specific optimization opportunities "
                    "(join order, predicate/filter pushdown, CTE materialization vs re-scan, "
                    "projection pruning, partition-filter hints). Return: (1) a one-paragraph "
                    "diagnosis, (2) a single rewritten SQL block — if the query is already "
                    "well-shaped, return it unchanged and say so explicitly, (3) a short list of "
                    "any further checks the user should perform before re-running."
                )
                user_prompt = (
                    f"Original SQL:\n```sql\n{original_sql.rstrip()}\n```\n\n"
                    f"Reason for no-data path: {reason_human}.\n"
                    "No static findings were reported."
                )

            req = CompletionRequest(
                messages=[new_msg("system", sys_prompt), new_msg("user", user_prompt)],
                model=model,
                reasoning=reasoning,
            )
            with output.status("Analyzing and drafting an optimized rewrite (LLM, this can take a while)..."):
                llm_finishing = provider.complete_text(req)
        except Exception as exc:
            output.progress(f"  [warn] LLM finishing pass failed: {exc}")
            llm_finishing = None

    # Pull the rewrite out of the LLM finishing prose into a clean, copy-paste-ready
    # block (the LLM bundles diagnosis + SQL + checks together).
    optimized_sql: Optional[str] = None
    if llm_finishing:
        from genie.core.sql_extraction import extract_sql_from_reply
        extracted = extract_sql_from_reply(str(llm_finishing))
        if extracted and extracted.strip() and extracted.strip() != original_sql.strip():
            optimized_sql = extracted
            output.progress("  Advisory optimized SQL extracted (unverified — no data)")

    # v48 T7: prefer advisory decompose SQL if LLM finishing pass didn't produce one
    if optimized_sql is None and _nd_advisory_sql != original_sql:
        optimized_sql = _nd_advisory_sql

    report_md = _no_data_report(
        sql=original_sql,
        reason=no_data_reason,
        static_report=static_report,
        llm_finishing=llm_finishing,
        model=model,
        optimized_sql=optimized_sql,
    )

    # v48 T8: splice step trace into no-data report
    if _nd_trace:
        from genie.output.step_trace import render_report as _render_step_report
        step_section = _render_step_report(_nd_trace)
        if step_section.strip():
            report_md = report_md + "\n\n## Step Trace\n\n" + step_section + "\n"

    return {
        "status": "no_data",
        "reason": no_data_reason,
        "baseline_error": str(baseline_exc) if baseline_exc else None,
        "original_sql": original_sql,
        "best_sql": optimized_sql or original_sql,
        "advisory_optimized_sql": optimized_sql,
        "static_findings": (
            [
                {
                    "severity": f.severity,
                    "rule_id": f.rule_id,
                    "message": f.message,
                    "suggestion": f.suggestion,
                    "line": f.line,
                }
                for f in static_report.findings
            ]
            if static_report else []
        ),
        "llm_finishing": llm_finishing,
        "report_markdown": report_md,
        "step_trace": _nd_trace,  # v48
    }


# ---------------------------------------------------------------------------
# Long-query plan-cost loop (v28 T4)
# ---------------------------------------------------------------------------

def _run_plan_cost_loop(
    *,
    provider,
    model: str,
    reasoning: str,
    original_sql: str,
    metric_key: str,
    max_iterations: int,
    verify_runs: int,
    output,
    build_prompt: Callable[..., str],
    baseline: dict,
    baseline_data: list,
    static_report,
    explain_runner: Callable[[str], Optional[str]],
    max_fallbacks: int,
    candidate_timeout_ms: Optional[float] = None,
    execution_policy: ExecutionPolicy | None = None,
) -> dict:
    """Plan-cost ranking + L1 structural guard + K-retry on row-equivalence.

    Delegates the iteration + verification loop to _plan_cost_loop_core (shared
    with the MCP path). This adapter is responsible for:
    - baseline metric / rows extraction (dict fields)
    - candidate_timeout_ms derivation from _baseline_wall_ms
    - plan_cost call; directions assembled via pre_execution_diagnosis (includes join diagnosis)
    - building sys_prompt (rule_gate_block only, no directions_block)
    - wrapping output in _SafeOutput
    - building the four injected callables (measure_fn, metric_fn, row_equiv_fn, explain_runner)
    - reconstructing the three-case return dict from _PlanCostCoreResult fields
    """
    from genie.skills.mcp_trino.pre_execution_diagnosis import pre_execution_diagnosis
    from genie.skills.mcp_trino.preflight import (
        _SafeOutput,
        _plan_cost_loop_core,
        plan_cost,
        _combine_cost,
    )
    from genie.skills.mcp_trino.rule_gate import (
        build_rule_gate_summary,
        format_rule_gate_for_prompt,
        render_rule_gate_summary,
    )
    from genie.skills.trino_query.plan_signature import plan_signature

    policy = execution_policy or ExecutionPolicy(None)
    _assert_executable_read_only(original_sql)
    baseline_metric = baseline["median"]
    baseline_rows = baseline["row_count"]
    if candidate_timeout_ms is None:
        baseline_wall_ms = _baseline_wall_ms(baseline["metrics"])
        candidate_timeout_ms = make_candidate_timeout_ms(baseline_wall_ms) if baseline_wall_ms > 0 else None

    # Baseline plan cost + signature
    baseline_rows_est, baseline_bytes_est, baseline_plan = plan_cost(
        original_sql, explain_runner
    )
    baseline_sig = plan_signature(baseline_plan) if baseline_plan is not None else None
    baseline_cost = _combine_cost(baseline_rows_est, baseline_bytes_est)
    directions = pre_execution_diagnosis(
        original_sql,
        static_report=static_report,
        explain_cost=(baseline_rows_est, baseline_bytes_est, baseline_plan),
        table_metadata=None,
        # D4: direct path omits peak_memory_limit_bytes → default None preserves identical behavior
        peak_memory_bytes=getattr(baseline.get("metrics"), "peak_memory_bytes", 0) or None,
    )
    rule_gate = build_rule_gate_summary(static_report, directions)
    rule_gate_block = format_rule_gate_for_prompt(rule_gate)

    output.print("")
    timeout_text = (
        f", candidate_timeout={candidate_timeout_ms / 1000.0:.1f}s"
        if candidate_timeout_ms is not None else ""
    )
    output.progress(
        f"  [long-query] Plan-cost loop active "
        f"(baseline rows~{baseline_rows_est}, bytes~{baseline_bytes_est}, "
        f"max_fallbacks={max_fallbacks}{timeout_text})"
    )
    render_rule_gate_summary(output, rule_gate)

    # Condensed skeleton of the already-fetched baseline plan (no extra
    # EXPLAIN round-trip). Fail-open ("" → not injected).
    from genie.skills.mcp_trino.research import _format_plan_skeleton_block
    from genie.skills.trino_query.plan_render import render_plan_skeleton
    skeleton_block = _format_plan_skeleton_block(
        render_plan_skeleton(baseline_plan), label="Baseline plan skeleton"
    )

    # Session setup — same prompt structure as the legacy loop
    # (direct path: rule_gate_block only, no directions_block)
    skill_prompt = build_prompt(True, model)
    sys_prompt = (
        f"You are optimizing a Trino SQL query for performance.\n"
        f"Target metric: {metric_key} (lower is better).\n\n"
        f"Rules:\n"
        f"- Return the COMPLETE optimized SQL in a ```sql code block\n"
        f"- Do NOT use file_patch or any tool calls\n"
        f"- Keep the EXACT same result set — same columns, same rows, same values\n"
        f"- Make ONE focused change per iteration\n"
        f"- Trino best practices: partition filters, named columns, predicate pushdown, "
        f"projection pruning, APPROX_DISTINCT over COUNT(DISTINCT), COALESCE instead of NVL\n"
        f"- Treat CTE step materialization as advisory only; keep this loop read-only\n\n"
        f"{(rule_gate_block + chr(10) + chr(10)) if rule_gate_block else ''}"
        f"{(skeleton_block + chr(10) + chr(10)) if skeleton_block else ''}"
        f"{skill_prompt}"
    )

    # ── Direct adapter closures (§2.2 reconstruction) ──
    measure_fn = lambda sql, label: _measure_logical_sql(
        sql, metric_key, verify_runs, policy=policy,
        capture_rows=True, output=output, label=label,
        timeout_ms=candidate_timeout_ms,
    )
    metric_fn = lambda m: m["median"]
    # Without a top-level ORDER BY, result order is not part of the query's
    # semantics — compare row multisets, not positions.
    _rows_ordered = _has_top_level_order_by(original_sql)
    # Do not let a plan-cost candidate become a winner from partial rows.
    def row_equiv_fn(measured):
        if not _correctness_authorized(baseline, measured):
            return False, _incomplete_rejection_reason(baseline, measured)
        return _results_equivalent(baseline_data, measured["rows"],
                                   ordered=_rows_ordered)
    # Single-emission empty-branch message; core emits this via empty_message param.
    _DIRECT_EMPTY_MSG = "  [verify] No candidate beats baseline plan cost — emitting no_verifiable_improvement"

    # ── v48 T6: Decompose-seed validation for --direct plan-cost loop ──
    # Guard: disabled by GENIE_V48_SEED_DECOMPOSE=0 for test/debugging isolation.
    import os as _os_v48_dpcl
    _v48_dpcl_seed_enabled = _os_v48_dpcl.environ.get("GENIE_V48_SEED_DECOMPOSE", "1") != "0"

    # v58: fragment rewrite opt-in (mirroring MCP plan-cost path env vars)
    _v58_dpcl_frag_rewrite = _os_v48_dpcl.environ.get("GENIE_FRAGMENT_REWRITE", "0") == "1"
    _v58_dpcl_frag_cap = max(1, int(_os_v48_dpcl.environ.get("GENIE_FRAGMENT_REWRITE_CAP", "5")))

    from genie.output.step_trace import StepTrace as _DPCL_StepTrace
    _dpcl_step_trace: _DPCL_StepTrace = []
    # Seed verification occurs before the ranked L3 phase, so retain its
    # canonical authorization rejection and merge it into returned history.
    _dpcl_seed_rejection_history: list[dict] = []
    _dpcl_seed_sql = original_sql
    if _v48_dpcl_seed_enabled:
        try:
            from genie.skills.mcp_trino.write_analysis import _make_advisory_llm_fn as _dpcl_llm_fn_factory
            from genie.skills.mcp_trino.write_analysis import _advisory_cost_reader as _dpcl_cost_reader
            from genie.skills.mcp_trino.research import _produce_decompose_candidate as _dpcl_decompose
            if provider is not None:
                _dpcl_llm_fn = _dpcl_llm_fn_factory(provider, model, reasoning)
                _dpcl_recomposed, _dpcl_frags, _dpcl_cands, _dpcl_rr = _dpcl_decompose(
                    original_sql, _dpcl_llm_fn, _dpcl_cost_reader,
                    run_static_gates=False, step_trace=_dpcl_step_trace,
                    enable_fragment_rewrite=_v58_dpcl_frag_rewrite,
                    max_fragment_model_calls=_v58_dpcl_frag_cap,
                )
                if _dpcl_recomposed != original_sql:
                    _dpcl_seed_meas = _measure_logical_sql(
                        _dpcl_recomposed, metric_key, verify_runs, policy=policy,
                        capture_rows=True, output=output, label="seed",
                        timeout_ms=candidate_timeout_ms,
                    )
                    _dpcl_seed_authorized = _correctness_authorized(baseline, _dpcl_seed_meas)
                    if not _dpcl_seed_authorized:
                        # The decompose seed is not a ranked L3 candidate, but it
                        # is still a correctness authorization attempt. Persist the
                        # exact canonical record once and keep the coupled baseline.
                        _dpcl_seed_rejection_history.append(_incomplete_history(
                            iteration=0, baseline=baseline, candidate=_dpcl_seed_meas,
                            base_sql=original_sql, candidate_sql=_dpcl_recomposed,
                            metric=_dpcl_seed_meas["median"],
                            delta=_dpcl_seed_meas["median"] - baseline_metric,
                        ))
                    _dpcl_seed_equiv = (
                        _dpcl_seed_authorized
                        and _dpcl_seed_meas["row_count"] == baseline_rows
                        and _results_equivalent(baseline_data, _dpcl_seed_meas["rows"],
                                                ordered=_rows_ordered)[0]
                    )
                    if _dpcl_seed_equiv and _dpcl_seed_meas["median"] < baseline_metric:
                        _dpcl_seed_sql = _dpcl_recomposed
                        if output:
                            output.progress(
                                f"  [seed] decompose→recompose accepted (plan-cost loop): "
                                f"{baseline_metric:.1f} → {_dpcl_seed_meas['median']:.1f}"
                            )
        except Exception as _dpcl_seed_exc:
            if output:
                output.progress(f"  [seed] decompose failed in direct plan-cost loop (degraded): {_dpcl_seed_exc}")

    def incomplete_history_fn(measured, ranked):
        if _correctness_authorized(baseline, measured):
            return None
        return _incomplete_history(
            iteration=ranked["iteration"], baseline=baseline, candidate=measured,
            base_sql=original_sql, candidate_sql=ranked["sql"],
            metric=measured["median"],
            delta=measured["median"] - baseline_metric,
        )

    result = _plan_cost_loop_core(
        provider=provider,
        model=model,
        reasoning=reasoning,
        sys_prompt=sys_prompt,
        original_sql=_dpcl_seed_sql,
        metric_key=metric_key,
        max_iterations=max_iterations,
        max_fallbacks=max_fallbacks,
        baseline_cost=baseline_cost,
        baseline_sig=baseline_sig,
        baseline_plan=baseline_plan,
        baseline_rows_est=baseline_rows_est,
        baseline_bytes_est=baseline_bytes_est,
        explain_runner=explain_runner,
        measure_fn=measure_fn,
        metric_fn=metric_fn,
        row_equiv_fn=row_equiv_fn,
        static_report=static_report,
        output=_SafeOutput(output),
        candidate_timeout_ms=candidate_timeout_ms,
        empty_message=_DIRECT_EMPTY_MSG,
        incomplete_history_fn=incomplete_history_fn,
    )

    # Persist a seed authorization failure alongside core L3 history.  Inserting
    # once here preserves it for every return shape without affecting ranking.
    if _dpcl_seed_rejection_history:
        result.history[:0] = _dpcl_seed_rejection_history

    # ── Three-case return reconstruction (§3.1 / §3.2 / §3.3) ──

    if result.surviving_better_was_empty:
        # Case A: no candidates beat baseline plan cost (13 keys; NO verify_log, NO fallbacks_used)
        return {
            "status": "no_verifiable_improvement",
            "baseline_metric": baseline_metric,
            "best_metric": baseline_metric,
            "total_improvement": 0.0,
            "improvement_pct": 0.0,
            "iterations": len(result.history),
            "kept": 0,
            "baseline_rows": baseline_rows,
            "baseline_plan_cost": result.baseline_cost,
            "original_sql": original_sql,
            "best_sql": original_sql,
            "history": result.history,
            "candidates_evaluated": result.candidates_evaluated,
            # NO "verify_log"     <-- T-S1-K1
            # NO "fallbacks_used" <-- T-S1-K1
        }

    if result.winner_sql is None:
        # Case B: non-empty surviving_better but all L3 candidates failed (14 keys; verify_log present, NO fallbacks_used)
        return {
            "status": "no_verifiable_improvement",
            "baseline_metric": baseline_metric,
            "best_metric": baseline_metric,
            "total_improvement": 0.0,
            "improvement_pct": 0.0,
            "iterations": len(result.history),
            "kept": 0,
            "baseline_rows": baseline_rows,
            "baseline_plan_cost": result.baseline_cost,
            "original_sql": original_sql,
            "best_sql": original_sql,
            "history": result.history,
            "candidates_evaluated": result.candidates_evaluated,
            "verify_log": result.verify_log,    # PRESENT in Case B  <-- T-S1-K2
            # still NO "fallbacks_used"          <-- T-S1-K2
        }

    # Case C: winner found
    # CRITICAL (ILG[11]): do NOT use {**h, 'status': 'improved'}.
    # result.history entries have 4 keys: {iteration, status, candidate_sql, plan_cost}.
    # _generate_report reads h['metric'], h['hypothesis'], h['delta'], h['base_sql']:
    # all absent from the 4-key history entries — must build 7-key dicts explicitly.
    winner_metric = result.winner_measure["median"]   # direct path: measure is a dict
    winner_history = [
        # Seed/L3 authorization rejections already have a canonical persisted
        # shape. Preserve it verbatim even when a later ranked candidate wins.
        h if h.get("status") == "equivalence_unverified_incomplete_result" else {
            "iteration": h["iteration"],
            "status": "improved" if (h["candidate_sql"] == result.winner_sql) else h["status"],
            # Ranking-only entries were never measured.  Do not turn their
            # absent measurements into baseline/zero facts during report-shape
            # reconstruction; formatters intentionally render these as n/a.
            "metric": winner_metric if (h["candidate_sql"] == result.winner_sql) else h.get("metric"),
            "delta": winner_metric - baseline_metric if (h["candidate_sql"] == result.winner_sql) else h.get("delta"),
            "hypothesis": "(plan-cost-loop)",
            "base_sql": original_sql,
            "candidate_sql": h.get("candidate_sql"),
        }
        for h in result.history
    ]
    return {
        "status": "completed",
        "mode": "plan_cost",
        "baseline_metric": baseline_metric,
        "best_metric": winner_metric,
        "total_improvement": winner_metric - baseline_metric,
        "improvement_pct": (
            (winner_metric - baseline_metric) / baseline_metric * 100
            if baseline_metric else 0
        ),
        "iterations": len(result.history),
        "kept": 1,
        "baseline_rows": baseline_rows,
        "baseline_plan_cost": result.baseline_cost,
        "winner_plan_cost": result.winner_ranked["plan_cost"],  # EXPLAIN cost, NOT from measure dict  <-- T-S1-WPC
        "original_sql": original_sql,
        "best_sql": result.winner_sql,
        "history": winner_history,
        "verify_log": result.verify_log,
        "candidates_evaluated": result.candidates_evaluated,
        "fallbacks_used": result.fallbacks_used,               # PRESENT in Case C  <-- T-S1-K3
    }


# ---------------------------------------------------------------------------
# Direct-path table metadata fetcher (F1/F2: v46 dual-path parity)
# ---------------------------------------------------------------------------

def _execute_direct_as_dicts(sql: str) -> list:
    """Execute *sql* on the direct Trino connection; return rows as list[dict].

    Uses the active profile's cursor; builds dicts from cursor.description
    (DB-API 2 column names) and fetchall() tuples.  Returns [] on any error
    (fail-open — no propagation to caller).  Never raises.
    """
    try:
        profile = get_active_profile()
        conn = profile.connect()
    except Exception:
        return []
    try:
        cur = conn.cursor()
        cur.execute(sql)
        description = cur.description or []
        if not description:
            return []
        col_names = [col[0] for col in description]
        rows = cur.fetchall() or []
        return [dict(zip(col_names, row)) for row in rows]
    except Exception:
        return []
    finally:
        # fail-open must not leak the connection when execute/fetch raises
        try:
            conn.close()
        except Exception:
            pass


def _fetch_table_metadata_direct(sql: str) -> list:
    """Fetch table metadata for qualified tables in *sql* via the --direct connection.

    Extracts catalog.schema.table triples from *sql*, then calls
    ``_fetch_table_metadata_from_runner`` with ``_execute_direct_as_dicts`` as the
    execute_fn.  Returns [] when no qualified tables are found or on any error.
    Duck-type compatible with ``_fetch_table_metadata`` (returns list[TableMetadata]).
    """
    from genie.skills.mcp_trino.research import (
        _extract_table_names,
        _fetch_table_metadata_from_runner,
    )

    try:
        tables = _extract_table_names(sql)
        qualified = [(c, s, t) for (c, s, t) in tables if c and s]
        if not qualified:
            return []
        return _fetch_table_metadata_from_runner(qualified, _execute_direct_as_dicts)
    except Exception:
        return []


# ---------------------------------------------------------------------------
# Core iteration loop (no RunManager / file_patch / git dependency)
# ---------------------------------------------------------------------------

def _render_direct_plan_skeleton(
    explain_runner: Optional[Callable[[str], Optional[str]]], sql: str
) -> str:
    """Condensed EXPLAIN (FORMAT JSON) skeleton for the --direct path.

    Planner-only round-trip through the injected explain_runner. Fail-open:
    returns "" (no runner / EXPLAIN failure / unusable plan) so callers gate
    prompt injection on truthiness.
    """
    from genie.skills.mcp_trino.preflight import plan_cost as _plan_cost
    from genie.skills.trino_query.plan_render import render_plan_skeleton

    if explain_runner is None:
        return ""
    try:
        _, _, plan = _plan_cost(sql, explain_runner)
        return render_plan_skeleton(plan)
    except Exception:
        return ""


def _assemble_direct_directions(
    original_sql: str,
    static_report,
    explain_runner: Optional[Callable[[str], Optional[str]]],
    *,
    peak_memory_bytes: Optional[int] = None,
    table_metadata=None,
    explain_cost=None,
):
    """Assemble ranked optimization directions for the --direct path.

    Mirrors the MCP path's ``_assemble_mcp_directions``.  Returns a 2-tuple
    ``(directions, pre_table_metadata)`` — same shape as the MCP assembler —
    so callers can unpack symmetrically.

    F2: Fetches real table metadata via ``_fetch_table_metadata_direct`` when
    ``table_metadata`` is not injected (None).  When metadata is injected
    (non-None list), the fetch is skipped (same reuse logic as MCP path).

    F3: The ``metadata-unavailable`` note is CONDITIONAL on the fetch outcome:
    present only when ``pre_table_metadata`` is empty (fetch failed or
    returned nothing), absent when metadata flows.  The predicate keys on
    ``bool(pre_table_metadata)``, not on contributor output.

    Fail-open: any fetch error degrades to pre_table_metadata=[] → today's
    exact behavior including the note.  Never crashes or delays the diagnosis
    beyond the fetch attempt.
    """
    from genie.skills.mcp_trino.pre_execution_diagnosis import (
        pre_execution_diagnosis,
        OptimizationDirection,
    )
    from genie.skills.mcp_trino.preflight import plan_cost as _plan_cost

    # F2: metadata acquisition (mirrors MCP assembler's table_metadata= logic)
    if table_metadata is not None:
        pre_table_metadata = list(table_metadata)
    else:
        try:
            pre_table_metadata = _fetch_table_metadata_direct(original_sql)
        except Exception:
            pre_table_metadata = []

    # explain_cost may be injected by callers that already ran the EXPLAIN
    # round-trip for this SQL (per-iteration evidence cache) — one planner
    # call instead of one per consumer.
    if explain_cost is None and explain_runner is not None:
        try:
            explain_cost = _plan_cost(original_sql, explain_runner)
        except Exception:
            explain_cost = None

    base_dirs = pre_execution_diagnosis(
        original_sql,
        static_report=static_report,
        explain_cost=explain_cost,
        table_metadata=pre_table_metadata or None,
        peak_memory_bytes=peak_memory_bytes,
    )

    directions = list(base_dirs)

    # F3: conditional note — present only when metadata was unavailable
    if not pre_table_metadata:
        # S3: honest degradation note — severity="info" → sort rank 3 → always last.
        metadata_note = OptimizationDirection(
            kind="metadata-unavailable",
            severity="info",
            rationale=(
                "This report was generated via --direct mode: no table metadata was fetched. "
                "Partition pruning, clustering, and statistics-dependent directions are "
                "not available. Use the MCP path for full diagnosis."
            ),
            evidence="direct-path:no-metadata",
            target_metric="query_time_ms",
        )
        directions.append(metadata_note)

    return directions, pre_table_metadata


def _run_optimization_loop(
    provider,
    model: str,
    reasoning: str,
    original_sql: str,
    metric_key: str,
    max_iterations: int,
    verify_runs: int,
    output,
    build_prompt: Callable[..., str],
    *,
    long_query_opt_in: bool = True,
    long_query_threshold_s: Optional[int] = None,
    max_fallbacks: Optional[int] = None,
    explain_runner: Optional[Callable[[str], Optional[str]]] = None,
    diagnose_only: bool = False,
    execution_policy: ExecutionPolicy | None = None,
) -> dict:
    """Run the optimization loop. Returns summary dict.

    v28 dispatch:
        - baseline raises with table-not-found-shaped error → no-data path
        - baseline returns 0 rows                          → no-data path
        - else                                              → has-data iteration
          (with sql_static findings injected into the per-iteration context)
    """
    from genie.core.provider import CompletionRequest
    from genie.session.manager import new_msg, new_session
    from genie.skills.mcp_trino.preflight import (
        build_preflight_decision, PreflightDecision, PreflightRoute,
        detect_no_data_reason, check_long_query_gate,
        DEFAULT_LONG_QUERY_THRESHOLD_S, DEFAULT_MAX_FALLBACKS,
    )
    from genie.skills.trino_query.sql_static import analyze as static_analyze
    from genie.skills.trino_query.sql_static import summary_line as _static_summary_line
    from genie.output.step_trace import StepTrace

    # v48: step-level trace — populated as the loop runs; spliced into report at end
    _step_trace: StepTrace = []
    policy = execution_policy or ExecutionPolicy(None)

    # ── Static analysis (cheap; runs in both paths) ──
    try:
        static_report = static_analyze(original_sql)
    except Exception as exc:
        output.progress(f"  [warn] static analysis skipped: {exc}")
        static_report = None
    if static_report is not None:
        output.progress(f"  Static analysis: {_static_summary_line(static_report)}")

    # ── Pre-decision: fact computation ──
    _baseline = None
    _baseline_exc: Optional[BaseException] = None
    _baseline_metrics = None
    if not diagnose_only:
        output.progress("  Measuring baseline...")
        try:
            _baseline = _measure_logical_sql(
                original_sql, metric_key, verify_runs, policy=policy, capture_rows=True,
                output=output, label="baseline",
            )
            _baseline_metrics = _baseline["metrics"] if _baseline else None
        except Exception as e:
            _baseline_exc = e
    _baseline_row_count = _baseline["row_count"] if _baseline else None

    _gate = None
    fallbacks = max_fallbacks if max_fallbacks is not None else DEFAULT_MAX_FALLBACKS
    _plan_cost_available = False
    _plan_seen_no_estimates = False
    if _baseline is not None:
        # Baseline progress prints (verbatim from current code)
        baseline_metric = _baseline["median"]
        baseline_rows = _baseline["row_count"]
        baseline_data = _baseline["rows"]
        baseline_wall_ms = _baseline_wall_ms(_baseline["metrics"])
        candidate_timeout_ms = make_candidate_timeout_ms(baseline_wall_ms) if baseline_wall_ms > 0 else None
        output.progress(f"  Baseline {metric_key}: {baseline_metric} (median of {verify_runs} runs)")
        output.progress(f"  Baseline row count: {baseline_rows}")
        _print_metrics(output, _baseline["metrics"])
        if candidate_timeout_ms is not None:
            output.progress(
                f"  Candidate timeout: {candidate_timeout_ms / 1000.0:.1f}s "
                f"(baseline wall-time)"
            )
        if static_report and static_report.findings:
            output.progress(
                f"  Static analysis: {static_report.summary} "
                f"({len(static_report.findings)} finding(s) — feeding into prompt)"
            )
        threshold_s = long_query_threshold_s if long_query_threshold_s is not None else DEFAULT_LONG_QUERY_THRESHOLD_S
        _gate = check_long_query_gate(
            baseline_wall_ms=baseline_wall_ms,
            max_iterations=max_iterations, long_query_opt_in=long_query_opt_in,
            threshold_s=threshold_s, max_fallbacks=fallbacks)
        if _gate.ok and long_query_opt_in and explain_runner is not None:
            from genie.skills.mcp_trino.preflight import plan_cost as _plan_cost_probe
            try:
                _pr, _pb, _pp = _plan_cost_probe(original_sql, explain_runner)
                _plan_cost_available = _pr is not None or _pb is not None
                _plan_seen_no_estimates = (not _plan_cost_available) and _pp is not None
            except Exception:
                _plan_cost_available = False

    decision = build_preflight_decision(
        diagnose_only=diagnose_only,
        baseline_row_count=_baseline_row_count,
        baseline_exc=_baseline_exc,
        gate=_gate,
        long_query_opt_in=long_query_opt_in,
        plan_cost_available=_plan_cost_available,
        seen_no_estimates=_plan_seen_no_estimates,
        max_iterations=max_iterations,
    )

    # ── Consumption blocks — bodies verbatim from current code ──
    if decision.route == PreflightRoute.DIAGNOSE_ONLY:
        from genie.skills.mcp_trino.pre_execution_diagnosis import format_directions_report
        output.progress("  --diagnose-only: EXPLAIN-cost + static, no query execution")
        directions, _ = _assemble_direct_directions(
            original_sql, static_report, explain_runner, peak_memory_bytes=None
        )
        report_md = format_directions_report(
            directions, sql=original_sql,
            reason="--diagnose-only requested (no baseline, no iteration)",
            model=model,
        )
        return {"status": "diagnosed", "report_markdown": report_md}

    elif decision.route == PreflightRoute.NO_DATA:
        # NO progress line on direct NO_DATA today — do NOT add one
        return _run_no_data_path(
            provider=provider,
            model=model,
            reasoning=reasoning,
            original_sql=original_sql,
            no_data_reason=decision.no_data_reason,
            static_report=static_report,
            baseline_exc=decision.baseline_exc,
            output=output,
            step_trace=_step_trace,  # GAP-1: thread step_trace so critical_path StepEvent is visible
        )

    elif decision.route == PreflightRoute.REAL_FAILURE:
        output.error(f"  Baseline measurement failed: {decision.baseline_exc}")
        return {"status": "failed", "error": str(decision.baseline_exc)}

    elif decision.route == PreflightRoute.LONG_QUERY_ABORT:
        from genie.skills.mcp_trino.pre_execution_diagnosis import format_directions_report
        g = decision.gate_result
        output.progress(f"  Long-query gate: {g.message}")
        output.progress("  Writing directed report and skipping further query executions")
        directions, _ = _assemble_direct_directions(
            original_sql, static_report, explain_runner,
            peak_memory_bytes=getattr(_baseline_metrics, "peak_memory_bytes", 0) or None,
        )
        report_md = format_directions_report(
            directions, sql=original_sql, reason=g.message, model=model,
            baseline_already_ran=True,
        )
        return {
            "status": "diagnosed",
            "reason": "long_query_gate",
            "message": g.message,
            "report_markdown": report_md,
        }

    # Else: PLAN_COST_LOOP or STANDARD_LOOP — continue to pre-loop blocks below.

    # ── Pre-loop: seen_no_estimates progress (verbatim from current code) ──
    if decision.seen_no_estimates:
        output.progress(
            "  [info] Plan-cost mode unavailable: EXPLAIN returned a plan but no cost "
            "estimates (table statistics missing — run ANALYZE). Using standard iteration loop."
        )

    # ── Loop dispatch ──
    if decision.route == PreflightRoute.PLAN_COST_LOOP:
        return _run_plan_cost_loop(
            provider=provider,
            model=model,
            reasoning=reasoning,
            original_sql=original_sql,
            metric_key=metric_key,
            max_iterations=max_iterations,
            verify_runs=verify_runs,
            output=output,
            build_prompt=build_prompt,
            baseline=_baseline,
            baseline_data=baseline_data,
            static_report=static_report,
            explain_runner=explain_runner,
            max_fallbacks=fallbacks,
            candidate_timeout_ms=candidate_timeout_ms,
            execution_policy=policy,
        )

    # STANDARD_LOOP fall-through — rule-gate + directions block + session setup + loop body.
    # baseline/_baseline are available (we only reach here when _baseline is not None).
    baseline = _baseline

    # Runtime stage hotspots must be fetched promptly after the run — the
    # coordinator evicts finished queries from its in-memory history
    # (query.min-expire-age / query.max-history). Grab the baseline's NOW,
    # before the EXPLAIN round-trips and seed-decompose LLM calls below can
    # let the query_id expire.
    from genie.skills.trino_query.iteration_pipeline import stepwise_enabled as _stepwise_enabled
    from genie.skills.trino_query.query_info import stage_hotspot_block
    _stepwise = _stepwise_enabled() and provider is not None
    _baseline_query_id = getattr(baseline["metrics"], "query_id", "") or ""
    _baseline_hotspot_block = ""
    if _stepwise and _baseline_query_id:
        _baseline_hotspot_block = stage_hotspot_block(_baseline_query_id)

    # One EXPLAIN (FORMAT JSON) round-trip per distinct SQL, shared by every
    # evidence consumer (directions assembly, plan skeleton, repeated-subtree
    # note) — each previously ran its own EXPLAIN for the same SQL, up to
    # three planner round-trips per improving iteration.
    from genie.skills.mcp_trino.preflight import plan_cost as _plan_cost_for_dup
    from genie.skills.trino_query.plan_render import (
        render_plan_skeleton as _render_skeleton_from_plan,
    )
    _ev_cost_cache: dict[str, object] = {}

    def _evidence_cost(sql: str):
        """Cached plan_cost 3-tuple for sql; None when EXPLAIN unavailable."""
        if sql not in _ev_cost_cache:
            cost = None
            if explain_runner is not None:
                try:
                    cost = _plan_cost_for_dup(sql, explain_runner)
                except Exception:
                    cost = None
            _ev_cost_cache[sql] = cost
        return _ev_cost_cache[sql]

    def _evidence_plan(sql: str):
        cost = _evidence_cost(sql)
        return cost[2] if cost else None

    def _evidence_skeleton(sql: str) -> str:
        plan = _evidence_plan(sql)
        if plan is None:
            return ""
        try:
            return _render_skeleton_from_plan(plan)
        except Exception:
            return ""

    # ── Pre-execution diagnosis (v29 T2 — dual-path parity with MCP path) ──
    # The --direct path has no table-metadata fetcher, so diagnosis is driven by
    # static findings + plan-cost estimates + the baseline's actual peak memory.
    from genie.skills.mcp_trino.pre_execution_diagnosis import format_directions_for_prompt
    from genie.skills.mcp_trino.rule_gate import (
        build_rule_gate_summary,
        format_rule_gate_for_prompt,
        render_rule_gate_summary,
    )

    directions, _ = _assemble_direct_directions(
        original_sql, static_report, explain_runner,
        peak_memory_bytes=getattr(baseline["metrics"], "peak_memory_bytes", 0) or None,
        explain_cost=_evidence_cost(original_sql),
    )
    rule_gate = build_rule_gate_summary(static_report, directions)
    rule_gate_block = format_rule_gate_for_prompt(rule_gate)
    directions_block = format_directions_for_prompt(directions)
    render_rule_gate_summary(output, rule_gate)
    if directions:
        output.progress(f"  Pre-execution diagnosis: {len(directions)} ranked direction(s) → prompt")

    # Condensed plan skeleton (dual-path parity with the MCP standard loop):
    # directions carry only rule-recognized patterns; the skeleton exposes the
    # full optimizer-chosen tree. Fail-open ("" → not injected).
    from genie.skills.mcp_trino.research import _format_plan_skeleton_block
    baseline_skeleton = _evidence_skeleton(original_sql)
    skeleton_block = _format_plan_skeleton_block(
        baseline_skeleton, label="Baseline plan skeleton"
    )
    if skeleton_block:
        output.progress(f"  Plan skeleton: {len(baseline_skeleton.splitlines())} line(s) → prompt")

    # ── Session setup ──
    skill_prompt = build_prompt(True, model)
    sys_prompt = (
        f"You are optimizing a Trino SQL query for performance.\n"
        f"Target metric: {metric_key} (lower is better).\n\n"
        f"Rules:\n"
        f"- Return the COMPLETE optimized SQL in a ```sql code block\n"
        f"- Do NOT use file_patch or any tool calls\n"
        f"- Keep the EXACT same result set — same columns, same rows, same values\n"
        f"- Make ONE focused change per iteration\n"
        f"- Trino best practices: partition filters, named columns, predicate pushdown, "
        f"projection pruning, APPROX_DISTINCT over COUNT(DISTINCT), COALESCE instead of NVL\n"
        f"- Treat CTE step materialization as advisory only; keep this loop read-only\n\n"
        f"{(rule_gate_block + chr(10) + chr(10)) if rule_gate_block else ''}"
        f"{(directions_block + chr(10) + chr(10)) if directions_block else ''}"
        f"{(skeleton_block + chr(10) + chr(10)) if skeleton_block else ''}"
        f"{skill_prompt}"
    )
    session = new_session(sys_prompt)

    # ── v48 T5: Decompose-seed validation (§3.1 NORMATIVE, --direct mirror) ──
    # Single call to _seed_decompose_and_select (T-SYM: same locus as MCP path).
    # Guard: GENIE_V48_SEED_DECOMPOSE=0 → immediate passthrough, no LLM calls.
    import os as _os_v48_direct
    _v48_direct_seed_enabled = _os_v48_direct.environ.get("GENIE_V48_SEED_DECOMPOSE", "1") != "0"

    # v58: fragment rewrite opt-in (mirroring MCP standard-loop env vars)
    _v58_dir_frag_rewrite = _os_v48_direct.environ.get("GENIE_FRAGMENT_REWRITE", "0") == "1"
    _v58_dir_frag_cap = max(1, int(_os_v48_direct.environ.get("GENIE_FRAGMENT_REWRITE_CAP", "5")))

    _direct_llm_fn = None
    _dir_cost_reader_fn = None
    if _v48_direct_seed_enabled:
        try:
            from genie.skills.mcp_trino.write_analysis import _make_advisory_llm_fn as _wa_llm_fn
            from genie.skills.mcp_trino.write_analysis import _advisory_cost_reader as _dir_cost_reader_fn
            if provider is not None:
                _direct_llm_fn = _wa_llm_fn(provider, model, reasoning)
        except Exception:
            pass

    # Convert dict-based direct measurements uniformly without treating missing
    # provenance as a legacy-compatible complete capture.
    from genie.skills.mcp_trino.research import (
        MeasureResult as _MeasureResult,
        _produce_decompose_candidate,
        _seed_decompose_and_select,
    )
    _dir_baseline_mr = _direct_measure_to_measure_result(baseline)

    def _dir_produce_fn(_sql: str):
        return _produce_decompose_candidate(
            _sql, _direct_llm_fn, _dir_cost_reader_fn,
            run_static_gates=False, step_trace=_step_trace,
            enable_fragment_rewrite=_v58_dir_frag_rewrite,
            max_fragment_model_calls=_v58_dir_frag_cap,
        )

    def _dir_measure_fn(_sql: str) -> "_MeasureResult":
        _d = _measure_logical_sql(
            _sql, metric_key, verify_runs, policy=policy, capture_rows=True,
            output=output, label="seed",
        )
        return _direct_measure_to_measure_result(_d)

    history = []

    def _record_seed_rejection(record: dict) -> None:
        """Persist the shared seed gate's complete canonical failure record."""
        history.append({**record, "hypothesis": record["rejection_reason"]})

    # §3.1 NORMATIVE: best_sql and best_measure come from the SAME tuple arm.
    _dir_winner_sql, _dir_winner_mr, _ = _seed_decompose_and_select(
        original_sql, _dir_baseline_mr,
        produce_fn=_dir_produce_fn if (_direct_llm_fn and _dir_cost_reader_fn) else (lambda s: (s, [], [], None)),
        measure_fn=_dir_measure_fn,
        flag_enabled=_v48_direct_seed_enabled,
        output=output,
        trace=_step_trace,
        rejection_history=_record_seed_rejection,
    )
    best_sql = _dir_winner_sql
    best_metric = _dir_winner_mr.median_metric
    best_metrics_obj = _dir_winner_mr.metrics
    # v32 T1: per-iteration re-diagnosis cache keyed by SQL (mirrors the MCP
    # path). Seeded with the original block (already in the system prompt) so a
    # stable best_sql is never re-diagnosed.
    rediag_cache: dict[str, str] = {original_sql: directions_block}

    # ── Stepwise Step A/B/C evidence bootstrap ──
    # Runtime hotspots (QueryInfo REST), table landscape (SHOW STATS) and the
    # repeated-subtree note feed the diagnose step. All fail-open: an empty
    # block is simply omitted from the Step A prompt.
    from genie.skills.trino_query.iteration_pipeline import (
        IterationEvidence,
        record_fields,
        run_stepwise_prelude,
    )
    from genie.skills.trino_query.plan_render import repeated_subtree_note
    from genie.skills.trino_query.table_landscape import table_landscape_block

    # Without a top-level ORDER BY, result order is not part of the query's
    # semantics — the equivalence gate compares row multisets, not positions.
    _rows_ordered = _has_top_level_order_by(original_sql)

    landscape_block = ""
    best_hotspot_block = ""
    _ev_skeleton_cache: dict[str, str] = {original_sql: skeleton_block}
    _ev_dup_cache: dict[str, str] = {}
    if _stepwise:
        landscape_block = table_landscape_block(original_sql, _execute_direct_as_dicts)
        if landscape_block:
            output.progress("  Table landscape: SHOW STATS collected → Step A")
        try:
            _dup_plan = _evidence_plan(original_sql)
            _ev_dup_cache[original_sql] = (
                repeated_subtree_note(_dup_plan) if _dup_plan is not None else "")
        except Exception:
            _ev_dup_cache[original_sql] = ""
        # The baseline's hotspot block was fetched promptly after its run
        # (before the seed phase); reuse it when the seed winner IS the
        # baseline, otherwise fetch for the winner — whose own run just
        # finished, so its query_id is still fresh in coordinator history.
        _winner_query_id = getattr(best_metrics_obj, "query_id", "") or ""
        if _winner_query_id and _winner_query_id != _baseline_query_id:
            best_hotspot_block = stage_hotspot_block(_winner_query_id)
        else:
            best_hotspot_block = _baseline_hotspot_block
        if best_hotspot_block:
            output.progress("  Runtime stage hotspots (QueryInfo) collected → Step A")

    # ── Iteration loop ──
    for iteration in range(1, max_iterations + 1):
        output.print("")
        output.progress(f"  ── Iteration {iteration}/{max_iterations} ──")
        iter_base_sql = best_sql  # snapshot for per-iteration diff in report

        # Build context for AI
        last_str = "N/A (first iteration)"
        if history:
            last = history[-1]
            last_metric, last_delta = _format_history_measurement(last)
            last_str = f"{last['status']} (metric={last_metric}, delta={last_delta})"

        static_block = ""
        if iteration == 1 and static_report and static_report.findings:
            static_block = (
                "Static analysis findings (sqlglot AST rules — apply these in priority order):\n"
                f"{_format_static_findings(static_report)}\n\n"
            )

        # v32 T1: re-diagnose the current best_sql once it diverges from the
        # original (the system-prompt directions describe original_sql). Zero
        # query cost (static + EXPLAIN FORMAT JSON); cached by SQL so a stable
        # best_sql is not re-diagnosed.
        fresh_block = rediag_cache.get(best_sql)
        if fresh_block is None:
            try:
                _rediag_dirs, _ = _assemble_direct_directions(
                    best_sql, static_analyze(best_sql), explain_runner,
                    peak_memory_bytes=None,
                    explain_cost=_evidence_cost(best_sql),
                )
                fresh_block = format_directions_for_prompt(_rediag_dirs)
            except Exception:
                fresh_block = ""
            # Refresh the plan skeleton alongside directions — the tree in the
            # system prompt describes original_sql and goes stale the same way.
            # Cached per SQL; the lean-history trim bounds accumulation.
            _fresh_skeleton = _format_plan_skeleton_block(
                _evidence_skeleton(best_sql),
                label="Current plan skeleton",
            )
            if _fresh_skeleton:
                fresh_block = (
                    f"{fresh_block}\n\n{_fresh_skeleton}" if fresh_block
                    else _fresh_skeleton
                )
            rediag_cache[best_sql] = fresh_block
            # The stepwise evidence cache wants the same formatted block —
            # share it so the stepwise branch below never re-renders (or
            # re-EXPLAINs) for this SQL.
            _ev_skeleton_cache.setdefault(best_sql, _fresh_skeleton)
        diag_line = f"{fresh_block}\n\n" if (fresh_block and best_sql != original_sql) else ""

        step_fields: dict = {}
        step_hypothesis = ""
        if _stepwise:
            # Refresh per-best evidence (planner round-trips only; cached by SQL).
            _ev_skeleton = _ev_skeleton_cache.get(best_sql)
            if _ev_skeleton is None:
                _ev_skeleton = _format_plan_skeleton_block(
                    _evidence_skeleton(best_sql),
                    label="Current plan skeleton",
                )
                _ev_skeleton_cache[best_sql] = _ev_skeleton
            _ev_dup = _ev_dup_cache.get(best_sql)
            if _ev_dup is None:
                try:
                    _p = _evidence_plan(best_sql)
                    _ev_dup = repeated_subtree_note(_p) if _p is not None else ""
                except Exception:
                    _ev_dup = ""
                _ev_dup_cache[best_sql] = _ev_dup

            _prelude = run_stepwise_prelude(
                provider, model, reasoning,
                IterationEvidence(
                    metric_key=metric_key,
                    baseline_metric=baseline_metric,
                    best_metric=best_metric,
                    last_result_line=last_str if history else "",
                    iteration=iteration,
                    max_iterations=max_iterations,
                    best_sql=best_sql,
                    hotspot_block=best_hotspot_block,
                    landscape_block=landscape_block,
                    skeleton_block=_ev_skeleton,
                    dup_subtree_note=_ev_dup,
                    static_block=static_block.strip(),
                    directions_block=(diag_line.strip() or directions_block),
                ),
                output=output,
            )
            step_fields = record_fields(_prelude)
            step_hypothesis = _prelude.hypothesis
            context = _prelude.rewrite_user_msg
        else:
            context = (
                f"[Trino Query Optimization — Iteration {iteration}]\n"
                f"Target metric: {metric_key} (lower is better)\n"
                f"Baseline: {baseline_metric}\n"
                f"Current best: {best_metric}\n"
                f"Last iteration: {last_str}\n\n"
                f"{static_block}"
                f"Current SQL:\n```sql\n{best_sql}\n```\n\n"
                f"{diag_line}"
                f"Return the COMPLETE optimized SQL in a ```sql block. ONE change only. "
                f"Do NOT include a trailing semicolon."
            )

        # Keep history lean: only system + last 4 messages (2 user/assistant pairs)
        # to avoid context bloat with local models
        sys_msgs = [m for m in session["history"] if m["role"] == "system"]
        non_sys = [m for m in session["history"] if m["role"] != "system"]
        session["history"] = sys_msgs + non_sys[-4:]

        session["history"].append(new_msg("user", context))

        # Get AI response
        output.progress("  AI thinking...")
        req = CompletionRequest(messages=session["history"], model=model, reasoning=reasoning)
        reply = provider.complete_text(req)

        if not reply:
            output.error("  Empty AI response — stopping.")
            break

        session["history"].append(new_msg("assistant", reply))

        # Extract SQL
        candidate_sql = extract_sql_from_reply(reply)
        if not candidate_sql:
            output.progress("  [SKIP] No SQL found in AI response.")
            session["history"].append(new_msg("user", "I couldn't find a SQL block in your response. Return the COMPLETE SQL in a ```sql block."))
            # One retry
            req2 = CompletionRequest(messages=session["history"], model=model, reasoning=reasoning)
            reply2 = provider.complete_text(req2)
            if reply2:
                session["history"].append(new_msg("assistant", reply2))
                candidate_sql = extract_sql_from_reply(reply2)
            if not candidate_sql:
                output.progress("  [SKIP] Still no SQL — skipping iteration.")
                history.append({
                    "iteration": iteration, "status": "no_sql",
                    "metric": best_metric, "delta": 0.0, "hypothesis": "no SQL extracted",
                    "base_sql": iter_base_sql, "candidate_sql": None,
                    **step_fields,
                })
                continue

        # Hypothesis: Step B's explicit HYPOTHESIS line when the stepwise
        # prelude produced one; else first meaningful line of the reply.
        hypothesis = step_hypothesis or "?"
        if hypothesis == "?" and reply:
            for line in reply.split("\n"):
                line = line.strip()
                if line and not line.startswith("```") and not line.startswith("|"):
                    hypothesis = line
                    break
        output.progress(f"  [Hypothesis] {hypothesis[:80]}")

        # Guard 1: Lint check
        lint_ok, lint_msg = _lint_sql(candidate_sql)
        if not lint_ok:
            output.progress(f"  [REVERT] Lint failed: {lint_msg}")
            session["history"].append(new_msg("user", f"SQL failed lint: {lint_msg}. Change REVERTED."))
            history.append({
                "iteration": iteration, "status": "lint_failed",
                "metric": best_metric, "delta": 0.0, "hypothesis": hypothesis,
                "base_sql": iter_base_sql, "candidate_sql": candidate_sql,
                **step_fields,
            })
            continue

        # Guard 2: Execute and measure
        try:
            candidate = _measure_logical_sql(
                candidate_sql, metric_key, verify_runs, policy=policy, capture_rows=True,
                output=output, label=f"iter {iteration} candidate",
                timeout_ms=candidate_timeout_ms,
            )
        except CandidateTimeoutError as e:
            output.progress(f"  [REVERT] timeout_worse: {e}")
            session["history"].append(new_msg(
                "user",
                f"SQL exceeded the baseline wall-time limit: {e}. Change REVERTED."
            ))
            history.append({
                "iteration": iteration, "status": "timeout_worse",
                "metric": best_metric, "delta": 0.0, "hypothesis": hypothesis,
                "base_sql": iter_base_sql, "candidate_sql": candidate_sql,
                **step_fields,
            })
            continue
        except Exception as e:
            output.progress(f"  [REVERT] Execution failed: {e}")
            session["history"].append(new_msg("user", f"SQL execution failed: {e}. Change REVERTED."))
            history.append({
                "iteration": iteration, "status": "exec_failed",
                "metric": best_metric, "delta": 0.0, "hypothesis": hypothesis,
                "base_sql": iter_base_sql, "candidate_sql": candidate_sql,
                **step_fields,
            })
            continue

        candidate_metric = candidate["median"]
        candidate_rows = candidate["row_count"]
        candidate_data = candidate["rows"]
        delta = candidate_metric - best_metric

        # Guard 3: only complete direct captures authorize semantic comparison.
        if not _correctness_authorized(baseline, candidate):
            reason = _incomplete_rejection_reason(baseline, candidate)
            history.append({
                **_incomplete_history(
                    iteration=iteration, baseline=baseline, candidate=candidate,
                    base_sql=iter_base_sql, candidate_sql=candidate_sql,
                    metric=candidate_metric, delta=delta,
                ),
                "hypothesis": hypothesis,
                **step_fields,
            })
            continue
        equiv, equiv_reason = _results_equivalent(baseline_data, candidate_data,
                                                  ordered=_rows_ordered)
        if not equiv:
            output.progress(
                f"  [REVERT] Result mismatch: {equiv_reason} "
                f"(semantic drift detected)"
            )
            session["history"].append(new_msg(
                "user",
                f"Query results differ from baseline: {equiv_reason}. "
                f"This means the optimization changed the query semantics. Change REVERTED. "
                f"Try a different approach that preserves the exact same result set."
            ))
            history.append({
                "iteration": iteration, "status": "semantic_drift",
                "metric": candidate_metric, "delta": delta, "hypothesis": hypothesis,
                "base_sql": iter_base_sql, "candidate_sql": candidate_sql,
                **step_fields,
            })
            continue

        # Decision: keep or revert
        improved = candidate_metric < best_metric
        if improved:
            best_sql = candidate_sql
            best_metric = candidate_metric
            best_metrics_obj = candidate["metrics"]  # v32 T2
            status = "KEPT"
            status_icon = "+"
            if _stepwise:
                # New best → refresh runtime hotspots from ITS median run,
                # promptly (coordinator history eviction). An unreachable
                # API yields "" — Step A then omits the block rather than
                # citing stats from a previous best.
                best_hotspot_block = stage_hotspot_block(
                    getattr(candidate["metrics"], "query_id", "") or "")
        else:
            status = "REVERTED"
            status_icon = "-"

        output.progress(
            f"  [{status_icon}] {status} | {metric_key}={candidate_metric:.1f} "
            f"(delta={delta:+.1f}, samples={candidate['samples']}) | "
            f"rows={candidate_rows}"
        )
        _print_metrics(output, candidate["metrics"])

        # Feed result back to AI
        session["history"].append(new_msg(
            "user",
            f"[Iteration {iteration} result]\n"
            f"Status: {status}\n"
            f"Metric ({metric_key}, median of {verify_runs} runs): {candidate_metric:.1f}\n"
            f"Delta vs current best: {delta:+.1f}\n"
            f"Row count: {candidate_rows} (baseline: {baseline_rows})\n"
            f"Samples: {candidate['samples']}\n"
            f"{'Change KEPT — this is now the current best.' if improved else 'Change REVERTED — current best unchanged.'}"
        ))

        history.append({
            "iteration": iteration,
            "status": "improved" if improved else "worse",
            "metric": candidate_metric,
            "delta": delta,
            "hypothesis": hypothesis,
            "base_sql": iter_base_sql,
            "candidate_sql": candidate_sql,
            **step_fields,
        })

        # Early exit: 3 consecutive non-improvements → plateau
        if len(history) >= 3:
            last_3 = history[-3:]
            if all(h["status"] != "improved" for h in last_3):
                output.progress(
                    f"\n  [EARLY STOP] 3 consecutive iterations without improvement — "
                    f"optimization has plateaued."
                )
                break

    # ── Direction efficacy (v32 T2) — observational attribution (mirrors MCP) ──
    from genie.skills.mcp_trino.pre_execution_diagnosis import (
        attribute_directions as _attribute_directions,
        format_attribution_report as _format_attribution_report,
    )
    _ATTR_KEYS = (
        "wall_time_ms", "query_time_ms", "cpu_time_ms",
        "peak_memory_bytes", "physical_input_bytes", "processed_rows", "total_splits",
    )

    def _metrics_attr_map(m):
        return {
            k: float(getattr(m, k))
            for k in _ATTR_KEYS
            if isinstance(getattr(m, k, None), (int, float))
        }

    _outcomes = _attribute_directions(
        directions, _metrics_attr_map(baseline["metrics"]), _metrics_attr_map(best_metrics_obj)
    )
    _attr_block = _format_attribution_report(_outcomes)
    if _attr_block:
        output.print("")
        for _line in _attr_block.splitlines():
            output.print(f"  {_line}")

    # ── Summary ──
    kept_count = sum(1 for h in history if h["status"] == "improved")
    total_improvement = best_metric - baseline_metric

    return {
        "status": "completed",
        "baseline_metric": baseline_metric,
        "best_metric": best_metric,
        "total_improvement": total_improvement,
        "improvement_pct": (total_improvement / baseline_metric * 100) if baseline_metric else 0,
        "iterations": len(history),
        "kept": kept_count,
        "baseline_rows": baseline_rows,
        "original_sql": original_sql,
        "best_sql": best_sql,
        "history": history,
        "step_trace": _step_trace,  # v48
    }


def _print_metrics(output, metrics: QueryMetrics) -> None:
    """Print key metrics in dim style."""
    output.print(f"    [dim]cpu={metrics.cpu_time_ms}ms wall={metrics.wall_time_ms}ms "
                 f"splits={metrics.total_splits} rows={metrics.processed_rows}[/dim]")


_VERDICT = {
    "improved": "kept — new best",
    "worse": "reverted — slower than current best",
    "semantic_drift": "reverted — row results diverged from baseline",
    "lint_failed": "reverted — failed lint",
    "exec_failed": "reverted — execution error",
    "no_sql": "skipped — no SQL block in AI reply",
}


def _iteration_diff(base_sql: str, candidate_sql: str) -> str:
    """Unified diff scoped to a single iteration's change."""
    import difflib
    diff = difflib.unified_diff(
        base_sql.splitlines(keepends=True),
        candidate_sql.splitlines(keepends=True),
        fromfile="base",
        tofile="candidate",
        n=2,
    )
    return "".join(diff).rstrip()


def _format_history_measurement(history_entry: dict) -> tuple[str, str]:
    """Render optional history measurement fields without upgrading compact records.

    Shared plan-cost ranking records intentionally contain only ranking facts
    (including ``plan_cost``), while canonical incomplete-result records retain
    their complete persisted shape.  A direct no-winner result can contain both.
    Report and terminal renderers therefore display unavailable measured values
    as ``n/a`` rather than assuming every ranking record was measured.
    """
    def format_value(value: object, spec: str) -> str:
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            return "n/a"
        return format(value, spec)

    return (
        format_value(history_entry.get("metric"), ".1f"),
        format_value(history_entry.get("delta"), "+.1f"),
    )


def _render_terminal_history(output, history: list[dict]) -> None:
    """Render all direct history shapes safely in the terminal summary."""
    for entry in history:
        icon = "+" if entry["status"] == "improved" else "-" if entry["status"] == "worse" else "!"
        metric, delta = _format_history_measurement(entry)
        output.print(
            f"    [{icon}] iter {entry['iteration']}: {entry['status']:<15s} "
            f"metric={metric} delta={delta}"
        )


def _report_correctness_status(history: list[dict]) -> tuple[bool, str | None]:
    """Return whether this report may make a full-equivalence claim.

    A persisted incomplete-result rejection is authoritative evidence that the
    correctness gate was not authorized for this run.  Keep report rendering
    fail-closed: absent that evidence the existing direct completed-run contract
    remains the verified path, while any such rejection suppresses semantic and
    full row-level equivalence language.
    """
    for entry in history:
        if entry.get("status") == "equivalence_unverified_incomplete_result":
            return False, entry.get("rejection_reason", "mixed_incomplete_result")
    return True, None


def _generate_report(result: dict, metric_key: str, model: str, verify_runs: int) -> str:
    """Generate a markdown report — iteration-centric, single Best SQL block."""
    from datetime import datetime

    correctness_authorized, rejection_reason = _report_correctness_status(result["history"])
    if correctness_authorized:
        validation_line = "**Result validation:** full row-level equivalence check"
        row_count_line = f"| Row count | {result['baseline_rows']} (preserved) |"
    else:
        validation_line = (
            "**Result validation:** unverified/incomplete result — full row-level "
            f"equivalence and semantic preservation were not authorized "
            f"(rejection reason: `{rejection_reason}`)"
        )
        row_count_line = (
            f"| Row count | {result['baseline_rows']} "
            "(observed baseline; preservation unverified) |"
        )

    lines = [
        "# Trino Query Optimization Report",
        "",
        f"**Date:** {datetime.now().strftime('%Y-%m-%d %H:%M')}",
        f"**Model:** {model}",
        f"**Metric:** {metric_key} (lower is better)",
        f"**Verify runs:** {verify_runs} (median)",
        validation_line,
        "",
        "## Summary",
        "",
        "| Metric | Value |",
        "|--------|-------|",
        f"| Baseline | {result['baseline_metric']:.1f} |",
        f"| Best | {result['best_metric']:.1f} |",
        f"| Improvement | {result['total_improvement']:+.1f} ({result['improvement_pct']:+.1f}%) |",
        f"| Iterations | {result['iterations']} ({result['kept']} kept) |",
        row_count_line,
        "",
        "## Iteration history",
        "",
        "| # | Status | Metric | Delta |",
        "|---|--------|--------|-------|",
    ]
    for h in result["history"]:
        metric, delta = _format_history_measurement(h)
        lines.append(f"| {h['iteration']} | {h['status']} | {metric} | {delta} |")

    lines.append("")
    lines.append("## Iterations")
    lines.append("")
    for h in result["history"]:
        lines.append(f"### Iteration {h['iteration']} — {h['status']}")
        lines.append("")
        lines.append(f"**Hypothesis:** {h.get('hypothesis', h.get('rejection_reason', 'n/a'))}")
        lines.append("")
        # Stepwise pipeline provenance (present when GENIE_STEPWISE_ITERATION
        # ran): what the model diagnosed and which named strategy it chose.
        if h.get("diagnosis"):
            lines.append(f"**Step A diagnosis:**\n```\n{h['diagnosis']}\n```")
            lines.append("")
        if h.get("strategy"):
            lines.append(f"**Step B strategy:**\n```\n{h['strategy']}\n```")
            lines.append("")
        metric, delta = _format_history_measurement(h)
        lines.append(
            f"**Metric:** {metric} (delta {delta}) — "
            f"{_VERDICT.get(h['status'], h['status'])}"
        )
        lines.append("")
        candidate_sql = h.get("candidate_sql")
        base_sql = h.get("base_sql")
        if candidate_sql and base_sql:
            diff_text = _iteration_diff(base_sql, candidate_sql)
            if diff_text:
                lines.append("```diff")
                lines.append(diff_text)
                lines.append("```")
            else:
                lines.append("_(candidate SQL identical to current best — no diff)_")
            lines.append("")

    lines.append("## Best SQL")
    lines.append("")
    if result["best_sql"] == result["original_sql"]:
        lines.append("_No improvement kept. Original SQL is the best so far for this metric._")
        lines.append("")
        lines.append("```sql")
        lines.append(result["original_sql"])
        lines.append("```")
    else:
        lines.append("```sql")
        lines.append(result["best_sql"])
        lines.append("```")

    lines.append("")

    # v48: splice step trace into report
    step_trace = result.get("step_trace")
    if step_trace:
        from genie.output.step_trace import render_report as _render_step_report
        step_section = _render_step_report(step_trace)
        if step_section.strip():
            lines.append("## Step Trace")
            lines.append("")
            lines.append(step_section)
            lines.append("")

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Entry points
# ---------------------------------------------------------------------------

def run_trino_research(
    provider,
    cfg: dict,
    model: str,
    reasoning: str,
    output,
    build_prompt: Callable[..., str],
    *,
    # Non-interactive params (used when called with --flags from chat.py)
    sql_file: Optional[str] = None,
    sql_text: Optional[str] = None,
    metric: Optional[str] = None,
    iterations: Optional[int] = None,
    runs: Optional[int] = None,
    safe_limit: Optional[int] = None,
    query_timeout: Optional[int] = None,
    long_query_opt_in: bool = True,
    long_query_threshold_s: Optional[int] = None,
    max_fallbacks: Optional[int] = None,
    diagnose_only: bool = False,
) -> None:
    """Entry point for /trino-research command.

    Supports both interactive mode (no kwargs) and non-interactive mode
    (all params passed in via kwargs).
    """
    METRICS = ["cpu_time_ms", "wall_time_ms", "physical_input_bytes", "processed_rows", "total_splits", "peak_memory_bytes"]

    output.print("\n  [yellow]== Trino Query Optimization (v2) ==[/yellow]")

    # ── Get SQL ──
    if sql_file:
        sql = Path(sql_file).read_text().strip()
        output.progress(f"  SQL from file: {sql_file}")
    elif sql_text:
        sql = sql_text.strip()
    else:
        # Interactive: paste mode
        from genie.input import _read_paste_mode
        output.print("  [cyan]Paste SQL (Ctrl-D to finish):[/cyan]")
        sql = _read_paste_mode()

    if not sql:
        output.error("Empty SQL.")
        return

    from genie.skills.mcp_trino.write_analysis import classify_write_operation, run_write_analysis_only

    # Validate as soon as public SQL exists, before advisory/provider/EXPLAIN work.
    validated_safe_limit = validate_safe_limit(safe_limit)

    if classify_write_operation(sql) is not None:
        run_write_analysis_only(
            provider, cfg, model, reasoning, sql, output, build_prompt,
            sql_source=sql_file or ("sql_text" if sql_text else "stdin"),
            route="direct",
            safe_limit=validated_safe_limit,
        )
        return

    output.print(f"  [dim]SQL: {sql[:80]}...[/dim]\n")

    # ── Get metric ──
    if not metric:
        from genie.input import _read_input
        output.print("  [yellow]Metric to minimize:[/yellow]")
        for i, m in enumerate(METRICS, 1):
            output.print(f"    [cyan]{i}[/cyan]. {m}")
        try:
            choice = _read_input("  Choose [1]: ").strip() or "1"
            idx = int(choice) - 1
            metric = METRICS[idx] if 0 <= idx < len(METRICS) else "cpu_time_ms"
        except (ValueError, EOFError, KeyboardInterrupt):
            metric = "cpu_time_ms"

    if metric not in METRICS:
        output.error(f"Unknown metric: {metric}. Use one of: {METRICS}")
        return

    # ── Get iterations ──
    if iterations is None:
        from genie.input import _read_input
        try:
            iter_str = _read_input("  Max iterations [5]: ").strip() or "5"
            iterations = max(1, int(iter_str))
        except (ValueError, EOFError, KeyboardInterrupt):
            iterations = 5

    # ── Get verify runs ──
    if runs is None:
        from genie.input import _read_input
        try:
            runs_str = _read_input("  Verify runs per candidate [3]: ").strip() or "3"
            runs = max(1, int(runs_str))
        except (ValueError, EOFError, KeyboardInterrupt):
            runs = 3

    output.progress(f"  Metric:     {metric} (lower is better)")
    output.progress(f"  Iterations: {iterations}")
    output.progress(f"  Verify:     median of {runs} runs")
    output.print("")

    # ── EXPLAIN (FORMAT JSON) runner — zero query cost, feeds plan diagnosis ──
    def _direct_explain_runner(s: str) -> Optional[str]:
        _assert_executable_read_only(s)
        try:
            _, _, rows = _execute_sql(f"EXPLAIN (FORMAT JSON) {s}", capture_rows=True)
        except Exception:
            return None
        if not rows:
            return None
        first = rows[0]
        cell = first[0] if isinstance(first, (list, tuple)) and first else first
        return cell if isinstance(cell, str) else None

    # Entry safety preflight is deliberately evaluated against original SQL.
    from genie.skills.mcp_trino.preflight import run_preflight
    preflight = run_preflight(sql, _direct_explain_runner)
    if not preflight.ok:
        output.error(f"  Preflight rejected: {preflight.reason}")
        return
    # Policy is created only after accepted original-SQL entry preflight. Existing
    # loop call sites retain logical SQL; adapter boundaries can derive execution
    # SQL independently through _measure_logical_sql.
    _policy = ExecutionPolicy(validated_safe_limit)

    # ── Run ──
    result = _run_optimization_loop(
        provider=provider,
        model=model,
        reasoning=reasoning,
        original_sql=sql,
        metric_key=metric,
        max_iterations=iterations,
        verify_runs=runs,
        output=output,
        build_prompt=build_prompt,
        long_query_opt_in=long_query_opt_in,
        long_query_threshold_s=long_query_threshold_s,
        max_fallbacks=max_fallbacks,
        explain_runner=_direct_explain_runner,
        diagnose_only=diagnose_only,
        execution_policy=_policy,
    )

    # ── Print summary ──
    output.print("")
    if result["status"] == "failed":
        output.error(f"  Run failed: {result.get('error', 'unknown')}")
        return
    if result["status"] == "diagnosed":
        # Directed report (gate-trip fallback or --diagnose-only).
        from datetime import datetime
        report_md = result.get("report_markdown") or ""
        report_dir = Path.cwd() / "report"
        report_dir.mkdir(parents=True, exist_ok=True)
        report_name = f"trino-research-diagnose-{datetime.now().strftime('%Y%m%d-%H%M%S')}.md"
        report_path = report_dir / report_name
        try:
            report_path.write_text(report_md)
            output.progress(f"\n  Directed report saved: {report_path}")
        except Exception as e:
            output.error(f"  Failed to save directed report: {e}")
        return
    if result["status"] == "no_data":
        # Static-analysis-only report — save it and exit early.
        from datetime import datetime
        report_md = result.get("report_markdown") or ""
        report_dir = Path.cwd() / "report"
        report_dir.mkdir(parents=True, exist_ok=True)
        report_name = f"trino-research-nodata-{datetime.now().strftime('%Y%m%d-%H%M%S')}.md"
        report_path = report_dir / report_name
        try:
            report_path.write_text(report_md)
            output.progress(f"\n  Static report saved: {report_path}")
        except Exception as e:
            output.error(f"  Failed to save report: {e}")
        return

    output.print("  [yellow]══ Summary ══[/yellow]")
    output.print(f"  Baseline:    {result['baseline_metric']:.1f}")
    output.print(f"  Best:        {result['best_metric']:.1f}")
    output.print(f"  Improvement: {result['total_improvement']:+.1f} ({result['improvement_pct']:+.1f}%)")
    output.print(f"  Iterations:  {result['iterations']} ({result['kept']} kept)")
    correctness_authorized, rejection_reason = _report_correctness_status(result["history"])
    if correctness_authorized:
        output.print(f"  Row count:   {result['baseline_rows']} (preserved)")
    else:
        output.print(
            f"  Row count:   {result['baseline_rows']} "
            "(observed baseline; preservation unverified/incomplete result)"
        )
        output.print(f"  Result validation: unverified/incomplete result ({rejection_reason})")
    output.print("")

    # Iteration history
    _render_terminal_history(output, result["history"])

    # Final SQL — full body lives in the report; terminal stays scannable.
    if result["best_sql"] == result["original_sql"]:
        output.print("\n  [dim]No improvement found — original SQL unchanged.[/dim]")

    # Generate and save report
    report = _generate_report(result, metric, model, runs)
    from datetime import datetime
    report_name = f"trino-research-{datetime.now().strftime('%Y%m%d-%H%M%S')}.md"
    report_dir = Path.cwd() / "report"
    report_dir.mkdir(parents=True, exist_ok=True)
    report_path = report_dir / report_name
    try:
        report_path.write_text(report)
        output.progress(f"\n  Report saved: {report_path}")
    except Exception as e:
        output.error(f"  Failed to save report: {e}")
