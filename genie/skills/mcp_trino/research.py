"""mcp_trino research — Autoresearch query enhancement via MCP Trino server.

Uses the MCP client to execute queries and collect metrics, then runs
an AI-driven optimization loop with 5 iterations. Outputs a fixed-format
report that is identical in structure every run.

Architecture:
- MCP client sends tools/call to the Trino MCP server for execution
- Metrics are collected from the MCP response (or timed locally)
- AI proposes SQL rewrites; each is verified for correctness + performance
- Report uses a fixed template (see REPORT_TEMPLATE)
"""
from __future__ import annotations

import json
import math
import re
import statistics
import time
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Optional

import requests
from rich.markup import escape

from genie.core.sql_extraction import extract_sql_from_reply
from .client import McpClient, McpConfig, McpError, load_mcp_config
from .preflight import (
    CandidateTimeoutError, ExecutionPolicy, _assert_executable_read_only,
    _execution_sql_for, make_candidate_timeout_ms, validate_safe_limit,
)
from .write_analysis import classify_write_operation, run_write_analysis_only

# sqlglot is already a project dependency — used for table name extraction
import sqlglot


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------

@dataclass
class RunMetrics:
    """Metrics from a single query execution."""
    query_time_ms: float = 0.0
    cpu_time_ms: float = 0.0
    wall_time_ms: float = 0.0
    peak_memory_bytes: int = 0
    physical_input_bytes: int = 0
    processed_rows: int = 0
    total_splits: int = 0

    def summary(self) -> str:
        return (f"query={self.query_time_ms:.0f}ms cpu={self.cpu_time_ms:.0f}ms "
                f"wall={self.wall_time_ms:.0f}ms rows={self.processed_rows}")


@dataclass
class MeasureResult:
    """Aggregated result from multiple runs and local capture provenance."""
    median_metric: float
    samples: list[float]
    row_count: int
    rows: list  # received MCP envelope rows, possibly locally retained prefix
    columns: list[str]
    metrics: RunMetrics
    observed_row_count: int | None = None
    captured_row_count: int = 0
    max_capture_rows: int = 100_000
    capture_status: str = "not_captured"
    completeness: str = "not_captured"

    def __post_init__(self) -> None:
        if self.observed_row_count is None:
            self.observed_row_count = self.row_count
        # row_count remains the compatibility alias.
        self.row_count = self.observed_row_count
        if self.captured_row_count == 0 and self.rows:
            self.captured_row_count = len(self.rows)


@dataclass
class IterationRecord:
    """Record of a single optimization iteration."""
    iteration: int
    status: str  # improved | worse | lint_failed | exec_failed | semantic_drift | no_sql
    # Compact plan-cost ranking records have no execution measurement.
    metric_value: float | None
    delta: float | None
    hypothesis: str
    sql: str = ""
    rejection_reason: str | None = None
    base_sql: str | None = None
    candidate_sql: str | None = None
    baseline_capture_status: str | None = None
    candidate_capture_status: str | None = None
    baseline_completeness: str | None = None
    candidate_completeness: str | None = None

    @classmethod
    def from_history(cls, history: dict, *, hypothesis: str, sql: str = "") -> "IterationRecord":
        """Map the canonical persisted history shape to the report record shape."""
        return cls(
            iteration=history["iteration"],
            status=history["status"],
            metric_value=history.get("metric"),
            delta=history.get("delta"),
            hypothesis=hypothesis,
            sql=sql,
            rejection_reason=history.get("rejection_reason"),
            base_sql=history.get("base_sql"),
            candidate_sql=history.get("candidate_sql"),
            baseline_capture_status=history.get("baseline_capture_status"),
            candidate_capture_status=history.get("candidate_capture_status"),
            baseline_completeness=history.get("baseline_completeness"),
            candidate_completeness=history.get("candidate_completeness"),
        )


@dataclass
class EnhancementReport:
    """Complete report from an enhancement run."""
    timestamp: str
    original_sql: str
    original_result_sample: list[dict]
    original_columns: list[str]
    original_row_count: int
    original_metrics: RunMetrics
    enhanced_sql: str
    enhanced_result_sample: list[dict]
    enhanced_columns: list[str]
    enhanced_row_count: int
    enhanced_metrics: RunMetrics
    metric_key: str
    baseline_value: float
    best_value: float
    improvement_abs: float
    improvement_pct: float
    iterations: list[IterationRecord]
    data_consistent: bool
    data_consistency_reason: str
    mcp_server_url: str
    verify_runs: int
    table_suggestions: list[TableSuggestion] = field(default_factory=list)
    had_qualified_tables: bool = False
    original_explain: ExplainAnalyzeResult | None = None
    enhanced_explain: ExplainAnalyzeResult | None = None
    step_trace: list = field(default_factory=list)  # v48: StepTrace (typed loosely to avoid circular)


@dataclass
class ColumnInfo:
    """Column metadata from information_schema."""
    column_name: str
    data_type: str
    is_nullable: str
    ordinal_position: int


@dataclass
class TableMetadata:
    """Metadata for a single table."""
    catalog: str
    schema: str
    table_name: str
    columns: list[ColumnInfo] = field(default_factory=list)
    properties: dict[str, str] = field(default_factory=dict)


@dataclass
class TableSuggestion:
    """A single table-level optimization suggestion."""
    table: str
    category: str  # partition | bucket | data_type | sort | general
    suggestion: str
    suggestion_zh: str  # 繁體中文 version
    severity: str = "info"  # info | warning | critical


# ---------------------------------------------------------------------------
# Table metadata helpers
# ---------------------------------------------------------------------------

_SAFE_IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _validate_ident(name: str) -> None:
    """Raise ValueError if *name* is not a safe bare SQL identifier.

    Accepted: letters, digits, underscores, starting with a letter or underscore.
    Rejected: quotes, spaces, dots, semicolons, or any other characters that
    could permit SQL injection when interpolated into a query string.
    This is intentionally strict — the caller's except-guard catches the
    ValueError and skips the table (fail-open, no propagation).
    """
    if not _SAFE_IDENT_RE.match(name):
        raise ValueError(f"Unsafe SQL identifier: {name!r}")


def _fetch_table_metadata_from_runner(
    tables: list[tuple[str, str, str]],
    execute_fn,
    default_catalog: str = "",
    default_schema: str = "",
) -> list:
    """Pure shared helper — fetch metadata for *tables* using *execute_fn*.

    *execute_fn* is injected so this function is usable from both the MCP path
    (where a ``_make_mcp_execute_fn(client)`` adapter is passed) and the
    --direct path (where ``_execute_direct_as_dicts`` is passed directly).

    Contract:
      execute_fn(sql: str) -> list[dict]  — callers provide rows as plain dicts.

    Returns a ``list[TableMetadata]`` — only tables with at least one column or
    one property are appended.  Any execute_fn exception for a given table is
    caught and that table is silently skipped (fail-open).
    """
    results = []
    for catalog, schema, table_name in tables:
        cat = catalog or default_catalog
        sch = schema or default_schema
        if not cat or not sch:
            continue

        # Validate all three identifier components before interpolating into SQL.
        # ValueError here → caught by `except ValueError` below → table skipped (fail-open).
        try:
            _validate_ident(cat)
            _validate_ident(sch)
            _validate_ident(table_name)
        except ValueError:
            continue

        meta = TableMetadata(catalog=cat, schema=sch, table_name=table_name)

        # Probe 1: column schema
        col_sql = (
            f"SELECT column_name, data_type, is_nullable, ordinal_position "
            f"FROM {cat}.information_schema.columns "
            f"WHERE table_schema = '{sch}' AND table_name = '{table_name}' "
            f"ORDER BY ordinal_position"
        )
        try:
            rows = execute_fn(col_sql)
            for row in rows:
                if isinstance(row, dict):
                    meta.columns.append(ColumnInfo(
                        column_name=row.get("column_name", ""),
                        data_type=row.get("data_type", ""),
                        is_nullable=row.get("is_nullable", ""),
                        ordinal_position=int(row.get("ordinal_position", 0)),
                    ))
        except Exception:
            pass

        # Probe 2: table properties (Iceberg partition/sort/etc.)
        prop_sql = (
            f"SELECT property_name, property_value "
            f"FROM system.metadata.table_properties "
            f"WHERE catalog_name = '{cat}' "
            f"AND schema_name = '{sch}' "
            f"AND table_name = '{table_name}'"
        )
        try:
            rows = execute_fn(prop_sql)
            for row in rows:
                if isinstance(row, dict):
                    key = row.get("property_name", "")
                    val = row.get("property_value", "")
                    if key:
                        meta.properties[key] = val
        except Exception:
            pass

        if meta.columns or meta.properties:
            results.append(meta)

    return results


def _make_mcp_execute_fn(client):
    """Return an execute_fn that wraps _execute_via_mcp, extracting the rows list.

    _execute_via_mcp returns a dict envelope ``{"rows": [...], "columns": [...],
    "error": ...}``.  Iterating the envelope directly yields dict keys (strings),
    not row dicts — silently producing empty metadata.  This adapter extracts
    ``envelope["rows"]`` (or [] on error / None rows) so the shared
    ``_fetch_table_metadata_from_runner`` receives a plain ``list[dict]``.
    """
    def _fn(sql: str) -> list:
        envelope = _execute_via_mcp(client, sql)
        if envelope.get("error") or not envelope.get("rows"):
            return []
        return list(envelope["rows"])
    return _fn


def _extract_table_names(sql: str) -> list[tuple[str, str, str]]:
    """Extract (catalog, schema, table) tuples from SQL using sqlglot.

    Returns tuples where catalog/schema may be empty strings if not qualified.
    """
    tables = set()
    try:
        for statement in sqlglot.parse(sql, dialect="trino"):
            if statement is None:
                continue
            for table in statement.find_all(sqlglot.exp.Table):
                catalog = table.catalog or ""
                schema = table.db or ""
                name = table.name or ""
                if name and not name.startswith("__"):
                    tables.add((catalog, schema, name))
    except sqlglot.errors.ParseError:
        pass
    return sorted(tables)


# Trino DataSize units — binary base (1 kB = 1024 B), Sam-locked (sam-decisions.md:5).
# Distinct from the inline EXPLAIN-ANALYZE map at research.py:495 (do NOT touch that one —
# it is case-sensitive uppercase and has no callers to update; surgical-diff discipline).
_TRINO_DATASIZE_UNITS: dict[str, int] = {
    "B": 1,
    "KB": 1024, "KIB": 1024,
    "MB": 1024 ** 2, "MIB": 1024 ** 2,
    "GB": 1024 ** 3, "GIB": 1024 ** 3,
    "TB": 1024 ** 4, "TIB": 1024 ** 4,
    "PB": 1024 ** 5, "PIB": 1024 ** 5,
}


def _parse_trino_datasize(s: Any) -> Optional[int]:
    """Parse a Trino DataSize string ('5GB', '1.5kB', '512MB') to bytes.

    Binary base (1 kB = 1024 B). Case-insensitive units, whitespace-tolerant,
    fractional values (int(val * mult)). Returns None for empty/None/non-string/
    bare-number/unknown-unit/zero/negative/garbage. Never raises.
    """
    if not s or not isinstance(s, str):
        return None
    s = s.strip()
    if not s:
        return None
    m = re.match(r"^([\d.]+)\s*([A-Za-z]+)$", s)
    if not m:
        return None
    try:
        val = float(m.group(1))
    except ValueError:
        return None
    if not math.isfinite(val):
        return None
    mult = _TRINO_DATASIZE_UNITS.get(m.group(2).upper())
    if mult is None:
        return None
    product = val * mult
    if not math.isfinite(product):
        return None
    result = int(product)
    return result if result > 0 else None


def _fetch_table_metadata(
    client: McpClient,
    tables: list[tuple[str, str, str]],
    default_catalog: str = "",
    default_schema: str = "",
) -> list[TableMetadata]:
    """Query information_schema.columns and table properties via MCP.

    Gracefully returns empty list if the MCP server can't handle these queries.
    Delegates to the shared pure helper ``_fetch_table_metadata_from_runner``
    via the MCP envelope-extracting adapter ``_make_mcp_execute_fn``.
    """
    execute_fn = _make_mcp_execute_fn(client)
    return _fetch_table_metadata_from_runner(
        tables, execute_fn,
        default_catalog=default_catalog,
        default_schema=default_schema,
    )


from typing import NamedTuple


class MemoryLimitResult(NamedTuple):
    """Source-tagged result from _fetch_per_node_memory_limit.

    bytes: resolved per-node limit in bytes, or None when no usable value.
    source: one of "env" | "show_session" | "bad_env_fallthrough" | "default-fallback".
      "env"                — GENIE_TRINO_MEMORY_LIMIT_PER_NODE_BYTES was set and valid.
      "show_session"       — SHOW SESSION query_max_memory_per_node parsed successfully.
      "bad_env_fallthrough"— env var was set but invalid (non-int or ≤ 0); bytes is from
                             SHOW SESSION (if available) or None.
      "default-fallback"   — no usable value from any source; bytes is None → 1 GiB used.
    """
    bytes: Optional[int]
    source: str


def _fetch_per_node_memory_limit(client: McpClient) -> MemoryLimitResult:
    """Resolve the effective per-node memory limit (bytes), source-tagged.

    Source priority (highest first):
      1. GENIE_TRINO_MEMORY_LIMIT_PER_NODE_BYTES env var — direct int bytes,
         > 0. Sam sets this to his cluster's query.max-memory-per-node. No
         round-trip when set+valid.
      2. SHOW SESSION best-effort — row Name == 'query_max_memory_per_node'
         (the genuine per-node session property, present on older Trino).
         Value column, falling back to Default column when Value is empty.
      3. MemoryLimitResult(None, "default-fallback") → _memory_pressure_threshold
         falls back to HIGH_PEAK_MEMORY_BYTES (1 GiB). Behaviorally identical
         to today — no regression.

    INVARIANT: query_max_memory (total cross-cluster budget) is NEVER read,
    NEVER used as a proxy. Using the total would make the threshold N× too
    large → memory-pressure fires N× less often → signal silently weakened.
    Match is on the EXACT name only — no prefix or partial match (D02).

    Read at CALL TIME (not module load) so Sam can export the env before a
    run and tests can monkeypatch os.environ. Never raises; failure-safe.
    """
    import os  # local import — research.py does NOT import os at module level

    _bad_env = False

    # Source 1: env override (no round-trip)
    env_val = os.environ.get("GENIE_TRINO_MEMORY_LIMIT_PER_NODE_BYTES")
    if env_val:
        try:
            parsed = int(env_val)
            if parsed > 0:
                return MemoryLimitResult(bytes=parsed, source="env")
            else:
                _bad_env = True  # ≤ 0 → fall through
        except (ValueError, TypeError):
            _bad_env = True  # non-int → fall through to SHOW SESSION (C03)

    # Source 2: best-effort SHOW SESSION — per-node property only
    show_session_result: Optional[int] = None
    try:
        result = _execute_via_mcp(client, "SHOW SESSION")
        if result.get("error"):
            if _bad_env:
                return MemoryLimitResult(bytes=None, source="bad_env_fallthrough")
            return MemoryLimitResult(bytes=None, source="default-fallback")
        for row in result.get("rows") or []:
            if not isinstance(row, dict):
                continue
            row_lower = {k.lower(): v for k, v in row.items()}
            if str(row_lower.get("name", "")).strip().lower() == "query_max_memory_per_node":
                val = str(row_lower.get("value", "")).strip()
                if not val:
                    val = str(row_lower.get("default", "")).strip()  # D08 value→default fallback
                show_session_result = _parse_trino_datasize(val)
                break
    except Exception:
        pass  # transport error / CandidateTimeoutError / any non-row shape → fallback

    if _bad_env:
        return MemoryLimitResult(bytes=show_session_result, source="bad_env_fallthrough")

    if show_session_result is not None:
        return MemoryLimitResult(bytes=show_session_result, source="show_session")

    # Source 3: no usable value
    return MemoryLimitResult(bytes=None, source="default-fallback")


def _generate_table_suggestions(metadata: list[TableMetadata]) -> list[TableSuggestion]:
    """Analyze table metadata and generate optimization suggestions."""
    suggestions: list[TableSuggestion] = []

    for meta in metadata:
        fqn = f"{meta.catalog}.{meta.schema}.{meta.table_name}"

        # ── Partition analysis ──
        partitioning = meta.properties.get("partitioning", "")
        if not partitioning or partitioning == "[]":
            # Check for date/timestamp columns that could be partition keys
            date_cols = [
                c for c in meta.columns
                if any(t in c.data_type.lower() for t in ["date", "timestamp"])
            ]
            if date_cols:
                col_names = ", ".join(c.column_name for c in date_cols[:3])
                suggestions.append(TableSuggestion(
                    table=fqn,
                    category="partition",
                    suggestion=(
                        f"No partitioning detected. Consider partitioning by "
                        f"date/timestamp column(s): {col_names}. "
                        f"This enables partition pruning and reduces scan volume."
                    ),
                    suggestion_zh=(
                        f"未偵測到分區設定。建議使用日期/時間戳記欄位進行分區："
                        f"{col_names}。"
                        f"啟用分區裁剪可大幅減少掃描量。"
                    ),
                    severity="warning",
                ))

        # ── Bucketing analysis ──
        bucket_count = meta.properties.get("bucket_count", "")
        if not bucket_count or bucket_count == "0":
            id_cols = [
                c for c in meta.columns
                if any(k in c.column_name.lower() for k in ["_id", "id", "_key", "key"])
                and c.data_type.lower() in ("integer", "bigint", "varchar")
            ]
            if id_cols:
                col_names = ", ".join(c.column_name for c in id_cols[:2])
                suggestions.append(TableSuggestion(
                    table=fqn,
                    category="bucket",
                    suggestion=(
                        f"No bucketing configured. For tables frequently joined on "
                        f"{col_names}, bucketing can improve join performance "
                        f"by enabling bucket-pruned joins."
                    ),
                    suggestion_zh=(
                        f"未設定分桶。若此表經常以 {col_names} 進行 JOIN，"
                        f"建議設定 bucketing 以啟用分桶裁剪，提升 JOIN 效能。"
                    ),
                    severity="info",
                ))

        # ── Data type analysis ──
        for col in meta.columns:
            dtype = col.data_type.lower()
            # varchar without length → potential issue
            if dtype == "varchar" and col.column_name.lower().endswith(("_id", "_code", "_type")):
                suggestions.append(TableSuggestion(
                    table=fqn,
                    category="data_type",
                    suggestion=(
                        f"Column '{col.column_name}' is varchar (unbounded). "
                        f"Consider varchar(N) with explicit length for ID/code columns "
                        f"to improve memory estimation and query planning."
                    ),
                    suggestion_zh=(
                        f"欄位 '{col.column_name}' 使用無限長度 varchar。"
                        f"建議 ID/代碼欄位使用 varchar(N) 指定長度，"
                        f"有助於記憶體估算與查詢規劃。"
                    ),
                    severity="info",
                ))
            # double where decimal might be better
            if dtype == "double" and any(
                k in col.column_name.lower()
                for k in ["amount", "price", "cost", "revenue", "balance"]
            ):
                suggestions.append(TableSuggestion(
                    table=fqn,
                    category="data_type",
                    suggestion=(
                        f"Column '{col.column_name}' uses DOUBLE. "
                        f"For financial/monetary data, consider DECIMAL(p,s) "
                        f"to avoid floating-point precision issues."
                    ),
                    suggestion_zh=(
                        f"欄位 '{col.column_name}' 使用 DOUBLE 型別。"
                        f"財務/金額資料建議改用 DECIMAL(p,s)，"
                        f"避免浮點數精度問題。"
                    ),
                    severity="warning",
                ))

        # ── Sort order analysis ──
        sort_order = meta.properties.get("sorted_by", meta.properties.get("sort_order", ""))
        if not sort_order or sort_order == "[]":
            if len(meta.columns) > 10:
                suggestions.append(TableSuggestion(
                    table=fqn,
                    category="sort",
                    suggestion=(
                        f"No sort order configured on a wide table ({len(meta.columns)} columns). "
                        f"Setting a sort order on frequently filtered columns "
                        f"can improve min/max predicate pushdown and file skipping."
                    ),
                    suggestion_zh=(
                        f"寬表（{len(meta.columns)} 欄位）未設定排序。"
                        f"建議對經常用於篩選的欄位設定排序，"
                        f"可提升 min/max 述詞下推與檔案跳過效率。"
                    ),
                    severity="info",
                ))

    return suggestions


@dataclass
class ExplainAnalyzeResult:
    """Parsed EXPLAIN ANALYZE output from Trino."""
    raw_text: str
    stages: list[dict] = field(default_factory=list)
    total_cpu_ms: float = 0.0
    total_wall_ms: float = 0.0
    total_memory_bytes: int = 0
    total_input_rows: int = 0
    total_output_rows: int = 0
    available: bool = True


# ---------------------------------------------------------------------------
# EXPLAIN ANALYZE helpers
# ---------------------------------------------------------------------------

def _fetch_explain_analyze(
    client: McpClient,
    sql: str,
    timeout_ms: Optional[float] = None,
    label: str = "explain analyze",
) -> ExplainAnalyzeResult:
    """Run EXPLAIN ANALYZE via MCP and parse the output.

    Returns ExplainAnalyzeResult with available=False if the query fails
    (e.g. MCP server doesn't support EXPLAIN ANALYZE, or the query errors out).
    This is the fallback-safe path — never raises.
    """
    _assert_executable_read_only(sql)
    explain_sql = f"EXPLAIN ANALYZE {sql}"
    try:
        result = _execute_via_mcp(client, explain_sql, timeout_ms=timeout_ms, label=label)
        if result.get("error"):
            return ExplainAnalyzeResult(
                raw_text=str(result["error"]),
                available=False,
            )

        # EXPLAIN ANALYZE returns text rows, not tabular data
        rows = result.get("rows", [])
        raw_lines = []
        for row in rows:
            if isinstance(row, dict):
                # Trino returns single-column result with plan text
                line = str(next(iter(row.values()), ""))
            else:
                line = str(row)
            raw_lines.append(line)

        raw_text = "\n".join(raw_lines)
        if not raw_text.strip():
            raw_text = result.get("raw", "")

        # Parse stage-level metrics from EXPLAIN ANALYZE output
        stages = _parse_explain_stages(raw_text)

        # Aggregate totals
        total_cpu = sum(s.get("cpu_ms", 0) for s in stages)
        total_wall = sum(s.get("wall_ms", 0) for s in stages)
        total_mem = max((s.get("memory_bytes", 0) for s in stages), default=0)
        total_input = sum(s.get("input_rows", 0) for s in stages)
        total_output = sum(s.get("output_rows", 0) for s in stages)

        return ExplainAnalyzeResult(
            raw_text=raw_text,
            stages=stages,
            total_cpu_ms=total_cpu,
            total_wall_ms=total_wall,
            total_memory_bytes=total_mem,
            total_input_rows=total_input,
            total_output_rows=total_output,
            available=True,
        )
    except CandidateTimeoutError:
        raise
    except Exception as exc:
        return ExplainAnalyzeResult(
            raw_text=f"EXPLAIN ANALYZE failed: {exc}",
            available=False,
        )


def _parse_explain_stages(text: str) -> list[dict]:
    """Extract stage-level metrics from Trino EXPLAIN ANALYZE text output.

    Trino EXPLAIN ANALYZE output contains lines like:
        Fragment 1 [HASH]
            CPU: 1.23s, Scheduled: 2.00s, Blocked: ...
            Input: 1000 rows (50kB), Output: 100 rows (5kB)
            ...

    This parser extracts what it can and is lenient about format changes.
    """
    stages: list[dict] = []
    current_stage: dict | None = None

    for line in text.split("\n"):
        stripped = line.strip()

        # Detect stage/fragment boundaries
        fragment_match = re.match(r"(?:Fragment|Stage)\s+(\d+)", stripped, re.IGNORECASE)
        if fragment_match:
            if current_stage:
                stages.append(current_stage)
            current_stage = {"id": int(fragment_match.group(1))}
            continue

        if current_stage is None:
            continue

        # First-match-wins per stage: fragment-level metrics appear before
        # nested operator metrics; we don't want operator-level zeros to
        # overwrite the aggregated fragment values.
        time_units_to_ms = {
            "ns": 1 / 1_000_000,
            "us": 1 / 1000,
            "µs": 1 / 1000,
            "ms": 1.0,
            "s": 1000.0,
            "min": 60_000.0,
            "h": 3_600_000.0,
        }
        time_unit_re = r"(ns|us|µs|ms|s|min|h)"

        # CPU: "CPU: 52.94us" / "CPU: 1.23s" / "CPU: 123ms"
        if "cpu_ms" not in current_stage:
            cpu_match = re.search(rf"CPU:\s*([\d.]+)\s*{time_unit_re}\b", stripped, re.IGNORECASE)
            if cpu_match:
                val = float(cpu_match.group(1))
                unit = cpu_match.group(2).lower()
                current_stage["cpu_ms"] = val * time_units_to_ms.get(unit, 1.0)

        # Wall / Scheduled
        if "wall_ms" not in current_stage:
            wall_match = re.search(rf"(?:Scheduled|Wall):\s*([\d.]+)\s*{time_unit_re}\b", stripped, re.IGNORECASE)
            if wall_match:
                val = float(wall_match.group(1))
                unit = wall_match.group(2).lower()
                current_stage["wall_ms"] = val * time_units_to_ms.get(unit, 1.0)

        # Memory: "Peak Memory: 1.5MB" / "Memory: 132B"
        if "memory_bytes" not in current_stage:
            mem_match = re.search(r"(?:Peak\s+)?Memory:\s*([\d.]+)\s*(B|KB|MB|GB|TB)", stripped, re.IGNORECASE)
            if mem_match:
                val = float(mem_match.group(1))
                unit = mem_match.group(2).upper()
                multiplier = {"B": 1, "KB": 1024, "MB": 1024**2, "GB": 1024**3, "TB": 1024**4}
                current_stage["memory_bytes"] = int(val * multiplier.get(unit, 1))

        # Rows: "Input: 1000 rows" / "Output: 100 rows"
        if "input_rows" not in current_stage:
            input_match = re.search(r"Input:\s*([\d,]+)\s*rows?", stripped, re.IGNORECASE)
            if input_match:
                current_stage["input_rows"] = int(input_match.group(1).replace(",", ""))

        if "output_rows" not in current_stage:
            output_match = re.search(r"Output:\s*([\d,]+)\s*rows?", stripped, re.IGNORECASE)
            if output_match:
                current_stage["output_rows"] = int(output_match.group(1).replace(",", ""))

    if current_stage:
        stages.append(current_stage)

    return stages


# ---------------------------------------------------------------------------
# MCP execution helpers
# ---------------------------------------------------------------------------

_resolved_tool: tuple[str, str] | None = None


def _resolve_query_tool(client: McpClient) -> tuple[str, str]:
    """Find the MCP tool that executes SQL queries. Returns (tool_name, sql_param_name)."""
    global _resolved_tool
    if _resolved_tool:
        return _resolved_tool
    tools = client.list_tools()
    candidates = ("query", "trino_query", "execute", "execute_query", "run_query")
    for t in tools:
        if t["name"] in candidates:
            param = _find_sql_param(t)
            _resolved_tool = (t["name"], param)
            return _resolved_tool
    for t in tools:
        param = _find_sql_param(t)
        if param:
            _resolved_tool = (t["name"], param)
            return _resolved_tool
    available = [t["name"] for t in tools]
    raise McpError(-1, f"No SQL query tool found on MCP server. Available tools: {available}")


def _find_sql_param(tool_def: dict) -> str:
    """Detect the SQL parameter name from a tool's input schema."""
    props = tool_def.get("inputSchema", {}).get("properties", {})
    for name in ("sql", "query", "statement"):
        if name in props:
            return name
    return "sql"


def _execute_via_mcp(client: McpClient, sql: str, timeout_ms: Optional[float] = None, label: str = "candidate") -> dict:
    """Execute SQL via MCP server, return parsed result with timing."""
    tool_name, sql_param = _resolve_query_tool(client)
    t0 = time.monotonic()
    try:
        kwargs = {"timeout": timeout_ms / 1000.0} if timeout_ms is not None else {}
        raw = client.call_tool(tool_name, {sql_param: sql}, **kwargs)
    except requests.exceptions.Timeout as exc:
        raise CandidateTimeoutError(timeout_ms or 0, label) from exc
    elapsed_ms = (time.monotonic() - t0) * 1000

    # Parse the response — MCP tools return text content
    try:
        data = json.loads(raw)
    except (json.JSONDecodeError, TypeError):
        data = {"text": raw}

    # Extract metrics from response if available
    metrics = RunMetrics(query_time_ms=elapsed_ms)
    if isinstance(data, dict):
        if "metrics" in data and isinstance(data["metrics"], dict):
            m = data["metrics"]
            metrics.cpu_time_ms = float(m.get("cpu_time_ms", m.get("cpuTimeMillis", 0)))
            metrics.wall_time_ms = float(m.get("wall_time_ms", m.get("wallTimeMillis", 0)))
            metrics.peak_memory_bytes = int(m.get("peak_memory_bytes", m.get("peakMemoryBytes", 0)))
            metrics.physical_input_bytes = int(m.get("physical_input_bytes", m.get("physicalInputBytes", 0)))
            metrics.processed_rows = int(m.get("processed_rows", m.get("processedRows", 0)))
            metrics.total_splits = int(m.get("total_splits", m.get("totalSplits", 0)))
        if "duration_ms" in data:
            metrics.query_time_ms = float(data["duration_ms"])

    # Extract rows and columns. Two response shapes seen in the wild:
    #   (a) {"rows": [...], "columns": [...], "metrics": {...}, ...}  — wrapped
    #   (b) [{"col1": val, ...}, ...]                                  — bare list
    # mcp-trino returns (b); previous code silently dropped to rows=[].
    if isinstance(data, dict):
        rows = data.get("rows", [])
        columns = data.get("columns", [])
        error = data.get("error")
    elif isinstance(data, list):
        rows = data
        columns = list(rows[0].keys()) if rows and isinstance(rows[0], dict) else []
        error = None
    else:
        rows = []
        columns = []
        error = None

    return {
        "rows": rows,
        "columns": columns,
        "row_count": len(rows),
        "metrics": metrics,
        "error": error,
        "raw": raw,
    }


def _build_mcp_explain_runner(client: McpClient):
    """Return an `(sql) -> str | None` callable that runs EXPLAIN (FORMAT JSON).

    Reuses the resolved query tool — most mcp-trino servers do not expose a
    dedicated explain tool, so EXPLAIN is issued as an ordinary statement and
    the raw plan text is pulled from the response. Returns None on any failure
    so callers can treat plan-cost as best-effort.
    """
    def _runner(s: str) -> Optional[str]:
        _assert_executable_read_only(s)
        try:
            result = _execute_via_mcp(client, f"EXPLAIN (FORMAT JSON) {s}")
        except Exception:
            return None
        if result.get("error"):
            return None
        rows = result.get("rows") or []
        # EXPLAIN (FORMAT JSON) returns a single-cell row holding the plan text.
        if rows:
            first = rows[0]
            if isinstance(first, dict):
                for val in first.values():
                    if isinstance(val, str) and val.strip():
                        return val
            elif isinstance(first, (list, tuple)) and first:
                if isinstance(first[0], str):
                    return first[0]
            elif isinstance(first, str):
                return first
        raw = result.get("raw")
        return raw if isinstance(raw, str) else None
    return _runner


def _render_mcp_plan_skeleton(client: McpClient, sql: str) -> str:
    """Condensed EXPLAIN (FORMAT JSON) skeleton of `sql` for prompt injection.

    Planner-only round-trip (no execution). Best-effort and fail-open: returns
    "" when the plan is unavailable so callers can gate injection on
    truthiness — same convention as ``format_directions_for_prompt``.
    """
    from genie.skills.mcp_trino.preflight import plan_cost
    from genie.skills.trino_query.plan_render import render_plan_skeleton

    try:
        _, _, plan = plan_cost(sql, _build_mcp_explain_runner(client))
        return render_plan_skeleton(plan)
    except Exception:
        return ""


def _format_plan_skeleton_block(skeleton: str, *, label: str) -> str:
    """Wrap a non-empty skeleton in its prompt block; "" passes through."""
    if not skeleton:
        return ""
    return (
        f"{label} (condensed EXPLAIN — operator tree, join distribution, "
        f"per-node estimates; volatile fields dropped):\n"
        f"```text\n{skeleton}\n```"
    )


def _format_hotspot_stages(
    explain: "ExplainAnalyzeResult | None", *, limit: int = 3
) -> str:
    """Render the baseline's top-CPU EXPLAIN ANALYZE stages as a prompt block.

    Runtime ground truth for targeting rewrites — estimates can mislead when
    statistics are missing; measured stage CPU cannot. Returns "" when stage
    data is unavailable or carries no usable CPU numbers (truthiness gating).
    """
    if explain is None or not explain.available or not explain.stages:
        return ""
    ranked = sorted(
        (s for s in explain.stages if isinstance(s, dict)),
        key=lambda s: float(s.get("cpu_ms", 0) or 0),
        reverse=True,
    )[:limit]
    if not ranked or all(float(s.get("cpu_ms", 0) or 0) <= 0 for s in ranked):
        return ""

    total_cpu = float(explain.total_cpu_ms or 0)
    lines = [
        f"Baseline EXPLAIN ANALYZE hotspots (top {len(ranked)} stage(s) by "
        f"measured CPU — target rewrites at these):"
    ]
    for s in ranked:
        cpu = float(s.get("cpu_ms", 0) or 0)
        pct = f" ({cpu / total_cpu * 100:.0f}% of total)" if total_cpu > 0 else ""
        bits = [f"cpu={cpu:.0f}ms{pct}"]
        if s.get("wall_ms") is not None:
            bits.append(f"wall={float(s['wall_ms']):.0f}ms")
        if s.get("input_rows") is not None:
            bits.append(f"input={s['input_rows']:,} rows")
        if s.get("output_rows") is not None:
            bits.append(f"output={s['output_rows']:,} rows")
        if s.get("memory_bytes"):
            bits.append(f"peak_mem={s['memory_bytes'] / (1024 ** 2):.1f}MB")
        lines.append(f"- Stage {s.get('id', '?')}: " + ", ".join(bits))
    return "\n".join(lines)


def _assemble_mcp_directions(
    client, sql, static_report, *,
    peak_memory_bytes=None,
    table_metadata=None,
    peak_memory_limit_bytes=None,   # NEW — per-node limit in bytes (None → 1 GiB fallback)
    explain_cost=None,              # injectable pre-fetched plan_cost 3-tuple (shared EXPLAIN)
):
    """Gather diagnostics → ranked directions at zero query-execution cost.

    Single source of truth for the four ``_assemble_mcp_directions`` call sites
    (success path, long-query gate-trip, ``--diagnose-only``, per-iteration re-diagnosis).
    EXPLAIN (FORMAT JSON) plans the query without running it; metadata is a
    cheap catalog round-trip. Returns ``(directions, table_metadata)`` so the
    success path can reuse the fetched metadata for its post-loop block.
    """
    from genie.skills.mcp_trino.pre_execution_diagnosis import pre_execution_diagnosis
    from .preflight import plan_cost

    if table_metadata is not None:
        # Reuse already-fetched metadata (same tables across a rewrite) to avoid
        # a redundant catalog round-trip on per-iteration re-diagnosis (v32 T1).
        pre_table_metadata = list(table_metadata)
    else:
        pre_table_metadata = []
        diag_refs = [(c, s, t) for (c, s, t) in _extract_table_names(sql) if c and s]
        if diag_refs:
            try:
                pre_table_metadata = _fetch_table_metadata(client, diag_refs)
            except Exception:
                pre_table_metadata = []

    # explain_cost may be injected by callers that already ran the EXPLAIN
    # round-trip for this SQL — one planner call instead of one per consumer.
    if explain_cost is None:
        try:
            explain_cost = plan_cost(sql, _build_mcp_explain_runner(client))
        except Exception:
            explain_cost = None

    directions = pre_execution_diagnosis(
        sql,
        static_report=static_report,
        explain_cost=explain_cost,
        table_metadata=pre_table_metadata or None,
        peak_memory_bytes=peak_memory_bytes,
        peak_memory_limit_bytes=peak_memory_limit_bytes,   # NEW
    )
    return directions, pre_table_metadata


def _measure_mcp(client: McpClient, sql: str, metric_key: str,
                  runs: int, capture_rows: bool = False,
                  max_capture_rows: int = 100_000,
                  output=None, label: str = "query",
                  timeout_ms: Optional[float] = None) -> MeasureResult:
    """Run SQL `runs` times via MCP, return median metric + all data.

    If captured row count exceeds max_capture_rows, rows are truncated to
    max_capture_rows to protect against OOM. Caller should treat the truncation
    as best-effort: equivalence comparison becomes partial.

    When `output` is passed and the sink supports `status()`, each run shows
    a live spinner so the user sees progress during long verify loops.
    """
    samples = []
    all_metrics = []
    last_rows = []
    last_columns = []
    row_count = 0

    for i in range(runs):
        run_label = f"{label}: run {i + 1}/{runs}"
        if timeout_ms is not None:
            run_label = f"{run_label} limit={timeout_ms / 1000.0:.1f}s"
        if output and hasattr(output, "status"):
            with output.status(run_label):
                result = _execute_via_mcp(client, sql, timeout_ms=timeout_ms, label=label)
        else:
            result = _execute_via_mcp(client, sql, timeout_ms=timeout_ms, label=label)
        if result["error"]:
            raise RuntimeError(f"MCP query failed: {result['error']}")

        metrics = result["metrics"]

        # Server-stat backfill: some MCP servers (e.g. mcp-trino) execute the
        # query but don't populate structured per-run stats — they return only
        # the rows, so query_time_ms (Python-measured) is non-zero but every
        # server-side field reads as 0. Fall back to a single EXPLAIN ANALYZE
        # round and parse the stage totals so the optimizer can rank candidates.
        if metrics.cpu_time_ms == 0 and metrics.peak_memory_bytes == 0:
            ea_label = f"{label}: explain-analyze backfill {i + 1}/{runs}"
            if timeout_ms is not None:
                ea_label = f"{ea_label} limit={timeout_ms / 1000.0:.1f}s"
            # The EXPLAIN ANALYZE backfill only ENRICHES cpu/memory stats; it
            # re-runs the query with instrumentation and is normally slower than
            # the plain run, so it must NOT inherit the candidate kill-timeout —
            # otherwise a slow backfill rejects an otherwise-valid candidate
            # (and the primary metric, query_time_ms, is already measured above).
            # Keep the plain-run metrics if the backfill times out or errors.
            try:
                if output and hasattr(output, "status"):
                    with output.status(ea_label):
                        ea = _fetch_explain_analyze(
                            client, sql, timeout_ms=timeout_ms, label=f"{label} explain-analyze backfill",
                        )
                else:
                    ea = _fetch_explain_analyze(
                        client, sql, timeout_ms=timeout_ms, label=f"{label} explain-analyze backfill",
                    )
            except CandidateTimeoutError:
                ea = ExplainAnalyzeResult(raw_text="backfill exceeded timeout", available=False)
                if output:
                    output.progress(
                        "    [dim]explain-analyze backfill skipped (over candidate timeout) "
                        "— ranking on measured query_time_ms[/dim]"
                    )
            if ea.available:
                metrics.cpu_time_ms = ea.total_cpu_ms
                metrics.wall_time_ms = ea.total_wall_ms
                metrics.peak_memory_bytes = ea.total_memory_bytes
                metrics.processed_rows = ea.total_input_rows

        value = getattr(metrics, metric_key, metrics.query_time_ms)
        samples.append(float(value))
        all_metrics.append(metrics)
        row_count = result["row_count"]

        if capture_rows and i == runs - 1:
            raw_rows = result["rows"] or []
            if len(raw_rows) > max_capture_rows:
                last_rows = raw_rows[:max_capture_rows]
            else:
                last_rows = raw_rows
            last_columns = result["columns"]

    median_val = statistics.median(samples)
    median_idx = min(range(len(samples)), key=lambda i: abs(samples[i] - median_val))

    if not capture_rows:
        capture_status, completeness = "not_captured", "not_captured"
    elif row_count > max_capture_rows:
        capture_status, completeness = "truncated", "unverified_received_envelope"
    else:
        capture_status, completeness = "complete", "unverified_received_envelope"
    return MeasureResult(
        median_metric=median_val,
        samples=samples,
        row_count=row_count,
        observed_row_count=row_count,
        rows=last_rows,
        captured_row_count=len(last_rows),
        max_capture_rows=max_capture_rows,
        capture_status=capture_status,
        completeness=completeness,
        columns=last_columns,
        metrics=all_metrics[median_idx],
    )


def _measure_mcp_logical_sql(client: McpClient, logical_sql: str, metric_key: str,
                             runs: int, *, policy: ExecutionPolicy, **kwargs) -> MeasureResult:
    """Validate logical SQL then derive a fresh MCP execution statement."""
    _assert_executable_read_only(logical_sql)
    execution_sql = _execution_sql_for(logical_sql, policy)
    result = _measure_mcp(client, execution_sql, metric_key, runs, **kwargs)
    # Preserve logical/execution provenance without changing MCP public delivery.
    result.logical_sql = logical_sql
    result.execution_sql = execution_sql
    result.safe_limit = policy.safe_limit
    return result


def _correctness_authorized(baseline: MeasureResult, candidate: MeasureResult) -> bool:
    return (
        baseline.capture_status == candidate.capture_status == "complete"
        and baseline.completeness == candidate.completeness == "verified_complete"
    )


def _incomplete_history(*, iteration: int, baseline: MeasureResult,
                        candidate: MeasureResult, base_sql: str,
                        candidate_sql: str, metric: float, delta: float) -> dict:
    """Build the sole persisted authorization-failure history representation."""
    return {
        "iteration": iteration,
        "status": "equivalence_unverified_incomplete_result",
        "rejection_reason": _incomplete_rejection_reason(baseline, candidate),
        "metric": metric,
        "delta": delta,
        "base_sql": base_sql,
        "candidate_sql": candidate_sql,
        "baseline_capture_status": baseline.capture_status,
        "candidate_capture_status": candidate.capture_status,
        "baseline_completeness": baseline.completeness,
        "candidate_completeness": candidate.completeness,
    }


def _incomplete_rejection_reason(baseline: MeasureResult, candidate: MeasureResult) -> str:
    """Return one of the complete, canonical incomplete-result reasons."""
    def kind(item):
        if item.completeness == "direct_truncated":
            return "direct_truncated"
        if item.capture_status == "not_captured":
            return "capture_not_captured"
        if item.capture_status == "truncated":
            return "capture_truncated"
        if item.completeness == "unverified_received_envelope":
            return "upstream_completeness_unverified"
        if item.capture_status == "complete" and item.completeness == "verified_complete":
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


# ---------------------------------------------------------------------------
# Row-equivalence comparator (Run A extension)
# ---------------------------------------------------------------------------

# Sentinel strings for non-finite floats.
# CRITICAL: must NOT be None — json.dumps(None) == "null", indistinguishable
# from Python None.  Sentinels serialize as JSON strings, keeping NaN/Inf/None
# distinct.  Symmetric: NaN==NaN, Inf==Inf, NaN!=Inf, NaN!=None.
_NAN_SENTINEL = "__NAN__"
_INF_SENTINEL = "__INF__"
_NEG_INF_SENTINEL = "__NEG_INF__"

# Float precision constant (6dp matches trino_query/research.py)
_FLOAT_PRECISION = 6


def _re_normalize_value(v: object) -> object:
    """Normalize a single cell value for stable JSON serialization.

    - float NaN  -> _NAN_SENTINEL  (NOT None/null)
    - float +Inf -> _INF_SENTINEL  (NOT None/null)
    - float -Inf -> _NEG_INF_SENTINEL
    - finite float -> round to _FLOAT_PRECISION decimal places
    - all other types -> unchanged
    """
    if isinstance(v, float):
        if math.isnan(v):
            return _NAN_SENTINEL
        if math.isinf(v):
            return _INF_SENTINEL if v > 0 else _NEG_INF_SENTINEL
        return round(v, _FLOAT_PRECISION)
    return v


def _re_normalize_row(row: object, exclude_indices: tuple) -> str:
    """Serialize one row to a stable JSON string for comparison.

    - dict rows: normalize float values; apply positional exclusion by the dict's
      insertion-order (Python 3.7+ dicts are ordered), then sort_keys=True for the
      remaining keys so the comparison is order-independent across rows.
      exclude_indices is 0-based over the dict's key-insertion order.
    - list/tuple rows: drop exclude_indices positions, normalize floats.
    - other: json.dumps with default=str.
    """
    if isinstance(row, dict):
        if exclude_indices:
            normalized = {
                k: _re_normalize_value(v)
                for i, (k, v) in enumerate(row.items())
                if i not in exclude_indices
            }
        else:
            normalized = {k: _re_normalize_value(v) for k, v in row.items()}
        return json.dumps(normalized, sort_keys=True, default=str)
    if isinstance(row, (list, tuple)):
        items = []
        for i, v in enumerate(row):
            if i in exclude_indices:
                continue
            items.append(_re_normalize_value(v))
        return json.dumps(items, default=str)
    return json.dumps(row, default=str)


@dataclass(frozen=True)
class EquivDiff:
    """Rich result from rows_equivalent."""
    equivalent: bool
    reason: str                   # exact legacy _results_equivalent string (verbatim)
    row_count_a: int
    row_count_b: int
    mismatches: int               # # differing entries after exclusion+normalize+sort
    first_mismatch: Optional[str] # first differing sorted entry ("a vs b"); None if equivalent
    excluded_columns: tuple       # 0-based indices actually removed before compare


def rows_equivalent(
    rows_a: list,
    rows_b: list,
    exclude_columns: tuple = (),
) -> tuple[bool, EquivDiff]:
    """Order-independent row-equivalence with optional column exclusion.

    Args:
        rows_a: baseline result rows (list of dicts or list/tuple rows)
        rows_b: candidate result rows
        exclude_columns: 0-based column indices to drop from BOTH sides before
            comparison (for non-deterministic columns like timestamps/UUIDs).
            Out-of-range indices are silently ignored.

    Returns:
        (equivalent: bool, EquivDiff)

    Reason strings are VERBATIM legacy _results_equivalent strings:
        "both empty", "exact match",
        "row count differs: {a} vs {b}",
        "{n} row(s) differ",
        "row count after normalize: {a} vs {b}"

    first_mismatch is an ADDITIVE diagnostic field — it is not folded into reason.
    Never raises.
    """
    count_a = len(rows_a)
    count_b = len(rows_b)

    # Build the set of indices actually present in any row (dicts + list/tuples both count)
    if exclude_columns:
        max_len = max(
            (max((len(r) for r in rows_a if isinstance(r, (list, tuple, dict))), default=0),
             max((len(r) for r in rows_b if isinstance(r, (list, tuple, dict))), default=0))
        )
        actually_excluded: tuple = tuple(
            i for i in exclude_columns if i < max_len
        )
    else:
        actually_excluded = ()

    if count_a != count_b:
        return False, EquivDiff(
            equivalent=False,
            reason=f"row count differs: {count_a} vs {count_b}",
            row_count_a=count_a,
            row_count_b=count_b,
            mismatches=0,
            first_mismatch=None,
            excluded_columns=actually_excluded,
        )

    if not rows_a:
        return True, EquivDiff(
            equivalent=True,
            reason="both empty",
            row_count_a=0,
            row_count_b=0,
            mismatches=0,
            first_mismatch=None,
            excluded_columns=actually_excluded,
        )

    exclude_set = frozenset(exclude_columns)

    set_a = sorted(_re_normalize_row(r, exclude_set) for r in rows_a)
    set_b = sorted(_re_normalize_row(r, exclude_set) for r in rows_b)

    mismatches = sum(1 for a, b in zip(set_a, set_b) if a != b)

    first_mismatch: Optional[str] = None
    for a, b in zip(set_a, set_b):
        if a != b:
            first_mismatch = f"{a} vs {b}"
            break

    if mismatches > 0:
        return False, EquivDiff(
            equivalent=False,
            reason=f"{mismatches} row(s) differ",
            row_count_a=count_a,
            row_count_b=count_b,
            mismatches=mismatches,
            first_mismatch=first_mismatch,
            excluded_columns=actually_excluded,
        )

    if len(set_a) != len(set_b):
        return False, EquivDiff(
            equivalent=False,
            reason=f"row count after normalize: {len(set_a)} vs {len(set_b)}",
            row_count_a=count_a,
            row_count_b=count_b,
            mismatches=0,
            first_mismatch=None,
            excluded_columns=actually_excluded,
        )

    return True, EquivDiff(
        equivalent=True,
        reason="exact match",
        row_count_a=count_a,
        row_count_b=count_b,
        mismatches=0,
        first_mismatch=None,
        excluded_columns=actually_excluded,
    )


def _results_equivalent(rows_a: list, rows_b: list) -> tuple[bool, str]:
    """Backward-compat shim — delegates to rows_equivalent without exclusions.

    The four existing call sites (lines 1242, 1283, 2175, 2301) are
    behaviorally untouched until a caller opts in by using rows_equivalent
    directly with exclude_columns.
    """
    equiv, diff = rows_equivalent(rows_a, rows_b)
    return equiv, diff.reason


# ---------------------------------------------------------------------------
# §3.1 Seed-candidate evaluation — pure decision, no I/O
# ---------------------------------------------------------------------------

class _SeedVerdict:
    """Return value from _evaluate_seed_candidate."""
    __slots__ = ("accepted", "winner_sql", "winner_metric", "winner_rows", "reason")

    def __init__(
        self,
        accepted: bool,
        winner_sql: str,
        winner_metric: float,
        winner_rows: list,
        reason: str,
    ) -> None:
        self.accepted = accepted
        self.winner_sql = winner_sql
        self.winner_metric = winner_metric
        self.winner_rows = winner_rows
        self.reason = reason


def _evaluate_seed_candidate(
    original_sql: str,
    recomposed_sql: str,
    baseline_metric: float,
    baseline_rows: list,
    seed_metric: float,
    seed_rows: list,
) -> "_SeedVerdict":
    """Pure §3.1 seed-validation decision — no I/O, no side effects.

    Rules (§3.1 NORMATIVE):
      - winner_sql and winner_metric are ALWAYS updated together.
      - A seed is accepted iff rows are equivalent AND seed is strictly faster.
      - An equivalent-but-slower seed is explicitly rejected (SCR-1).
      - A row-divergent seed is rejected (SEED_REJECTED).

    Returns a _SeedVerdict whose ``accepted`` flag signals the branch taken.
    Both the MCP standard loop and the --direct standard loop call this helper
    so the invariant is pinned in one place and covered by a single test suite.
    """
    equiv, equiv_reason = _results_equivalent(baseline_rows, seed_rows)
    if equiv and seed_metric < baseline_metric:
        return _SeedVerdict(
            accepted=True,
            winner_sql=recomposed_sql,
            winner_metric=seed_metric,
            winner_rows=seed_rows,
            reason="accepted",
        )
    reason = (
        f"row-equiv failed: {equiv_reason}"
        if not equiv
        else f"not faster: {seed_metric:.1f} >= {baseline_metric:.1f}"
    )
    return _SeedVerdict(
        accepted=False,
        winner_sql=original_sql,
        winner_metric=baseline_metric,
        winner_rows=baseline_rows,
        reason=reason,
    )


# ---------------------------------------------------------------------------
# §3.1 Single-call-site seed orchestrator — produce + measure + decide
# ---------------------------------------------------------------------------

def _seed_decompose_and_select(
    original_sql: str,
    baseline_measure: "MeasureResult",
    *,
    produce_fn: "Callable[[str], tuple[str, list, list, object]]",
    measure_fn: "Callable[[str], MeasureResult]",
    flag_enabled: bool,
    output=None,
    trace: "Optional[list]" = None,
    rejection_history: "Optional[Callable[[dict], None]]" = None,
) -> "tuple[str, MeasureResult, list]":
    """§3.1 NORMATIVE single locus: decompose → measure → decide, coupled.

    Returns ``(winner_sql, winner_measure, step_events)`` as a coupled triple.
    ``winner_sql`` and ``winner_measure`` are ALWAYS from the same decision arm —
    they can NEVER be decoupled by an in-between assignment.

    Args:
        original_sql: the unmodified query.
        baseline_measure: the already-measured baseline ``MeasureResult``.
        produce_fn: ``(sql) -> (recomposed_sql, frags, cands, rr)`` —
            wraps ``_produce_decompose_candidate`` with bound LLM/cost args.
        measure_fn: ``(sql) -> MeasureResult`` — wraps ``_measure_mcp`` or a
            dict→MeasureResult adapter for the --direct path.
        flag_enabled: ``GENIE_V48_SEED_DECOMPOSE`` guard — when False, returns
            ``(original_sql, baseline_measure, [])`` immediately without calling
            produce_fn or measure_fn (no LLM calls, no Trino round-trips).
        output: progress sink (optional).
        trace: ``StepTrace`` list to append events into (optional).
        rejection_history: optional sink for the canonical persisted record when
            provenance cannot authorize the seed comparison.

    T-SYM: both the MCP STANDARD site and the --direct STANDARD site call this
    function; it is the only place where the §3.1 coupled assignment lives.
    """
    if not flag_enabled:
        return original_sql, baseline_measure, []

    events: list = [] if trace is None else trace

    try:
        recomposed_sql, _frags, _cands, _rr = produce_fn(original_sql)

        # Fix 3 (v50): emit compact TUI breadcrumb after trace is populated.
        # Only fires when trace contains decompose/fragment events (i.e. decompose
        # actually ran). Both MCP (:2568) and --direct (:1231) standard loops reach
        # this locus — no edit to trino_query/research.py is needed.
        # v51 TODO: wire breadcrumb for plan-cost loop (:1470) and no-data paths too.
        try:
            if output and events:
                has_decompose_events = any(
                    ev.step_id == "decompose" or ev.step_id.startswith("fragment_")
                    for ev in events
                )
                if has_decompose_events:
                    from genie.output.step_trace import render_tui
                    output.progress(render_tui(events))
        except Exception:
            pass  # guard: never raise from display layer

        if recomposed_sql == original_sql:
            # Decompose produced no change — skip measure, keep original.
            return original_sql, baseline_measure, events

        seed_measure = measure_fn(recomposed_sql)

        # A faster received/partial sample cannot replace the coupled baseline
        # winner. This helper serves both adapters, so inspect the canonical
        # provenance fields rather than treating row equality as authorization.
        if not _correctness_authorized(baseline_measure, seed_measure):
            reason = _incomplete_rejection_reason(baseline_measure, seed_measure)
            # The seed has no loop iteration number; 0 unambiguously identifies
            # this pre-iteration comparison while preserving the canonical shape.
            if rejection_history is not None:
                rejection_history(_incomplete_history(
                    iteration=0, baseline=baseline_measure, candidate=seed_measure,
                    base_sql=original_sql, candidate_sql=recomposed_sql,
                    metric=seed_measure.median_metric,
                    delta=seed_measure.median_metric - baseline_measure.median_metric,
                ))
            if output:
                output.progress(
                    "  [seed] decompose→recompose not accepted "
                    f"({reason})"
                )
            return original_sql, baseline_measure, events

        verdict = _evaluate_seed_candidate(
            original_sql, recomposed_sql,
            baseline_measure.median_metric, baseline_measure.rows,
            seed_measure.median_metric, seed_measure.rows,
        )

        if verdict.accepted:
            if output:
                output.progress(
                    f"  [seed] decompose→recompose accepted: "
                    f"{baseline_measure.median_metric:.1f} → {seed_measure.median_metric:.1f}"
                )
            # §3.1 COUPLED return: winner_sql and winner_measure move together.
            return recomposed_sql, seed_measure, events
        else:
            if output:
                output.progress(
                    f"  [seed] decompose→recompose not accepted ({verdict.reason})"
                )
            # §3.1 COUPLED return: original SQL with its baseline measure.
            return original_sql, baseline_measure, events

    except Exception as exc:
        if output:
            output.progress(f"  [seed] decompose failed (degraded): {exc}")
        return original_sql, baseline_measure, events


# ---------------------------------------------------------------------------
# Long-query plan-cost loop (MCP parity)
# ---------------------------------------------------------------------------

def _run_mcp_plan_cost_loop(
    *,
    client: McpClient,
    provider,
    model: str,
    reasoning: str,
    original_sql: str,
    metric_key: str,
    max_iterations: int,
    verify_runs: int,
    output,
    build_prompt: Callable[..., str] | None,
    baseline: MeasureResult,
    static_report,
    explain_runner: Callable[[str], Optional[str]],
    max_fallbacks: int,
    candidate_timeout_ms: Optional[float] = None,
    peak_memory_limit_bytes: Optional[int] = None,   # NEW
    execution_policy: ExecutionPolicy | None = None,
) -> EnhancementReport:
    """Plan-cost ranking + L1 structural guard + K-retry for the MCP path.

    Delegates the iteration + verification loop to _plan_cost_loop_core (shared
    with the direct path). This adapter is responsible for:
    - baseline metric extraction (MeasureResult fields)
    - candidate_timeout_ms derivation from MCP run metrics
    - plan_cost call and directions assembly
    - building sys_prompt (including directions_block from pre_execution_diagnosis)
    - wrapping output in _SafeOutput
    - building the four injected callables (measure_fn, metric_fn, row_equiv_fn, explain_runner)
    - reconstructing EnhancementReport from _PlanCostCoreResult fields
    """
    from genie.skills.mcp_trino.pre_execution_diagnosis import (
        format_directions_for_prompt,
        pre_execution_diagnosis,
    )
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
    baseline_metric = baseline.median_metric
    if candidate_timeout_ms is None:
        # Use the LARGER of the measured end-to-end query time and the (often 0
        # or tiny, EXPLAIN-stage-derived) wall time, so the kill-timeout basis
        # never under-estimates how long baseline actually took.
        baseline_wall_ms = float(max(
            baseline.metrics.query_time_ms or 0,
            baseline.metrics.wall_time_ms or 0,
        ))
        candidate_timeout_ms = make_candidate_timeout_ms(baseline_wall_ms) if baseline_wall_ms > 0 else None

    _assert_executable_read_only(original_sql)
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
        peak_memory_bytes=getattr(baseline.metrics, "peak_memory_bytes", 0) or None,
        peak_memory_limit_bytes=peak_memory_limit_bytes,   # NEW
    )
    rule_gate = build_rule_gate_summary(static_report, directions)
    rule_gate_block = format_rule_gate_for_prompt(rule_gate)
    directions_block = format_directions_for_prompt(directions)

    if output:
        output.print("")
        timeout_text = (
            f", candidate_timeout={candidate_timeout_ms / 1000.0:.1f}s"
            if candidate_timeout_ms is not None else ""
        )
        output.progress(
            f"  [long-query] MCP plan-cost loop active "
            f"(baseline rows~{baseline_rows_est}, bytes~{baseline_bytes_est}, "
            f"max_fallbacks={max_fallbacks}{timeout_text})"
        )
        render_rule_gate_summary(output, rule_gate)

    # Condensed skeleton of the already-fetched baseline plan (no extra
    # EXPLAIN round-trip): full optimizer-chosen tree beyond what the ranked
    # directions capture. Fail-open ("" → not injected).
    from genie.skills.trino_query.plan_render import render_plan_skeleton
    skeleton_block = _format_plan_skeleton_block(
        render_plan_skeleton(baseline_plan), label="Baseline plan skeleton"
    )

    # D12: build_prompt handled here before sys_prompt; core is opaque to it.
    skill_prompt = build_prompt(True, model) if build_prompt else ""
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

    # ── MCP adapter closures (step 9 of §2.1 reconstruction) ──
    measure_fn = lambda sql, label: _measure_mcp_logical_sql(
        client, sql, metric_key, verify_runs, policy=policy,
        capture_rows=True, output=output, label=label,
        timeout_ms=candidate_timeout_ms,
    )
    metric_fn = lambda m: m.median_metric
    # D8 pre-check preserved: MCP path checks row_count before rows content.
    def row_equiv_fn(measured):
        if not _correctness_authorized(baseline, measured):
            return False, _incomplete_rejection_reason(baseline, measured)
        if baseline.row_count != measured.row_count:
            return False, f"row count differs: {baseline.row_count} vs {measured.row_count}"
        return _results_equivalent(baseline.rows, measured.rows)

    # ── v48 T6: Decompose-seed validation for MCP plan-cost loop ──
    # Guard: disabled by GENIE_V48_SEED_DECOMPOSE=0 for test/debugging isolation.
    import os as _os_v48_pcl
    _v48_pcl_seed_enabled = _os_v48_pcl.environ.get("GENIE_V48_SEED_DECOMPOSE", "1") != "0"

    # v57: fragment rewrite opt-in (same env vars as standard loop)
    _v57_pcl_frag_rewrite = _os_v48_pcl.environ.get("GENIE_FRAGMENT_REWRITE", "0") == "1"
    _v57_pcl_frag_cap = max(1, int(_os_v48_pcl.environ.get("GENIE_FRAGMENT_REWRITE_CAP", "5")))

    from genie.output.step_trace import StepTrace as _PCL_StepTrace
    _pcl_step_trace: _PCL_StepTrace = []
    # Seed provenance is checked before ranked verification. Retain its canonical
    # failure record so it is persisted in the MCP iteration history as well.
    _pcl_seed_rejection_history: list[dict] = []
    _pcl_seed_sql = original_sql
    _pcl_seed_baseline = baseline
    if _v48_pcl_seed_enabled:
        try:
            from genie.skills.mcp_trino.write_analysis import _make_advisory_llm_fn as _pcl_llm_fn_factory
            from genie.skills.mcp_trino.write_analysis import _advisory_cost_reader as _pcl_cost_reader
            if provider is not None:
                _pcl_llm_fn = _pcl_llm_fn_factory(provider, model, reasoning)
                _pcl_recomposed, _pcl_frags, _pcl_cands, _pcl_rr = _produce_decompose_candidate(
                    original_sql, _pcl_llm_fn, _pcl_cost_reader,
                    run_static_gates=False, step_trace=_pcl_step_trace,
                    enable_fragment_rewrite=_v57_pcl_frag_rewrite,
                    max_fragment_model_calls=_v57_pcl_frag_cap,
                )
                if _pcl_recomposed != original_sql:
                    _pcl_seed_meas = _measure_mcp_logical_sql(
                        client, _pcl_recomposed, metric_key, verify_runs, policy=policy,
                        capture_rows=True, output=output, label="seed",
                        timeout_ms=candidate_timeout_ms,
                    )
                    if _correctness_authorized(baseline, _pcl_seed_meas):
                        _pcl_seed_equiv, _pcl_seed_reason = _results_equivalent(
                            baseline.rows, _pcl_seed_meas.rows
                        )
                    else:
                        _pcl_seed_equiv = False
                        _pcl_seed_reason = _incomplete_rejection_reason(baseline, _pcl_seed_meas)
                        # Envelope equality cannot authorize this seed. Persist one
                        # canonical record and retain the original coupled winner.
                        _pcl_seed_rejection_history.append(_incomplete_history(
                            iteration=0, baseline=baseline, candidate=_pcl_seed_meas,
                            base_sql=original_sql, candidate_sql=_pcl_recomposed,
                            metric=_pcl_seed_meas.median_metric,
                            delta=_pcl_seed_meas.median_metric - baseline_metric,
                        ))
                    _pcl_seed_faster = _pcl_seed_meas.median_metric < baseline_metric
                    try:
                        from genie.output.step_trace import StepEvent as _PCL_StepEvent, StepStatus as _PCL_StepStatus
                        from genie.skills.mcp_trino.strategy_verify import (
                            LiveEvidence as _PCL_LiveEvidence,
                            build_evidence_coverage as _pcl_build_coverage,
                            coverage_summary_line as _pcl_coverage_summary_line,
                            evidence_coverage_enabled as _pcl_coverage_enabled,
                            verify_p9_fanout as _pcl_verify_p9_fanout,
                        )
                        if _pcl_coverage_enabled():
                            _pcl_reject_reason = None
                            if not _pcl_seed_equiv:
                                _pcl_reject_reason = "not equivalent"
                            elif not _pcl_seed_faster:
                                _pcl_reject_reason = "not faster"
                            _pcl_fanout = _pcl_verify_p9_fanout(original_sql, _pcl_recomposed)
                            _pcl_cov = _pcl_build_coverage(
                                strategy_id="P9",
                                fanout_result=_pcl_fanout,
                                p9_claimed=_pcl_recomposed != original_sql,
                                has_correlated_exists=bool(_pcl_fanout.correlation_keys),
                                live_result=_PCL_LiveEvidence(
                                    row_equivalent=_pcl_seed_equiv,
                                    faster=_pcl_seed_faster,
                                    metric_before=baseline_metric,
                                    metric_after=_pcl_seed_meas.median_metric,
                                    reason=_pcl_reject_reason,
                                ),
                            )
                            _pcl_step_trace.append(_PCL_StepEvent(
                                step_id="verify",
                                stage="Evidence Coverage",
                                status=_PCL_StepStatus.RAN,
                                applicable=True,
                                tui_headline=_pcl_cov.ship_status.value,
                                detail={"evidence_coverage_summary": _pcl_coverage_summary_line(_pcl_cov)},
                            ))
                    except Exception:
                        pass
                    if _pcl_seed_equiv and _pcl_seed_faster:
                        _pcl_seed_sql = _pcl_recomposed
                        _pcl_seed_baseline = _pcl_seed_meas
                        if output:
                            output.progress(
                                f"  [seed] decompose→recompose accepted (plan-cost loop): "
                                f"{baseline_metric:.1f} → {_pcl_seed_meas.median_metric:.1f}"
                            )
                    else:
                        _pcl_seed_sql = original_sql
        except Exception as _pcl_seed_exc:
            if output:
                output.progress(f"  [seed] decompose failed in plan-cost loop (degraded): {_pcl_seed_exc}")

    def incomplete_history_fn(measured, ranked):
        if _correctness_authorized(baseline, measured):
            return None
        return _incomplete_history(
            iteration=ranked["iteration"], baseline=baseline, candidate=measured,
            base_sql=original_sql, candidate_sql=ranked["sql"],
            metric=measured.median_metric,
            delta=measured.median_metric - baseline_metric,
        )

    result = _plan_cost_loop_core(
        provider=provider,
        model=model,
        reasoning=reasoning,
        sys_prompt=sys_prompt,
        original_sql=_pcl_seed_sql,
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
        empty_message=None,             # MCP uses core default
        incomplete_history_fn=incomplete_history_fn,
    )

    # Persist a pre-ranked seed authorization failure alongside L3 history.
    # This leaves the core winner untouched, preserving baseline coupling.
    if _pcl_seed_rejection_history:
        result.history[:0] = _pcl_seed_rejection_history

    # ── Reconstruct EnhancementReport from _PlanCostCoreResult ──
    # (spec §1.6 step 9 — MCP-specific path; NamedTuple fields only)
    best_sql = original_sql
    best_measure = baseline
    best_value = baseline_metric
    if result.winner_sql is not None:
        best_sql = result.winner_sql
        best_measure = result.winner_measure
        best_value = best_measure.median_metric

    iterations_records = []
    for h in result.history:
        if h.get("status") == "equivalence_unverified_incomplete_result":
            # Preserve the complete canonical persisted history shape from L3.
            iterations_records.append(IterationRecord(
                iteration=h["iteration"], status=h["status"],
                metric_value=h["metric"], delta=h["delta"],
                hypothesis=h["rejection_reason"], sql=h["candidate_sql"],
                rejection_reason=h["rejection_reason"], base_sql=h["base_sql"],
                candidate_sql=h["candidate_sql"],
                baseline_capture_status=h["baseline_capture_status"],
                candidate_capture_status=h["candidate_capture_status"],
                baseline_completeness=h["baseline_completeness"],
                candidate_completeness=h["candidate_completeness"],
            ))
            continue

        # Shared plan-cost records contain ranking facts, not a measurement.
        # Keep those fields unavailable instead of manufacturing a baseline
        # metric and zero delta. Any record that actually carries metric/delta
        # remains representable without status-specific special cases.
        has_measurement = "metric" in h and "delta" in h
        iterations_records.append(IterationRecord(
            iteration=h["iteration"],
            status="improved" if (result.winner_sql is not None and h.get("candidate_sql") == result.winner_sql) else h["status"],
            metric_value=h.get("metric") if has_measurement else None,
            delta=h.get("delta") if has_measurement else None,
            hypothesis="(plan-cost-loop)", sql=h.get("candidate_sql") or "",
        ))

    if not _correctness_authorized(baseline, best_measure):
        final_equiv, final_reason = False, _incomplete_rejection_reason(baseline, best_measure)
    elif baseline.row_count != best_measure.row_count:
        final_equiv = False
        final_reason = f"row count differs: {baseline.row_count} vs {best_measure.row_count}"
    else:
        final_equiv, final_reason = _results_equivalent(baseline.rows, best_measure.rows)

    improvement_abs = best_value - baseline_metric
    improvement_pct = (improvement_abs / baseline_metric * 100) if baseline_metric else 0.0
    qualified_refs = [(c, s, t) for (c, s, t) in _extract_table_names(original_sql) if c and s]

    report = EnhancementReport(
        timestamp=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        original_sql=original_sql,
        original_result_sample=baseline.rows[:10],
        original_columns=baseline.columns,
        original_row_count=baseline.row_count,
        original_metrics=baseline.metrics,
        enhanced_sql=best_sql,
        enhanced_result_sample=best_measure.rows[:10],
        enhanced_columns=best_measure.columns,
        enhanced_row_count=best_measure.row_count,
        enhanced_metrics=best_measure.metrics,
        metric_key=metric_key,
        baseline_value=baseline_metric,
        best_value=best_value,
        improvement_abs=improvement_abs,
        improvement_pct=improvement_pct,
        iterations=iterations_records,
        data_consistent=final_equiv,
        data_consistency_reason=final_reason,
        mcp_server_url=client.config.url,
        verify_runs=verify_runs,
        table_suggestions=[],
        had_qualified_tables=bool(qualified_refs),
        original_explain=None,
        enhanced_explain=None,
        step_trace=_pcl_step_trace,
    )

    _render_summary_card(
        output,
        baseline_value=baseline_metric,
        best_value=best_value,
        metric_key=metric_key,
        improvement_abs=improvement_abs,
        improvement_pct=improvement_pct,
        data_consistent=final_equiv,
        data_consistency_reason=final_reason,
        iterations_ran=len(iterations_records),
    )

    return report




# ---------------------------------------------------------------------------
# Report generation (fixed format)
# ---------------------------------------------------------------------------

_LABELS_EN = {
    "title": "Trino Query Enhancement Report",
    "meta": "Meta",
    "perf": "Performance Comparison",
    "summary": "Summary",
    "iter_history": "Iteration History",
    "orig_sql": "Original SQL",
    "orig_result": "Original Result (sample)",
    "enh_sql": "Enhanced SQL",
    "enh_result": "Enhanced Result (sample)",
    "table_suggestions": "Table Structure Suggestions",
    "field": "Field",
    "value": "Value",
    "metric": "Metric",
    "original": "Original",
    "enhanced": "Enhanced",
    "delta": "Delta",
    "change_pct": "Change %",
    "round": "Round",
    "status": "Status",
    "metric_value": "Metric Value",
    "hypothesis": "Hypothesis",
    "baseline": "Baseline",
    "best": "Best",
    "improvement": "Improvement",
    "orig_rows": "Original Row Count",
    "enh_rows": "Enhanced Row Count",
    "data_consistent": "Data Consistent",
    "consistency_detail": "Consistency Detail",
    "lower_better": "lower is better",
    "median": "median",
    "no_improve": "no improvement found — original SQL unchanged",
    "no_data": "no data",
    "sample_note": "First 10 rows of the query output, used to visually spot-check that the enhanced SQL preserves the original result set.",
    "generated_by": "Generated by genieCLI mcp_trino research — Lakehouse Team —",
    "unqualified_tables_note": "No fully-qualified tables (catalog.schema.table) found in the SQL, so information_schema metadata was not fetched. Use qualified names (e.g. `hive.default.orders`) or set a default catalog/schema to enable this section.",
    "table": "Table",
    "category": "Category",
    "severity": "Severity",
    "suggestion": "Suggestion",
    "no_suggestions": "No table structure issues detected.",
    "explain_analyze": "EXPLAIN ANALYZE",
    "explain_original": "Original Query Plan",
    "explain_enhanced": "Enhanced Query Plan",
    "explain_unavailable": "EXPLAIN ANALYZE data not available.",
    "stage": "Stage",
    "cpu_ms": "CPU (ms)",
    "wall_ms": "Wall (ms)",
    "memory": "Memory",
    "input_rows": "Input Rows",
    "output_rows": "Output Rows",
    "timestamp": "Timestamp",
    "mcp_server": "MCP Server",
    "target_metric": "Target Metric",
    "verify_runs": "Verify Runs",
    "iterations": "Iterations",
    "result_validation": "Result Validation",
}

_LABELS_ZH = {
    "title": "Trino 查詢優化報告",
    "meta": "基本資訊",
    "perf": "效能比較",
    "summary": "摘要",
    "iter_history": "迭代歷程",
    "orig_sql": "原始 SQL",
    "orig_result": "原始結果（樣本）",
    "enh_sql": "優化後 SQL",
    "enh_result": "優化後結果（樣本）",
    "table_suggestions": "表結構優化建議",
    "field": "欄位",
    "value": "值",
    "metric": "指標",
    "original": "原始",
    "enhanced": "優化後",
    "delta": "差異",
    "change_pct": "變化 %",
    "round": "輪次",
    "status": "狀態",
    "metric_value": "指標值",
    "hypothesis": "假說",
    "baseline": "基線",
    "best": "最佳",
    "improvement": "改善",
    "orig_rows": "原始列數",
    "enh_rows": "優化後列數",
    "data_consistent": "資料一致性",
    "consistency_detail": "一致性細節",
    "lower_better": "越低越好",
    "median": "中位數",
    "no_improve": "未找到改善方案 — 原始 SQL 不變",
    "no_data": "無資料",
    "sample_note": "查詢輸出的前 10 列，供目視比對優化後的 SQL 是否保留原始結果集。",
    "generated_by": "由 genieCLI mcp_trino research — Lakehouse Team — 產生於",
    "unqualified_tables_note": "SQL 中未發現完整限定名稱 (catalog.schema.table)，故未擷取 information_schema 中繼資料。請改用完整名稱（例如 `hive.default.orders`）或設定預設 catalog/schema 以啟用此區段。",
    "table": "表名",
    "category": "類別",
    "severity": "嚴重度",
    "suggestion": "建議",
    "no_suggestions": "未偵測到表結構問題。",
    "explain_analyze": "EXPLAIN ANALYZE",
    "explain_original": "原始查詢計畫",
    "explain_enhanced": "優化後查詢計畫",
    "explain_unavailable": "EXPLAIN ANALYZE 資料不可用。",
    "stage": "階段",
    "cpu_ms": "CPU (ms)",
    "wall_ms": "Wall (ms)",
    "memory": "記憶體",
    "input_rows": "輸入列數",
    "output_rows": "輸出列數",
    "timestamp": "時間戳記",
    "mcp_server": "MCP 伺服器",
    "target_metric": "目標指標",
    "verify_runs": "驗證次數",
    "iterations": "迭代次數",
    "result_validation": "結果驗證",
}


def _format_iteration_measurement(record: IterationRecord) -> tuple[str, str]:
    """Render optional measurement facts without inventing them for rankings."""
    def format_value(value: object, spec: str) -> str:
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            return "n/a"
        return format(value, spec)

    return (
        format_value(record.metric_value, ".1f"),
        format_value(record.delta, "+.1f"),
    )


def generate_report(report: EnhancementReport, locale: str = "en", step_trace=None) -> str:
    """Generate a fixed-format markdown report.

    This template is ALWAYS the same structure — sections, headers, and table
    columns never change between runs. Only the data values differ.

    Args:
        report: The enhancement report data.
        locale: "en" for English, "zh" for Traditional Chinese.
                SQL, metrics, and column names always stay English.
        step_trace: Optional StepTrace from v48 — appended as a step-level section.
    """
    L = _LABELS_ZH if locale == "zh" else _LABELS_EN
    lines = []

    def _fmt_ms(val: float) -> str:
        """Adaptive millisecond formatter.

        `:.0f` rounds sub-millisecond values to 0, which hides real data for
        fast queries (Trino 467 emits microseconds / nanoseconds for short
        queries). We scale decimal precision to magnitude.
        """
        if val is None:
            return "0"
        if val == 0:
            return "0"
        absv = abs(val)
        if absv < 0.001:
            return f"{val:.4f}"
        if absv < 1:
            return f"{val:.3f}"
        if absv < 100:
            return f"{val:.2f}"
        return f"{val:.0f}"

    # A report may claim full row-level equivalence only after the explicit
    # correctness gate has authorized and matched the comparison.
    if report.data_consistent:
        validation_detail = "full row-level equivalence check verified"
        sample_note = (
            "First 10 rows of the query output, displayed as a sample; "
            "full row-level equivalence was verified separately."
        )
    else:
        validation_detail = (
            "unverified/incomplete result — full row-level equivalence and "
            "semantic preservation were not authorized "
            f"(rejection reason: `{report.data_consistency_reason}`)"
        )
        sample_note = (
            "First 10 rows of the received query-output envelope, shown for "
            "diagnostic inspection only; semantic preservation is unverified."
        )

    # ── Header ──
    lines.append(f"# {L['title']}")
    lines.append("")
    lines.append(f"## {L['meta']}")
    lines.append("")
    lines.append(f"| {L['field']} | {L['value']} |")
    lines.append("|-------|-------|")
    lines.append(f"| {L['timestamp']} | {report.timestamp} |")
    lines.append(f"| {L['mcp_server']} | {report.mcp_server_url} |")
    lines.append(f"| {L['target_metric']} | {report.metric_key} ({L['lower_better']}) |")
    lines.append(f"| {L['verify_runs']} | {report.verify_runs} ({L['median']}) |")
    lines.append(f"| {L['iterations']} | {len(report.iterations)} |")
    lines.append(f"| {L['result_validation']} | {validation_detail} |")
    lines.append("")

    # ── Performance Comparison ──
    lines.append(f"## {L['perf']}")
    lines.append("")
    lines.append(f"| {L['metric']} | {L['original']} | {L['enhanced']} | {L['delta']} | {L['change_pct']} |")
    lines.append("|--------|----------|----------|-------|----------|")

    for attr in ["query_time_ms", "cpu_time_ms", "wall_time_ms"]:
        orig = getattr(report.original_metrics, attr, 0)
        enh = getattr(report.enhanced_metrics, attr, 0)
        delta = enh - orig
        pct = (delta / orig * 100) if orig else 0
        lines.append(
            f"| {attr} | {_fmt_ms(orig)} | {_fmt_ms(enh)} | {_fmt_ms(delta)} | {pct:+.1f}% |"
        )

    for attr in ["processed_rows", "total_splits", "peak_memory_bytes", "physical_input_bytes"]:
        orig = getattr(report.original_metrics, attr, 0)
        enh = getattr(report.enhanced_metrics, attr, 0)
        delta = enh - orig
        pct = (delta / orig * 100) if orig else 0
        lines.append(f"| {attr} | {orig} | {enh} | {delta:+} | {pct:+.1f}% |")

    lines.append("")

    # ── Summary ──
    lines.append(f"## {L['summary']}")
    lines.append("")
    lines.append(f"| {L['field']} | {L['value']} |")
    lines.append("|-------|-------|")
    lines.append(f"| {L['baseline']} ({report.metric_key}) | {report.baseline_value:.1f} |")
    lines.append(f"| {L['best']} ({report.metric_key}) | {report.best_value:.1f} |")
    lines.append(f"| {L['improvement']} | {report.improvement_abs:+.1f} ({report.improvement_pct:+.1f}%) |")
    lines.append(f"| {L['orig_rows']} | {report.original_row_count} |")
    lines.append(f"| {L['enh_rows']} | {report.enhanced_row_count} |")
    lines.append(f"| {L['data_consistent']} | {'YES' if report.data_consistent else 'NO'} |")
    lines.append(f"| {L['consistency_detail']} | {report.data_consistency_reason} |")
    lines.append("")

    # ── Iteration History ──
    lines.append(f"## {L['iter_history']}")
    lines.append("")
    lines.append(f"| {L['round']} | {L['status']} | {L['metric_value']} | {L['delta']} | {L['hypothesis']} |")
    lines.append("|-------|--------|-------------|-------|------------|")

    for it in report.iterations:
        metric_value, delta = _format_iteration_measurement(it)
        lines.append(
            f"| {it.iteration} | {it.status} | {metric_value} | "
            f"{delta} | {it.hypothesis[:60]} |"
        )

    lines.append("")

    # ── Original SQL ──
    lines.append(f"## {L['orig_sql']}")
    lines.append("")
    lines.append("```sql")
    lines.append(report.original_sql)
    lines.append("```")
    lines.append("")

    # ── Original Result (sample) ──
    lines.append(f"## {L['orig_result']}")
    lines.append("")
    lines.append(f"_{sample_note}_")
    lines.append("")
    if report.original_columns:
        lines.append("| " + " | ".join(report.original_columns) + " |")
        lines.append("| " + " | ".join("---" for _ in report.original_columns) + " |")
        for row in report.original_result_sample[:10]:
            if isinstance(row, dict):
                vals = [str(row.get(c, "")) for c in report.original_columns]
            else:
                vals = [str(v) for v in row]
            lines.append("| " + " | ".join(vals) + " |")
    else:
        lines.append(f"_({L['no_data']})_")
    lines.append("")

    # ── Enhanced SQL ──
    lines.append(f"## {L['enh_sql']}")
    lines.append("")
    if report.enhanced_sql != report.original_sql:
        lines.append("```sql")
        lines.append(report.enhanced_sql)
        lines.append("```")
    else:
        lines.append(f"_({L['no_improve']})_")
    lines.append("")

    # ── Enhanced Result (sample) ──
    lines.append(f"## {L['enh_result']}")
    lines.append("")
    lines.append(f"_{sample_note}_")
    lines.append("")
    if report.enhanced_columns:
        lines.append("| " + " | ".join(report.enhanced_columns) + " |")
        lines.append("| " + " | ".join("---" for _ in report.enhanced_columns) + " |")
        for row in report.enhanced_result_sample[:10]:
            if isinstance(row, dict):
                vals = [str(row.get(c, "")) for c in report.enhanced_columns]
            else:
                vals = [str(v) for v in row]
            lines.append("| " + " | ".join(vals) + " |")
    else:
        lines.append(f"_({L['no_data']})_")
    lines.append("")

    # ── Table Structure Suggestions ──
    lines.append(f"## {L['table_suggestions']}")
    lines.append("")
    if report.table_suggestions:
        lines.append(f"| {L['table']} | {L['category']} | {L['severity']} | {L['suggestion']} |")
        lines.append("|-------|----------|----------|------------|")
        for s in report.table_suggestions:
            text = s.suggestion_zh if locale == "zh" else s.suggestion
            lines.append(f"| {s.table} | {s.category} | {s.severity} | {text} |")
    elif report.had_qualified_tables:
        lines.append(f"_({L['no_suggestions']})_")
    else:
        lines.append(f"_{L['unqualified_tables_note']}_")
    lines.append("")

    # ── EXPLAIN ANALYZE ──
    lines.append(f"## {L['explain_analyze']}")
    lines.append("")

    def _render_explain(explain: ExplainAnalyzeResult | None, label: str) -> None:
        lines.append(f"### {label}")
        lines.append("")
        if explain is None or not explain.available:
            lines.append(f"_({L['explain_unavailable']})_")
            lines.append("")
            return
        if explain.stages:
            lines.append(f"| {L['stage']} | {L['cpu_ms']} | {L['wall_ms']} | {L['memory']} | {L['input_rows']} | {L['output_rows']} |")
            lines.append("|-------|---------|---------|--------|------------|-------------|")
            for s in explain.stages:
                mem = s.get("memory_bytes", 0)
                mem_str = f"{mem / 1024 / 1024:.1f}MB" if mem > 1024 * 1024 else f"{mem / 1024:.1f}KB" if mem > 1024 else f"{mem}B"
                lines.append(
                    f"| {s.get('id', '?')} "
                    f"| {_fmt_ms(s.get('cpu_ms', 0))} "
                    f"| {_fmt_ms(s.get('wall_ms', 0))} "
                    f"| {mem_str} "
                    f"| {s.get('input_rows', 0):,} "
                    f"| {s.get('output_rows', 0):,} |"
                )
            lines.append("")
            return
        # Stages not parseable (e.g. MCP server returned JSON or non-text plan).
        # Render a concise totals summary from whatever we captured, and hide
        # the raw dump to keep the report readable.
        mem = explain.total_memory_bytes or 0
        mem_str = f"{mem / 1024 / 1024:.1f}MB" if mem > 1024 * 1024 else f"{mem / 1024:.1f}KB" if mem > 1024 else f"{mem}B"
        lines.append(f"| {L['field']} | {L['value']} |")
        lines.append("|-------|-------|")
        lines.append(f"| {L['cpu_ms']} | {_fmt_ms(explain.total_cpu_ms)} |")
        lines.append(f"| {L['wall_ms']} | {_fmt_ms(explain.total_wall_ms)} |")
        lines.append(f"| {L['memory']} | {mem_str} |")
        lines.append(f"| {L['input_rows']} | {explain.total_input_rows:,} |")
        lines.append(f"| {L['output_rows']} | {explain.total_output_rows:,} |")
        lines.append("")
        lines.append(f"_Plan text was returned by the MCP server in a format this report cannot parse into stages. "
                     f"Run `EXPLAIN ANALYZE <sql>` directly for full detail._")
        lines.append("")

    _render_explain(report.original_explain, L["explain_original"])
    _render_explain(report.enhanced_explain, L["explain_enhanced"])

    # ── v48 Step Trace ──
    if step_trace:
        from genie.output.step_trace import render_report as _render_step_report
        step_section = _render_step_report(step_trace)
        if step_section.strip():
            lines.append("## Step Trace")
            lines.append("")
            lines.append(step_section)
            lines.append("")

    # ── Footer ──
    lines.append("---")
    lines.append(f"_{L['generated_by']} {report.timestamp}_")
    lines.append("")

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Decompose-seed helper (v48 T3)
# ---------------------------------------------------------------------------

def _produce_decompose_candidate(
    sql: str,
    llm_fn,
    cost_reader_fn,
    run_static_gates: bool,
    step_trace=None,
    *,
    enable_fragment_rewrite: bool = False,
    max_fragment_model_calls: int = 1,
):
    """Produce the optional decompose seed candidate.

    Executing read paths default to evidence-only decomposition: deterministic
    decorrelation may still apply, but per-fragment LLM ranking/rewrite is off
    unless explicitly enabled. Advisory paths can opt in to fragment rewrite.
    Never raises — degrades to returning (sql, [], [], rr) on any failure.
    Returns (recomposed_sql, fragments, candidates, rr).
    """
    from genie.skills.mcp_trino.trino_optimize import (
        decompose as _decompose,
        optimize as _optimize,
        recompose as _recompose,
        CpGuidance,
        RecomposeStatus,
        RewriteCandidate,
    )
    from genie.skills.trino_query.detection_scan import scan_sql as _scan_sql
    from genie.skills.mcp_trino.write_analysis import (
        _column_safe_candidates,
        _semantic_safe_candidates,
    )
    from genie.output.step_trace import StepEvent, StepStatus, CANONICAL_COPY

    try:
        # v51b PRE-PASS: decorrelate correlated EXISTS → semi-join IN (executing path only).
        # Advisory path (run_static_gates=True) skips this → falls through to v51a Priority 4.5 ADVISE.
        if not run_static_gates:
            from genie.skills.mcp_trino.trino_optimize import _try_decorrelate_exists
            decorr_sql = _try_decorrelate_exists(sql)
            if decorr_sql is not None and decorr_sql != sql:
                if step_trace is not None:
                    step_trace.append(StepEvent(
                        step_id="decorrelate",
                        stage="Decorrelate EXISTS→IN",
                        status=StepStatus.RAN,
                        applicable=True,
                        tui_headline="correlated EXISTS → semi-join IN (v51b)",
                        detail={
                            "action": "rewrite",
                            "changed": True,
                            "original_sql": sql,
                            "rewritten_sql": decorr_sql,
                        },
                    ))
                return decorr_sql, [], [], None

        # 1. Decompose (LLM monster ranking only when fragment rewrite is opted in)
        fragments = _decompose(sql, llm_fn, cost_reader_fn, use_llm_ranking=enable_fragment_rewrite)
        n = len(fragments)
        monster_ids = [f.fragment_id for f in fragments if f.is_monster]
        fragment_ids = [f.fragment_id for f in fragments]

        if n == 1 and not fragments[0].is_monster:
            decompose_headline = "1 fragment (whole query)"
        else:
            decompose_headline = f"{n} fragment(s) identified ({', '.join(fragment_ids)})"
            if monster_ids:
                decompose_headline += f" — monster(s): {', '.join(monster_ids)}"

        if step_trace is not None:
            step_trace.append(StepEvent(
                step_id="decompose",
                stage="Decompose",
                status=StepStatus.RAN,
                applicable=True,
                tui_headline=decompose_headline,
                detail={
                    "fragment_count": n,
                    "fragment_ids": fragment_ids,
                    "monster_ids": monster_ids,
                    "seed_changed": False,  # will update below
                },
            ))

        # v52: offline critical-path cost model (§7) — run AFTER decompose, before fragment optimize
        # v55: also build CpGuidance from the result so fragment optimize is bottleneck-aware.
        cp_guidance = None
        try:
            from genie.skills.mcp_trino.critical_path import analyze_critical_path as _analyze_cp
            from genie.output.step_trace import StepEvent, StepStatus
            cp_result = _analyze_cp(sql)
            # v55: surface the bottleneck + recommended strategy to fragment optimize.
            if cp_result.available and not cp_result.trivial and cp_result.bottleneck:
                _bn = cp_result.bottleneck
                _root_cost = cp_result.root.subtree_cost if cp_result.root else 0
                cp_guidance = CpGuidance(
                    bottleneck_label=_bn.label,
                    bottleneck_op=_bn.op,
                    bottleneck_cost_pct=(
                        round(100.0 * _bn.subtree_cost / _root_cost, 1) if _root_cost else 0.0
                    ),
                    recommended_strategy=_bn.strategy,
                )
            if step_trace is not None:
                if not cp_result.available:
                    cp_status = StepStatus.DEGRADED
                    cp_headline = f"offline cost model unavailable: {cp_result.reason}"
                    cp_detail: dict = {"available": False, "reason": cp_result.reason}
                elif cp_result.trivial:
                    cp_status = StepStatus.RAN
                    cp_headline = "no structural bottleneck identified"
                    cp_detail = {"available": True, "trivial": True}
                else:
                    bn = cp_result.bottleneck
                    bn_label = bn.label if bn else "unknown"
                    bn_cost_pct = (
                        round(100.0 * bn.subtree_cost / cp_result.root.subtree_cost, 1)
                        if (bn and cp_result.root and cp_result.root.subtree_cost)
                        else 0.0
                    )
                    cp_headline = f"hot node: {bn_label} ({bn_cost_pct}%)"
                    if bn and bn.strategy:
                        cp_headline += f" → {bn.strategy}"
                    cp_status = StepStatus.RAN
                    cp_detail = {
                        "available": True,
                        "trivial": False,
                        "bottleneck_label": bn_label,
                        "bottleneck_op": bn.op if bn else "",
                        "bottleneck_cost_pct": bn_cost_pct,
                        "strategy": bn.strategy if bn else None,
                        "offline_truth_ceiling": cp_result.offline_truth_ceiling,
                        "tied_paths": cp_result.tied_paths,
                        "critical_path": [
                            {"label": n.label, "op": n.op}
                            for n in cp_result.critical_path
                        ],
                    }
                step_trace.append(StepEvent(
                    step_id="critical_path",
                    stage="Critical Path (offline)",
                    status=cp_status,
                    applicable=True,
                    tui_headline=cp_headline,
                    detail=cp_detail,
                ))
        except Exception:
            pass  # never block the main path

        # 2. Fragment optimize — gated by enable_fragment_rewrite.
        # When off (default for read paths), all fragments are passthrough:
        # evidence is collected but no per-fragment LLM calls are made.
        _frag_cap = max_fragment_model_calls
        monsters = [fr for fr in fragments if fr.is_monster][:_frag_cap] if enable_fragment_rewrite else []
        over_cap_list = [fr for fr in fragments if fr.is_monster][_frag_cap:] if enable_fragment_rewrite else []
        over_cap_set = set(id(fr) for fr in over_cap_list)

        candidates = []
        frag_idx = 0
        for fr in fragments:
            frag_idx += 1
            is_over_cap = id(fr) in over_cap_set
            if enable_fragment_rewrite and fr in monsters:
                cand = _optimize(fr, llm_fn, cp_guidance=cp_guidance)
            else:
                # Non-monster, over-cap, or fragment rewrite disabled: passthrough
                _rationale = "passthrough (fragment rewrite disabled)" if not enable_fragment_rewrite else "passthrough (non-monster or over-cap)"
                cand = RewriteCandidate(
                    fragment_id=fr.fragment_id,
                    original_sql=fr.sql,
                    rewritten_sql=fr.sql,
                    action="unchanged",
                    changed=False,
                    admitted=True,
                    rationale=_rationale,
                )
            candidates.append(cand)

            if is_over_cap:
                ev_status = StepStatus.SKIPPED
                ev_detail = {
                    "fragment_id": fr.fragment_id,
                    "role": getattr(fr, "role", ""),
                    "is_monster": fr.is_monster,
                    "rank": getattr(fr, "monster_rank", None),
                    "action": "over_cap",
                    "changed": False,
                    "sql": fr.sql,
                    "col_gate_verdict": "off" if not run_static_gates else "n/a",
                    "sem_gate_verdict": "off" if not run_static_gates else "n/a",
                }
                ev_headline = f"{fr.fragment_id}: over-cap (not optimized)"
            else:
                if cand.changed and cand.admitted:
                    ev_status = StepStatus.RAN
                    ev_action = "optimized"
                elif cand.action == "advise" and not cand.changed:
                    # Change ⑤: honest action label for Priority 4.5 advisory.
                    # The non-monster passthrough builds its own RewriteCandidate(action="unchanged")
                    # and never calls optimize(), so its cand.action is "unchanged" — this elif
                    # fires ONLY for the Priority 4.5 correlated-exists-per-row path.
                    ev_status = StepStatus.SKIPPED
                    ev_action = "advise"
                else:
                    ev_status = StepStatus.SKIPPED
                    ev_action = "unchanged"
                ev_detail = {
                    "fragment_id": fr.fragment_id,
                    "role": getattr(fr, "role", ""),
                    "is_monster": fr.is_monster,
                    "rank": getattr(fr, "monster_rank", None),
                    "action": ev_action,
                    "changed": cand.changed,
                    "rationale": cand.rationale,
                    "sql": fr.sql,
                    "col_gate_verdict": "off" if not run_static_gates else "n/a",
                    "sem_gate_verdict": "off" if not run_static_gates else "n/a",
                }
                if cand.changed and cand.admitted:
                    ev_detail["original_sql"] = cand.original_sql
                    ev_detail["rewritten_sql"] = cand.rewritten_sql
                ev_headline = f"{fr.fragment_id}: {ev_action}"

            if step_trace is not None:
                step_trace.append(StepEvent(
                    step_id=f"fragment_{frag_idx}",
                    stage=f"Fragment {frag_idx}/{n}",
                    status=ev_status,
                    applicable=True,
                    tui_headline=ev_headline,
                    detail=ev_detail,
                ))

        # 3. Gate toggle (CRUX)
        col_reverted = []
        sem_reverted = []
        if run_static_gates:
            candidates, col_reverted = _column_safe_candidates(candidates)
            candidates, sem_reverted = _semantic_safe_candidates(candidates)
            # Update fragment StepEvents with gate verdicts
            if step_trace is not None:
                frag_events = [ev for ev in step_trace if ev.step_id.startswith("fragment_")]
                for ev in frag_events:
                    fid = ev.detail.get("fragment_id", "")
                    if fid in col_reverted:
                        ev.detail["col_gate_verdict"] = "reverted"
                        ev.detail["sem_gate_verdict"] = "n/a"
                        ev.detail["action"] = "reverted_by_col_gate"
                        ev.tui_headline = f"{fid}: reverted_by_col_gate"
                    elif fid in sem_reverted:
                        ev.detail["col_gate_verdict"] = "pass"
                        ev.detail["sem_gate_verdict"] = "reverted"
                        ev.detail["action"] = "reverted_by_sem_gate"
                        ev.tui_headline = f"{fid}: reverted_by_sem_gate"
                    elif (
                        ev.detail.get("col_gate_verdict") == "n/a"
                        and ev.detail.get("action") not in ("over_cap", "unchanged")
                    ):
                        ev.detail["col_gate_verdict"] = "pass"
                        ev.detail["sem_gate_verdict"] = "pass"

        # 4. Recompose
        rr = _recompose(sql, candidates, scan_fn=_scan_sql)

        applied_count = sum(1 for c in candidates if c.admitted and c.changed)
        status_val = rr.status.value if hasattr(rr.status, "value") else str(rr.status)

        recompose_parts = [f"{status_val} — {applied_count}/{n} fragments applied"]
        if col_reverted:
            recompose_parts.append(
                f"(col-gate reverted: {', '.join(col_reverted)}; "
                f"sem-gate reverted: {', '.join(sem_reverted)})"
            )
        elif sem_reverted:
            recompose_parts.append(
                f"(col-gate reverted: none; sem-gate reverted: {', '.join(sem_reverted)})"
            )

        if not rr.scan_ok_confident:
            recompose_headline = CANONICAL_COPY["RECOMPOSE_SCAN_INCONCLUSIVE"]
        else:
            recompose_headline = " ".join(recompose_parts)

        recompose_detail = {
            "status": status_val,
            "reverted_fragments": list(rr.reverted_fragments) if rr.reverted_fragments else [],
            "col_gate_reverted": col_reverted,
            "sem_gate_reverted": sem_reverted,
            "recomposed_sql": rr.sql if rr.sql != sql else None,
            "sql_changed": rr.sql != sql,
            "scan_ok_confident": rr.scan_ok_confident,
            "seed_changed": rr.sql != sql,
        }

        if step_trace is not None:
            step_trace.append(StepEvent(
                step_id="recompose",
                stage="Recompose",
                status=StepStatus.RAN,
                applicable=True,
                tui_headline=recompose_headline,
                detail=recompose_detail,
            ))
            # Update decompose event with seed_changed
            for ev in step_trace:
                if ev.step_id == "decompose":
                    ev.detail["seed_changed"] = rr.sql != sql
                    break

        return rr.sql, fragments, candidates, rr

    except Exception as exc:
        # Safe degrade: return original sql
        if step_trace is not None:
            step_trace.append(StepEvent(
                step_id="decompose",
                stage="Decompose",
                status=StepStatus.DEGRADED,
                applicable=True,
                tui_headline=f"decompose failed: {exc}",
                detail={"error": str(exc)},
            ))

        _fallback_sql = sql  # capture before class body (class scope doesn't close over locals)

        class _FallbackRR:
            sql = _fallback_sql
            reverted_fragments: tuple = ()
            scan_ok_confident = True
            class status:  # type: ignore[misc]
                value = "degraded"

        return sql, [], [], _FallbackRR()


# ---------------------------------------------------------------------------
# Enhancement loop
# ---------------------------------------------------------------------------

def run_mcp_enhancement(
    client: McpClient,
    sql: str,
    metric_key: str = "query_time_ms",
    max_iterations: int = 1,
    verify_runs: int = 3,
    provider=None,
    model: str = "",
    reasoning: str = "disable",
    output=None,
    build_prompt: Callable[..., str] | None = None,
    *,
    long_query_opt_in: bool = True,
    long_query_threshold_s: Optional[int] = None,
    max_fallbacks: Optional[int] = None,
    diagnose_only: bool = False,
    execution_policy: ExecutionPolicy | None = None,
) -> EnhancementReport:
    """Run the MCP-based query enhancement loop.

    Args:
        client: MCP client connected to the Trino server
        sql: Original SQL to enhance
        metric_key: Metric to optimize (default: query_time_ms)
        max_iterations: Number of enhancement rounds (default: 1)
        verify_runs: Runs per candidate for median measurement (default: 3)
        provider: LLM provider for generating SQL rewrites
        model: Model name
        reasoning: Reasoning mode
        output: OutputSink for progress messages
        build_prompt: Prompt builder function

    Returns:
        EnhancementReport with all results
    """
    from genie.core.provider import CompletionRequest
    from genie.session.manager import new_msg, new_session
    from genie.output.step_trace import StepTrace, render_report as _render_step_report

    # v48: step-level trace — populated as the loop runs; spliced into report at end
    _step_trace: StepTrace = []
    policy = execution_policy or ExecutionPolicy(None)

    if output:
        output.print("\n  [yellow]== MCP Trino Query Enhancement ==[/yellow]")
        output.progress(f"  Server: {client.config.url}")
        output.progress(f"  Metric: {metric_key} | Iterations: {max_iterations} | Verify: {verify_runs} runs")

    # ── Static analysis (cheap; runs in both has-data and no-data paths) ──
    from genie.skills.trino_query.sql_static import analyze as static_analyze
    from genie.skills.trino_query.sql_static import summary_line as _static_summary_line
    try:
        static_report = static_analyze(sql)
    except Exception as exc:
        if output:
            output.progress(f"  [warn] static analysis skipped: {exc}")
        static_report = None
    if output and static_report is not None:
        output.progress(f"  Static analysis: {_static_summary_line(static_report)}")

    from .preflight import (
        DEFAULT_LONG_QUERY_THRESHOLD_S,
        DEFAULT_MAX_FALLBACKS,
        LongQueryAbort,
        NoDataDetected,
        PreflightDecision,
        PreflightRoute,
        build_preflight_decision,
        check_long_query_gate,
        detect_no_data_reason,
        make_query_max_run_time_sql,
        plan_cost,
    )

    # ── Per-node memory limit (v34 residual #1): fetch once, thread to every
    #    pre_execution_diagnosis call. None → 1 GiB fallback. Never raises. ──
    _mem_limit = _fetch_per_node_memory_limit(client)
    memory_limit_bytes: Optional[int] = _mem_limit.bytes
    if output:
        if _mem_limit.source == "env":
            _limit_display = f"{memory_limit_bytes / 1024**3:.1f} GiB" if memory_limit_bytes else "?"
            output.progress(f"  memory limit  {_limit_display} (GENIE_TRINO_MEMORY_LIMIT_PER_NODE_BYTES)")
        elif _mem_limit.source == "bad_env_fallthrough":
            import os as _os_local
            _bad_val = _os_local.environ.get("GENIE_TRINO_MEMORY_LIMIT_PER_NODE_BYTES", "?")
            if memory_limit_bytes is not None:
                _limit_display = f"{memory_limit_bytes / 1024**3:.1f} GiB"
                output.progress(
                    f"  [yellow][warn] GENIE_TRINO_MEMORY_LIMIT_PER_NODE_BYTES={_bad_val!r} "
                    f"is not a valid positive integer — SHOW SESSION supplied "
                    f"query_max_memory_per_node={_limit_display}[/yellow]"
                )
            else:
                output.progress(
                    f"  [yellow][warn] GENIE_TRINO_MEMORY_LIMIT_PER_NODE_BYTES={_bad_val!r} "
                    f"is not a valid positive integer — SHOW SESSION did not provide a usable "
                    f"query_max_memory_per_node; using 1.0 GiB fallback[/yellow]"
                )
        elif _mem_limit.source == "show_session":
            _limit_display = f"{memory_limit_bytes / 1024**3:.1f} GiB" if memory_limit_bytes else "?"
            output.progress(f"  memory limit  {_limit_display} (SHOW SESSION query_max_memory_per_node)")
        else:  # default-fallback
            output.progress(
                "  memory limit  1.0 GiB (fallback — set "
                "GENIE_TRINO_MEMORY_LIMIT_PER_NODE_BYTES to your cluster's "
                "query.max-memory-per-node)"
            )

    # ── Pre-decision: fact computation ──
    _baseline = None
    _baseline_exc: BaseException | None = None
    _baseline_metrics = None
    if not diagnose_only:
        if output:
            output.progress("  Measuring baseline...")
        try:
            _baseline = _measure_mcp_logical_sql(
                client, sql, metric_key, verify_runs, policy=policy,
                capture_rows=True, output=output, label="baseline",
            )
            _baseline_metrics = _baseline.metrics
        except Exception as exc:
            _baseline_exc = exc
    _baseline_row_count = _baseline.row_count if _baseline else None

    _gate = None
    fallbacks = max_fallbacks if max_fallbacks is not None else DEFAULT_MAX_FALLBACKS
    mcp_explain_runner = _build_mcp_explain_runner(client)
    mcp_explain_available = False
    mcp_plan_seen_no_estimates = False
    if _baseline is not None:
        # Baseline progress prints (verbatim from current code)
        if output:
            output.progress(f"  Baseline {metric_key}: {_baseline.median_metric:.1f} (median of {verify_runs} runs)")
            output.progress(f"  Baseline rows: {_baseline.row_count}")
            output.print(f"    [dim]{_baseline.metrics.summary()}[/dim]")
            if static_report and static_report.findings:
                output.progress(
                    f"  Static analysis: {static_report.summary} "
                    f"({len(static_report.findings)} finding(s))"
                )
        threshold_s = long_query_threshold_s if long_query_threshold_s is not None else DEFAULT_LONG_QUERY_THRESHOLD_S
        _gate = check_long_query_gate(
            baseline_wall_ms=float(_baseline.metrics.wall_time_ms or _baseline.metrics.query_time_ms or 0),
            max_iterations=max_iterations, long_query_opt_in=long_query_opt_in,
            threshold_s=threshold_s, max_fallbacks=fallbacks)
        if _gate.ok and long_query_opt_in and max_iterations > 0:   # preserve guard
            try:
                rows_est, bytes_est, raw_plan = plan_cost(sql, mcp_explain_runner)
                mcp_explain_available = rows_est is not None or bytes_est is not None
                mcp_plan_seen_no_estimates = (not mcp_explain_available) and raw_plan is not None
            except Exception:
                mcp_explain_available = False

    decision = build_preflight_decision(
        diagnose_only=diagnose_only,
        baseline_row_count=_baseline_row_count,
        baseline_exc=_baseline_exc,
        gate=_gate,
        long_query_opt_in=long_query_opt_in,
        plan_cost_available=mcp_explain_available,
        seen_no_estimates=mcp_plan_seen_no_estimates,
        max_iterations=max_iterations,
    )

    # ── Consumption blocks — bodies verbatim from current code ──
    if decision.route == PreflightRoute.DIAGNOSE_ONLY:
        from genie.skills.mcp_trino.pre_execution_diagnosis import format_directions_report
        if output:
            output.progress("  Diagnose only: EXPLAIN-cost + static + metadata, no query execution")
        directions, _ = _assemble_mcp_directions(
            client, sql, static_report,
            peak_memory_bytes=None,
            peak_memory_limit_bytes=memory_limit_bytes,
        )
        report_md = format_directions_report(
            directions, sql=sql,
            reason="--diagnose-only requested (no baseline, no iteration)",
            model=model,
        )
        raise LongQueryAbort(
            "diagnose-only: directed report emitted (no query executed)",
            0.0, 0.0, report_markdown=report_md,
        )

    elif decision.route == PreflightRoute.NO_DATA:
        if output:
            if decision.no_data_reason == "table_not_found":
                output.progress(f"  [yellow]No-data path:[/yellow] table/schema not found — switching to static analysis report")
            else:
                output.progress(f"  [yellow]No-data path:[/yellow] baseline returned 0 rows — switching to static analysis report")
        from genie.skills.trino_query.research import _run_no_data_path
        result = _run_no_data_path(
            provider=provider,
            model=model,
            reasoning=reasoning,
            original_sql=sql,
            no_data_reason=decision.no_data_reason,
            static_report=static_report,
            baseline_exc=decision.baseline_exc,
            output=output,
            step_trace=_step_trace,  # GAP-1: thread step_trace so critical_path StepEvent is visible
        )
        raise NoDataDetected(decision.no_data_reason, result)

    elif decision.route == PreflightRoute.REAL_FAILURE:
        raise decision.baseline_exc

    elif decision.route == PreflightRoute.LONG_QUERY_ABORT:
        from genie.skills.mcp_trino.pre_execution_diagnosis import format_directions_report
        g = decision.gate_result
        if output:
            output.progress(f"  Long-query gate: {g.message}")
            output.progress("  Writing directed report and skipping further query executions")
        directions, _ = _assemble_mcp_directions(
            client, sql, static_report,
            peak_memory_bytes=getattr(_baseline_metrics, "peak_memory_bytes", 0) or None,
            peak_memory_limit_bytes=memory_limit_bytes,
        )
        report_md = format_directions_report(
            directions, sql=sql,
            reason=g.message,
            model=model,
            baseline_already_ran=True,
        )
        raise LongQueryAbort(
            g.message, g.baseline_s, g.predicted_total_s,
            report_markdown=report_md,
        )

    # Else: PLAN_COST_LOOP or STANDARD_LOOP — continue to pre-loop blocks below.

    # ── Pre-loop: seen_no_estimates progress (one-block, verbatim from current code) ──
    if output and decision.seen_no_estimates:
        output.progress(
            "  [info] Plan-cost mode unavailable: EXPLAIN returned a plan but no cost "
            "estimates (table statistics missing — run ANALYZE). Using standard iteration loop."
        )

    # ── Per-candidate wall-clock kill (best-effort) ──
    # mcp-trino may or may not persist SET SESSION across separate tool calls.
    # We emit it anyway; if the server ignores it, candidates that overshoot
    # baseline wall-time are also capped by the MCP request timeout below.
    baseline_wall_ms = float(_baseline.metrics.wall_time_ms or _baseline.metrics.query_time_ms or 0)
    candidate_timeout_ms = make_candidate_timeout_ms(baseline_wall_ms) if baseline_wall_ms > 0 else None
    if baseline_wall_ms > 0:
        timeout_sql = make_query_max_run_time_sql(baseline_wall_ms)
        try:
            _execute_via_mcp(client, timeout_sql)
            if output:
                output.progress(f"  Session property set: {timeout_sql}")
        except Exception as exc:
            if output:
                output.progress(f"  [dim]Session property emit failed (best-effort): {exc}[/dim]")
        if output and candidate_timeout_ms is not None:
            output.progress(
                f"  Candidate timeout: {candidate_timeout_ms / 1000.0:.1f}s "
                f"(baseline wall-time)"
            )

    # ── Loop dispatch ──
    if decision.route == PreflightRoute.PLAN_COST_LOOP:
        return _run_mcp_plan_cost_loop(
            client=client,
            provider=provider,
            model=model,
            reasoning=reasoning,
            original_sql=sql,
            metric_key=metric_key,
            max_iterations=max_iterations,
            verify_runs=verify_runs,
            output=output,
            build_prompt=build_prompt,
            baseline=_baseline,
            static_report=static_report,
            explain_runner=mcp_explain_runner,
            max_fallbacks=fallbacks,
            candidate_timeout_ms=candidate_timeout_ms,
            peak_memory_limit_bytes=memory_limit_bytes,
            execution_policy=policy,
        )

    # STANDARD_LOOP fall-through — EXPLAIN ANALYZE baseline follows.
    # Alias _baseline → baseline for the standard-loop body below (no rename needed).
    baseline = _baseline

    # ── EXPLAIN ANALYZE baseline ──
    original_explain: ExplainAnalyzeResult | None = None
    if output:
        output.progress("  Running EXPLAIN ANALYZE on baseline...")
    if output and hasattr(output, "status"):
        with output.status("baseline: explain analyze"):
            original_explain = _fetch_explain_analyze(client, sql)
    else:
        original_explain = _fetch_explain_analyze(client, sql)
    if output:
        if original_explain.available:
            output.progress(f"  EXPLAIN ANALYZE: {len(original_explain.stages)} stage(s), "
                          f"CPU={original_explain.total_cpu_ms:.0f}ms")
        else:
            output.progress("  EXPLAIN ANALYZE: unavailable (fallback to MCP metrics)")

    # ── Pre-execution diagnosis (v29 T2) ──
    # Combine static findings + plan-cost estimates + table metadata + the
    # baseline's actual peak memory into a ranked list of optimization
    # directions, then feed the top ones into the optimizer prompt so the LLM
    # works with a direction instead of brainstorming blind. Metadata fetched
    # here is reused by the post-loop suggestions block (single fetch).
    from genie.skills.mcp_trino.pre_execution_diagnosis import (
        format_directions_for_prompt,
    )
    from genie.skills.mcp_trino.rule_gate import (
        build_rule_gate_summary,
        format_rule_gate_for_prompt,
        render_rule_gate_summary,
    )

    # One EXPLAIN round-trip shared by the directions assembler and the plan
    # skeleton below — previously each ran its own for the same SQL.
    from .preflight import plan_cost as _plan_cost_shared
    try:
        _shared_explain_cost = _plan_cost_shared(sql, _build_mcp_explain_runner(client))
    except Exception:
        _shared_explain_cost = None
    directions, pre_table_metadata = _assemble_mcp_directions(
        client, sql, static_report,
        peak_memory_bytes=getattr(baseline.metrics, "peak_memory_bytes", 0) or None,
        peak_memory_limit_bytes=memory_limit_bytes,   # NEW
        explain_cost=_shared_explain_cost,
    )
    rule_gate = build_rule_gate_summary(static_report, directions)
    rule_gate_block = format_rule_gate_for_prompt(rule_gate)
    directions_block = format_directions_for_prompt(directions)
    if output:
        render_rule_gate_summary(output, rule_gate)
    if output and directions:
        output.progress(f"  Pre-execution diagnosis: {len(directions)} ranked direction(s) → prompt")

    # Condensed plan skeleton + EXPLAIN ANALYZE hotspots: the directions block
    # carries only rule-recognized patterns; the skeleton lets the model see
    # the full optimizer-chosen tree, and the hotspot block anchors it to
    # where the baseline actually spent CPU. Both are fail-open ("").
    baseline_skeleton = ""
    if _shared_explain_cost is not None and _shared_explain_cost[2] is not None:
        try:
            from genie.skills.trino_query.plan_render import render_plan_skeleton as _rps
            baseline_skeleton = _rps(_shared_explain_cost[2])
        except Exception:
            baseline_skeleton = ""
    skeleton_block = _format_plan_skeleton_block(
        baseline_skeleton, label="Baseline plan skeleton"
    )
    hotspot_block = _format_hotspot_stages(original_explain)
    if output and skeleton_block:
        output.progress(f"  Plan skeleton: {len(baseline_skeleton.splitlines())} line(s) → prompt")
    if output and hotspot_block:
        output.progress(f"  EXPLAIN ANALYZE hotspots: {len(hotspot_block.splitlines()) - 1} stage(s) → prompt")

    # ── Session setup ──
    skill_prompt = build_prompt(True, model) if build_prompt else ""
    from genie.core.registry import SkillRegistry
    skill_instructions = SkillRegistry.get_instructions("mcp_trino")
    sys_prompt = (
        f"You are optimizing a Trino SQL query for performance.\n"
        f"Target metric: {metric_key} (lower is better).\n\n"
        f"Rules:\n"
        f"- Return the COMPLETE optimized SQL in a ```sql code block\n"
        f"- Do NOT use file_patch or any tool calls\n"
        f"- Keep the EXACT same result set — same columns, same rows, same values\n"
        f"- Make ONE focused change per iteration\n\n"
    )
    if rule_gate_block:
        sys_prompt += f"{rule_gate_block}\n\n"
    if skill_instructions:
        sys_prompt += f"## Trino Optimization Guide\n\n{skill_instructions}\n\n"
    if directions_block:
        sys_prompt += f"{directions_block}\n\n"
    if skeleton_block:
        sys_prompt += f"{skeleton_block}\n\n"
    if hotspot_block:
        sys_prompt += f"{hotspot_block}\n\n"
    sys_prompt += skill_prompt
    session = new_session(sys_prompt)

    # ── v48 T4: Decompose-seed validation (§3.1 NORMATIVE) ──
    # Single call to _seed_decompose_and_select; winner_sql+winner_measure
    # are returned as a coupled tuple — cannot be decoupled by intermediate
    # assignments.  Guard: GENIE_V48_SEED_DECOMPOSE=0 → immediate passthrough.
    import os as _os_v48
    _v48_seed_enabled = _os_v48.environ.get("GENIE_V48_SEED_DECOMPOSE", "1") != "0"

    # v57: fragment rewrite is off by default for read paths (evidence-only decompose).
    # Opt in via GENIE_FRAGMENT_REWRITE=1.  Hard cap from GENIE_FRAGMENT_REWRITE_CAP (default 5).
    _v57_frag_rewrite = _os_v48.environ.get("GENIE_FRAGMENT_REWRITE", "0") == "1"
    _v57_frag_cap = max(1, int(_os_v48.environ.get("GENIE_FRAGMENT_REWRITE_CAP", "5")))

    _read_llm_fn = None
    _mcp_cost_reader_fn = None
    if _v48_seed_enabled:
        try:
            from genie.skills.mcp_trino.write_analysis import _make_advisory_llm_fn as _wa_llm_fn
            from genie.skills.mcp_trino.write_analysis import _advisory_cost_reader as _mcp_cost_reader_fn
            if provider is not None:
                _read_llm_fn = _wa_llm_fn(provider, model, reasoning)
        except Exception:
            pass

    def _mcp_produce_fn(_sql: str):
        return _produce_decompose_candidate(
            _sql, _read_llm_fn, _mcp_cost_reader_fn,
            run_static_gates=False, step_trace=_step_trace,
            enable_fragment_rewrite=_v57_frag_rewrite,
            max_fragment_model_calls=_v57_frag_cap,
        )

    def _mcp_measure_fn(_sql: str) -> "MeasureResult":
        return _measure_mcp_logical_sql(
            client, _sql, metric_key, verify_runs, policy=policy,
            capture_rows=True, output=output, label="seed",
        )

    iterations: list[IterationRecord] = []

    def _record_seed_rejection(record: dict) -> None:
        """Persist the shared seed gate's canonical authorization failure."""
        iterations.append(IterationRecord(
            iteration=record["iteration"], status=record["status"],
            metric_value=record["metric"], delta=record["delta"],
            hypothesis=record["rejection_reason"], sql=record["candidate_sql"],
            rejection_reason=record["rejection_reason"], base_sql=record["base_sql"],
            candidate_sql=record["candidate_sql"],
            baseline_capture_status=record["baseline_capture_status"],
            candidate_capture_status=record["candidate_capture_status"],
            baseline_completeness=record["baseline_completeness"],
            candidate_completeness=record["candidate_completeness"],
        ))

    # §3.1 NORMATIVE: best_sql and best_measure come from the SAME tuple arm.
    # _seed_decompose_and_select is the single call-site locus — it cannot drift.
    # Flag-off path: produce_fn/measure_fn are never called (FLAG-OFF spy test verifies this).
    best_sql, best_measure, _ = _seed_decompose_and_select(
        sql, baseline,
        produce_fn=_mcp_produce_fn if (_read_llm_fn and _mcp_cost_reader_fn) else (lambda s: (s, [], [], None)),
        measure_fn=_mcp_measure_fn,
        flag_enabled=_v48_seed_enabled,
        output=output,
        trace=_step_trace,
        rejection_history=_record_seed_rejection,
    )
    best_metric = best_measure.median_metric
    # v32 T1: cache of rendered direction blocks keyed by SQL. Seeded with the
    # original (already in the system prompt) so a stable best_sql is never
    # re-diagnosed; refreshed only when an improvement changes best_sql.
    rediag_cache: dict[str, str] = {sql: directions_block}
    attempted_candidates: dict[str, tuple[int, str, str, str]] = {}

    def _candidate_key(candidate_sql: str) -> str:
        return " ".join(candidate_sql.split())

    def _remember_candidate(iteration: int, candidate_sql: str,
                            status: str, reason: str) -> None:
        attempted_candidates[_candidate_key(candidate_sql)] = (
            iteration, status, reason, candidate_sql,
        )

    def _attempted_changes_block() -> str:
        if not attempted_candidates:
            return ""
        lines = ["Previously attempted changes (do NOT repeat these SQL candidates):"]
        for attempted_iteration, status, reason, attempted_sql in list(
            attempted_candidates.values()
        )[-5:]:
            compact_sql = " ".join(attempted_sql.split())
            if len(compact_sql) > 500:
                compact_sql = compact_sql[:497] + "..."
            lines.append(
                f"- iteration {attempted_iteration}: {status}; reason={reason}; "
                f"candidate={compact_sql}"
            )
        return "\n".join(lines) + "\n\n"

    # ── Iteration loop ──
    for iteration in range(1, max_iterations + 1):
        iter_start = time.monotonic()
        if output:
            output.print("")
            output.progress(f"── iteration {iteration}/{max_iterations}")

        last_str = "N/A (first iteration)"
        if iterations:
            last = iterations[-1]
            last_str = f"{last.status} (metric={last.metric_value:.1f}, delta={last.delta:+.1f})"

        # v32 T1: re-diagnose the CURRENT best_sql. The system-prompt directions
        # describe the original query; once an improvement changes best_sql they
        # go stale. Recompute at zero query cost (static + EXPLAIN FORMAT JSON;
        # table metadata reused) and feed fresh directions into this turn. Cached
        # by SQL, so an unchanged best_sql across iterations is not re-diagnosed.
        fresh_block = rediag_cache.get(best_sql)
        if fresh_block is None:
            try:
                _rd, _ = _assemble_mcp_directions(
                    client, best_sql, static_analyze(best_sql),
                    peak_memory_bytes=getattr(best_measure.metrics, "peak_memory_bytes", 0) or None,
                    table_metadata=pre_table_metadata or None,
                    peak_memory_limit_bytes=memory_limit_bytes,   # NEW
                )
                fresh_block = format_directions_for_prompt(_rd)
            except Exception:
                fresh_block = ""
            # Refresh the plan skeleton alongside directions — the tree in the
            # system prompt describes the original SQL and goes stale the same
            # way. Cached per SQL; each iteration message carries only this
            # block, and the lean-history trim bounds accumulation.
            _fresh_skeleton = _format_plan_skeleton_block(
                _render_mcp_plan_skeleton(client, best_sql),
                label="Current plan skeleton",
            )
            if _fresh_skeleton:
                fresh_block = (
                    f"{fresh_block}\n\n{_fresh_skeleton}" if fresh_block
                    else _fresh_skeleton
                )
            rediag_cache[best_sql] = fresh_block
        # Only inject when the query has actually changed (iter 1 is already
        # covered by the system prompt) and the diagnosis produced directions.
        diag_line = f"{fresh_block}\n\n" if (fresh_block and best_sql != sql) else ""

        attempted_changes = _attempted_changes_block()
        context = (
            f"[Trino Query Enhancement — Iteration {iteration}]\n"
            f"Target metric: {metric_key} (lower is better)\n"
            f"Baseline: {baseline.median_metric:.1f}\n"
            f"Current best: {best_metric:.1f}\n"
            f"Last iteration: {last_str}\n\n"
            f"Current SQL:\n```sql\n{best_sql}\n```\n\n"
            f"{diag_line}"
            f"{attempted_changes}"
            f"Return the COMPLETE optimized SQL in a ```sql block. ONE change only. "
            f"Do NOT include a trailing semicolon."
        )

        # Keep history lean
        sys_msgs = [m for m in session["history"] if m["role"] == "system"]
        non_sys = [m for m in session["history"] if m["role"] != "system"]
        session["history"] = sys_msgs + non_sys[-4:]
        session["history"].append(new_msg("user", context))

        # Get AI response — provider/model failures become a reportable iteration.
        req = CompletionRequest(messages=session["history"], model=model, reasoning=reasoning)
        try:
            if output and hasattr(output, "status"):
                with output.status("AI thinking..."):
                    reply = provider.complete_text(req)
            else:
                reply = provider.complete_text(req)
        except Exception as _model_exc:
            elapsed = time.monotonic() - iter_start
            _fail_msg = f"model/provider failed: {_model_exc}"
            _render_iteration_result(
                output, iteration=iteration, total=max_iterations,
                status="model_failed", hypothesis=_fail_msg,
                metric_key=metric_key, metric_value=best_metric, delta=0.0,
                elapsed_s=elapsed,
                reason=_fail_msg,
            )
            iterations.append(IterationRecord(
                iteration=iteration, status="model_failed",
                metric_value=best_metric, delta=0.0,
                hypothesis=_fail_msg,
            ))
            break  # stop iterating — build report with what we have

        if not reply:
            if output:
                output.error("  Empty AI response — stopping.")
            break

        session["history"].append(new_msg("assistant", reply))

        # Extract SQL
        candidate_sql = extract_sql_from_reply(reply)
        if not candidate_sql:
            _render_iteration_result(
                output, iteration=iteration, total=max_iterations,
                status="no_sql", hypothesis="no SQL extracted",
                metric_key=metric_key, metric_value=best_metric, delta=0.0,
                elapsed_s=time.monotonic() - iter_start,
                reason="no SQL extracted from model response",
            )
            iterations.append(IterationRecord(
                iteration=iteration, status="no_sql",
                metric_value=best_metric, delta=0.0,
                hypothesis="no SQL extracted",
            ))
            continue

        candidate_key = _candidate_key(candidate_sql)
        if candidate_key in attempted_candidates:
            attempted_iteration, attempted_status, attempted_reason, _ = (
                attempted_candidates[candidate_key]
            )
            duplicate_reason = (
                f"same SQL as iteration {attempted_iteration} "
                f"({attempted_status}: {attempted_reason})"
            )
            if output:
                output.progress(f"  [SKIP] Duplicate candidate — {duplicate_reason}")
            session["history"].append(new_msg(
                "user",
                f"Candidate rejected without execution: {duplicate_reason}. "
                "Do not repeat an earlier SQL candidate; try a materially different change."
            ))
            iterations.append(IterationRecord(
                iteration=iteration, status="duplicate_candidate",
                metric_value=best_metric, delta=0.0,
                hypothesis=duplicate_reason, sql=candidate_sql,
                rejection_reason=duplicate_reason,
            ))
            continue

        # Extract hypothesis
        hypothesis = "?"
        for line in reply.split("\n"):
            line = line.strip()
            if line and not line.startswith("```") and not line.startswith("|"):
                hypothesis = line[:80]
                break

        # Show what the AI proposed (visible diff, not just "hypothesis")
        _render_sql_diff(output, best_sql, candidate_sql)

        # Execute and measure candidate
        try:
            candidate = _measure_mcp_logical_sql(client, candidate_sql, metric_key, verify_runs, policy=policy, capture_rows=True,
                                     output=output, label=f"iter {iteration} candidate",
                                     timeout_ms=candidate_timeout_ms)
        except CandidateTimeoutError as exc:
            elapsed = time.monotonic() - iter_start
            _render_iteration_result(
                output, iteration=iteration, total=max_iterations,
                status="timeout_worse", hypothesis=f"timeout: {exc}",
                metric_key=metric_key, metric_value=best_metric, delta=0.0,
                elapsed_s=elapsed,
                reason=str(exc),
            )
            session["history"].append(new_msg(
                "user",
                f"Query exceeded the baseline wall-time limit: {exc}. "
                f"Change REVERTED. Try a faster approach."
            ))
            iterations.append(IterationRecord(
                iteration=iteration, status="timeout_worse",
                metric_value=best_metric, delta=0.0,
                hypothesis=hypothesis, sql=candidate_sql,
            ))
            _remember_candidate(iteration, candidate_sql, "timeout_worse", str(exc))
            continue
        except Exception as exc:
            elapsed = time.monotonic() - iter_start
            _render_iteration_result(
                output, iteration=iteration, total=max_iterations,
                status="exec_failed", hypothesis=f"execution failed: {exc}",
                metric_key=metric_key, metric_value=best_metric, delta=0.0,
                elapsed_s=elapsed,
                reason=str(exc),
            )
            iterations.append(IterationRecord(
                iteration=iteration, status="exec_failed",
                metric_value=best_metric, delta=0.0,
                hypothesis=hypothesis, sql=candidate_sql,
            ))
            _remember_candidate(iteration, candidate_sql, "exec_failed", str(exc))
            session["history"].append(new_msg(
                "user",
                f"Candidate REVERTED: execution failed: {exc}. "
                "Do not repeat this SQL candidate; try a materially different change."
            ))
            continue

        candidate_metric = candidate.median_metric
        delta = candidate_metric - best_metric

        # Received MCP envelopes never authorize full-result correctness.
        if not _correctness_authorized(baseline, candidate):
            incomplete = _incomplete_history(
                iteration=iteration, baseline=baseline, candidate=candidate,
                base_sql=best_sql, candidate_sql=candidate_sql,
                metric=candidate_metric, delta=delta,
            )
            iterations.append(IterationRecord.from_history(
                incomplete,
                hypothesis=incomplete["rejection_reason"],
                sql=candidate_sql,
            ))
            _remember_candidate(
                iteration, candidate_sql, incomplete["status"],
                incomplete["rejection_reason"],
            )
            session["history"].append(new_msg(
                "user",
                f"Candidate REVERTED: {incomplete['rejection_reason']}. "
                "Do not repeat this SQL candidate; try a materially different change."
            ))
            continue
        if baseline.row_count != candidate.row_count:
            equiv = False
            equiv_reason = f"row count differs: {baseline.row_count} vs {candidate.row_count}"
        else:
            equiv, equiv_reason = _results_equivalent(baseline.rows, candidate.rows)
        if not equiv:
            elapsed = time.monotonic() - iter_start
            _render_iteration_result(
                output, iteration=iteration, total=max_iterations,
                status="semantic_drift", hypothesis=f"drift: {equiv_reason}",
                metric_key=metric_key, metric_value=candidate_metric, delta=delta,
                elapsed_s=elapsed,
                reason=f"semantic_drift: {equiv_reason}",
            )
            session["history"].append(new_msg(
                "user",
                f"Query results differ from baseline: {equiv_reason}. "
                f"Change REVERTED. Try a different approach that preserves the exact same result set."
            ))
            iterations.append(IterationRecord(
                iteration=iteration, status="semantic_drift",
                metric_value=candidate_metric, delta=delta,
                hypothesis=hypothesis, sql=candidate_sql,
            ))
            _remember_candidate(
                iteration, candidate_sql, "semantic_drift", equiv_reason,
            )
            continue

        # Decision: keep or revert
        improved = candidate_metric < best_metric
        if improved:
            best_sql = candidate_sql
            best_metric = candidate_metric
            best_measure = candidate
            status = "improved"
        else:
            status = "worse"

        elapsed = time.monotonic() - iter_start
        _render_iteration_result(
            output, iteration=iteration, total=max_iterations,
            status=status, hypothesis=hypothesis,
            metric_key=metric_key, metric_value=candidate_metric, delta=delta,
            elapsed_s=elapsed,
            reason=None if improved else "not faster than current best",
        )

        session["history"].append(new_msg(
            "user",
            f"[Iteration {iteration} result]\n"
            f"Status: {'KEPT' if improved else 'REVERTED'}\n"
            f"Metric ({metric_key}, median of {verify_runs} runs): {candidate_metric:.1f}\n"
            f"Delta vs current best: {delta:+.1f}\n"
            f"Row count: {candidate.row_count} (baseline: {baseline.row_count})\n"
            f"{'Change KEPT — this is now the current best.' if improved else 'Change REVERTED — current best unchanged.'}"
        ))

        iterations.append(IterationRecord(
            iteration=iteration, status=status,
            metric_value=candidate_metric, delta=delta,
            hypothesis=hypothesis, sql=candidate_sql,
        ))
        _remember_candidate(
            iteration, candidate_sql, status,
            "accepted as current best" if improved else "not faster than current best",
        )

    # ── Direction efficacy (v32 T2) ──
    # Observational attribution: did each diagnosed direction's target metric
    # actually improve from baseline to the final best? Rendered so "directed
    # optimization" is measurable, not asserted. No causal claim — directions
    # sharing a metric are flagged co-attributed.
    from genie.skills.mcp_trino.pre_execution_diagnosis import (
        attribute_directions,
        format_attribution_report,
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

    direction_outcomes = attribute_directions(
        directions, _metrics_attr_map(baseline.metrics), _metrics_attr_map(best_measure.metrics)
    )
    attribution_block = format_attribution_report(direction_outcomes)
    if output and attribution_block:
        output.print("")
        for _line in attribution_block.splitlines():
            output.print(f"  {_line}")

    # ── EXPLAIN ANALYZE enhanced ──
    enhanced_explain: ExplainAnalyzeResult | None = None
    if best_sql != sql:
        if output:
            output.progress("  Running EXPLAIN ANALYZE on enhanced SQL...")
        if output and hasattr(output, "status"):
            with output.status("enhanced: explain analyze"):
                enhanced_explain = _fetch_explain_analyze(client, best_sql)
        else:
            enhanced_explain = _fetch_explain_analyze(client, best_sql)
        if output and enhanced_explain.available:
            output.progress(f"  Enhanced EXPLAIN: {len(enhanced_explain.stages)} stage(s), "
                          f"CPU={enhanced_explain.total_cpu_ms:.0f}ms")

    # ── Table metadata + suggestions ──
    table_suggestions: list[TableSuggestion] = []
    table_refs = _extract_table_names(sql)
    qualified_refs = [(c, s, t) for (c, s, t) in table_refs if c and s]
    had_qualified_tables = bool(qualified_refs)
    if qualified_refs and output:
        output.progress(f"  Fetching table metadata for {len(qualified_refs)} qualified table(s)...")
    elif table_refs and output:
        output.progress(f"  Skipping table metadata — no fully-qualified tables (use catalog.schema.table).")
    if qualified_refs:
        try:
            # Reuse metadata fetched for the pre-execution diagnosis when present
            # (same refs, same query) to avoid a second round-trip.
            metadata = pre_table_metadata if pre_table_metadata else _fetch_table_metadata(client, qualified_refs)
            table_suggestions = _generate_table_suggestions(metadata)
            if output and table_suggestions:
                output.progress(f"  Found {len(table_suggestions)} table suggestion(s).")
        except Exception:
            pass  # graceful skip

    # ── Build report ──
    improvement_abs = best_metric - baseline.median_metric
    improvement_pct = (improvement_abs / baseline.median_metric * 100) if baseline.median_metric else 0

    # An MCP response is only a received envelope, never verification that the
    # server emitted the complete Trino result. Equality remains diagnostic only.
    if not _correctness_authorized(baseline, best_measure):
        final_equiv = False
        final_reason = _incomplete_rejection_reason(baseline, best_measure)
    else:
        final_equiv, final_reason = _results_equivalent(baseline.rows, best_measure.rows)

    report = EnhancementReport(
        timestamp=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        original_sql=sql,
        original_result_sample=baseline.rows[:10],
        original_columns=baseline.columns,
        original_row_count=baseline.row_count,
        original_metrics=baseline.metrics,
        enhanced_sql=best_sql,
        enhanced_result_sample=best_measure.rows[:10],
        enhanced_columns=best_measure.columns,
        enhanced_row_count=best_measure.row_count,
        enhanced_metrics=best_measure.metrics,
        metric_key=metric_key,
        baseline_value=baseline.median_metric,
        best_value=best_metric,
        improvement_abs=improvement_abs,
        improvement_pct=improvement_pct,
        iterations=iterations,
        data_consistent=final_equiv,
        data_consistency_reason=final_reason,
        mcp_server_url=client.config.url,
        verify_runs=verify_runs,
        table_suggestions=table_suggestions,
        had_qualified_tables=had_qualified_tables,
        original_explain=original_explain,
        enhanced_explain=enhanced_explain,
        step_trace=_step_trace,
    )

    # Final visual summary
    _render_summary_card(
        output,
        baseline_value=baseline.median_metric,
        best_value=best_metric,
        metric_key=metric_key,
        improvement_abs=improvement_abs,
        improvement_pct=improvement_pct,
        data_consistent=final_equiv,
        data_consistency_reason=final_reason,
        iterations_ran=len(iterations),
    )

    return report


# ---------------------------------------------------------------------------
# Adapter for /trino-research auto-routing
# ---------------------------------------------------------------------------

# Metrics supported on the MCP path. `query_time_ms` is MCP-native; the rest
# map onto what the MCP server returns via cursor/REST stats.
MCP_METRICS = [
    "query_time_ms", "cpu_time_ms", "wall_time_ms",
    "physical_input_bytes", "processed_rows", "total_splits",
    "peak_memory_bytes",
]


RESEARCH_QUERY_TIMEOUT = 300  # seconds — bumped from default 30s for long-running queries


# ---------------------------------------------------------------------------
# UX helpers (v22 sprint)
# ---------------------------------------------------------------------------

def _fmt_metric_value(val: float) -> str:
    """Adaptive formatter for metric values (used in live output)."""
    if val is None:
        return "0"
    if val == 0:
        return "0"
    absv = abs(val)
    if absv < 0.001:
        return f"{val:.4f}"
    if absv < 1:
        return f"{val:.3f}"
    if absv < 100:
        return f"{val:.2f}"
    return f"{val:.0f}"


def _render_plan_card(
    output, *, sql: str, sql_source: str, metric: str, iterations: int,
    runs: int, server: str, safe_limit: Optional[int], query_timeout: int,
) -> None:
    """Pre-launch summary — tells the user exactly what is about to happen."""
    if output is None:
        return
    sql_lines = sql.count("\n") + 1
    sql_bytes = len(sql.encode("utf-8"))
    output.print("")
    output.print("  [bold cyan]── Research Plan ──[/bold cyan]")
    output.print(f"  [dim]sql         [/dim] {sql_source} ({sql_lines} lines, {sql_bytes:,}B)")
    output.print(f"  [dim]metric      [/dim] {metric} (lower is better)")
    output.print(f"  [dim]iterations  [/dim] {iterations}")
    output.print(f"  [dim]verify      [/dim] {runs} runs per candidate (median)")
    output.print(f"  [dim]server      [/dim] {server}")
    if safe_limit and safe_limit > 0:
        output.print(f"  [dim]safe-limit  [/dim] LIMIT {safe_limit} wrapper active")
    output.print(f"  [dim]timeout     [/dim] {query_timeout}s per query")
    # SQL preview (first 5 lines with syntax highlighting)
    preview_lines = sql.strip().splitlines()[:5]
    preview_text = "\n".join(preview_lines)
    if len(sql.strip().splitlines()) > 5:
        preview_text += "\n..."
    try:
        from rich.syntax import Syntax
        from rich.console import Console as _C
        _c = _C(force_terminal=True, highlight=False)
        syn = Syntax(preview_text, "sql", theme="monokai", line_numbers=False, padding=(0, 2))
        _c.print(syn)
    except Exception:
        output.print(f"  [dim]{preview_text}[/dim]")
    output.print("")


def _render_sql_diff(output, old_sql: str, new_sql: str, max_lines: int = 20) -> None:
    """Render a colored unified diff of the AI's proposed SQL vs current best."""
    if output is None:
        return
    import difflib
    if old_sql.strip() == new_sql.strip():
        output.print(f"  [dim](no SQL change)[/dim]")
        return
    diff = list(difflib.unified_diff(
        old_sql.splitlines(),
        new_sql.splitlines(),
        lineterm="",
        n=1,  # small context
    ))
    # Skip the file header lines ("---" / "+++")
    body = [ln for ln in diff if not ln.startswith("---") and not ln.startswith("+++")]
    if not body:
        return
    shown = body[:max_lines]
    output.print("  [dim]sql diff:[/dim]")
    for ln in shown:
        if ln.startswith("+"):
            output.print(f"    [green]{ln}[/green]")
        elif ln.startswith("-"):
            output.print(f"    [red]{ln}[/red]")
        elif ln.startswith("@@"):
            output.print(f"    [dim]{ln}[/dim]")
        else:
            output.print(f"    {ln}")
    if len(body) > max_lines:
        output.print(f"    [dim]... +{len(body) - max_lines} more lines[/dim]")


def _render_iteration_result(
    output, *, iteration: int, total: int, status: str, hypothesis: str,
    metric_key: str, metric_value: float, delta: float, elapsed_s: float,
    reason: str | None = None,
) -> None:
    """One structured line per iteration outcome. Uses the HumanSink palette."""
    if output is None:
        return
    color_by_status = {
        "improved": "green",
        "worse": "yellow",
        "semantic_drift": "red",
        "exec_failed": "red",
        "timeout_worse": "red",
        "no_sql": "dim",
    }
    label_by_status = {
        "improved": "KEPT",
        "worse": "WORSE",
        "semantic_drift": "REVERT",
        "exec_failed": "FAIL",
        "timeout_worse": "TIMEOUT",
        "no_sql": "SKIP",
    }
    color = color_by_status.get(status, "white")
    label = label_by_status.get(status, status.upper())

    def _clean(value: str, limit: int = 110) -> str:
        text = " ".join(str(value).split())
        if len(text) > limit:
            text = text[: limit - 3] + "..."
        return escape(text)

    metric = _fmt_metric_value(metric_value)
    delta_text = _fmt_metric_value(delta)
    elapsed_text = f"{elapsed_s:.1f}s"

    output.print(
        f"  [{color}]{label:<7}[/{color}] "
        f"[dim]iteration[/dim] {iteration}/{total}"
    )
    output.print(
        f"    [dim]metric [/dim] {metric_key:<18} {metric:>10}   "
        f"[dim]delta[/dim] {delta_text:>10}   "
        f"[dim]elapsed[/dim] {elapsed_text:>8}"
    )
    if reason:
        output.print(f"    [dim]reason [/dim] {_clean(reason)}")
    if hypothesis and hypothesis != "?":
        output.print(f"    [dim]note   [/dim] {_clean(hypothesis)}")


def _render_summary_card(
    output, *, baseline_value: float, best_value: float, metric_key: str,
    improvement_abs: float, improvement_pct: float, data_consistent: bool,
    data_consistency_reason: str, iterations_ran: int,
) -> None:
    """Final visual summary — bars scale to the larger of baseline/best."""
    if output is None:
        return
    bar_width = 30
    peak = max(abs(baseline_value), abs(best_value), 1e-9)
    def _bar(v: float) -> str:
        n = int(round(abs(v) / peak * bar_width))
        return "█" * max(n, 0)
    output.print("")
    output.print("  [bold cyan]── Final Result ──[/bold cyan]")
    output.print(f"  [dim]baseline   [/dim] {_fmt_metric_value(baseline_value):>10}  [cyan]{_bar(baseline_value)}[/cyan]")
    output.print(f"  [dim]best       [/dim] {_fmt_metric_value(best_value):>10}  [green]{_bar(best_value)}[/green]")
    arrow = "↓" if improvement_abs < 0 else ("↑" if improvement_abs > 0 else "·")
    improved = improvement_abs < 0
    change_color = "green" if improved else ("yellow" if improvement_abs == 0 else "red")
    output.print(
        f"  [dim]change     [/dim] "
        f"[{change_color}]{_fmt_metric_value(improvement_abs)} ({improvement_pct:+.1f}%) {arrow}[/{change_color}]"
    )
    dc_label = "[green]PASS[/green]" if data_consistent else f"[red]FAIL[/red] ({data_consistency_reason})"
    output.print(f"  [dim]data check [/dim] {dc_label}")
    output.print(f"  [dim]iterations [/dim] {iterations_ran} rounds")
    output.print("")


def run_trino_research_via_mcp(
    provider,
    cfg: dict,
    model: str,
    reasoning: str,
    output,
    build_prompt: Callable[..., str],
    *,
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
    """MCP-routed entry point for /trino-research.

    Mirrors `trino_query.research.run_trino_research` so chat.py can dispatch
    to either path via a single call signature.
    """
    sql: str | None = None
    validated_safe_limit: int | None = None
    sql_source = "stdin"

    # File/text callers already provide SQL without live MCP dependencies. Classify
    # those first so write SQL can stay fully offline even when MCP is down.
    if sql_file:
        sql = Path(sql_file).read_text().strip()
        sql_source = sql_file
    elif sql_text:
        sql = sql_text.strip()
        sql_source = "sql_text"

    if sql is not None:
        if not sql:
            output.error("Empty SQL.")
            return
        validated_safe_limit = validate_safe_limit(safe_limit)
        if classify_write_operation(sql) is not None:
            run_write_analysis_only(
                provider, cfg, model, reasoning, sql, output, build_prompt,
                sql_source=sql_source,
                route="mcp",
                safe_limit=validated_safe_limit,
            )
            return

    # For supplied read SQL, reject invalid policy before MCP configuration,
    # reachability, provider work, or entry EXPLAIN.
    if sql is not None:
        validated_safe_limit = validate_safe_limit(safe_limit)

    mcp_cfg = load_mcp_config()
    if not mcp_cfg.enabled:
        output.error("  MCP Trino not enabled. Configure [mcp.trino] in ~/.genie/config.toml.")
        return

    # Bump timeout for research workloads; individual queries can be long.
    mcp_cfg.timeout = max(mcp_cfg.timeout, query_timeout or RESEARCH_QUERY_TIMEOUT)
    client = McpClient(mcp_cfg)

    # Reachability preflight — fall through to caller's direct fallback is
    # handled at chat.py; if we got here, caller already decided on MCP.
    try:
        client.list_tools()
    except Exception as exc:
        output.error(f"  MCP server unreachable at {mcp_cfg.url}: {exc}")
        return

    output.print("\n  [yellow]== Trino Query Optimization (MCP) ==[/yellow]")
    output.progress(f"  Server: {mcp_cfg.url}")

    # ── Get SQL ──
    if sql_file and sql is not None:
        output.progress(f"  SQL from file: {sql_file}")
    elif sql is None:
        from genie.input import _read_paste_mode
        output.print("  [cyan]Paste SQL (Ctrl-D to finish):[/cyan]")
        sql = _read_paste_mode()

    if not sql:
        output.error("Empty SQL.")
        return

    # Interactive paste reaches here after existing MCP reachability.
    validated_safe_limit = validate_safe_limit(safe_limit)
    if classify_write_operation(sql) is not None:
        run_write_analysis_only(
            provider, cfg, model, reasoning, sql, output, build_prompt,
            sql_source=sql_source,
            route="mcp",
            safe_limit=validated_safe_limit,
        )
        return

    output.print(f"  [dim]SQL: {sql[:80]}...[/dim]\n")

    # ── Get metric ──
    if not metric:
        from genie.input import _read_input
        output.print("  [yellow]Metric to minimize:[/yellow]")
        for i, m in enumerate(MCP_METRICS, 1):
            output.print(f"    [cyan]{i}[/cyan]. {m}")
        try:
            choice = _read_input("  Choose [1]: ").strip() or "1"
            idx = int(choice) - 1
            metric = MCP_METRICS[idx] if 0 <= idx < len(MCP_METRICS) else "query_time_ms"
        except (ValueError, EOFError, KeyboardInterrupt):
            metric = "query_time_ms"

    if metric not in MCP_METRICS:
        output.error(f"Unknown metric: {metric}. Use one of: {MCP_METRICS}")
        return

    # ── Get iterations ──
    if iterations is None:
        from genie.input import _read_input
        try:
            iter_str = _read_input("  Max iterations [1]: ").strip() or "1"
            iterations = max(1, int(iter_str))
        except (ValueError, EOFError, KeyboardInterrupt):
            iterations = 1

    # ── Get verify runs ──
    if runs is None:
        from genie.input import _read_input
        try:
            runs_str = _read_input("  Verify runs per candidate [3]: ").strip() or "3"
            runs = max(1, int(runs_str))
        except (ValueError, EOFError, KeyboardInterrupt):
            runs = 3

    # Public safe-limit validation is before every advisory/provider/EXPLAIN path.
    validated_safe_limit = validate_safe_limit(safe_limit)

    # ── Pre-flight: read-only + size estimation ──
    from .preflight import run_preflight, PreflightBudget

    def _explain_runner(s: str) -> Optional[str]:
        tool_name, _ = _resolve_query_tool(client)
        # Only run EXPLAIN if the server exposes an explain tool; otherwise skip
        tools = {t["name"] for t in client.list_tools()}
        explain_tool = next((n for n in ("explain", "explain_query", "trino_explain") if n in tools), None)
        if not explain_tool:
            return None
        try:
            return client.call_tool(explain_tool, {"sql": f"EXPLAIN (FORMAT JSON) {s}"})
        except Exception:
            return None

    output.progress("  Pre-flight: checking read-only SQL and size estimate...")
    report = run_preflight(sql, _explain_runner, PreflightBudget())
    if not report.ok:
        output.error(f"  Pre-flight rejected: {report.reason}")
        if report.estimated_rows or report.estimated_bytes:
            est = []
            if report.estimated_rows:
                est.append(f"rows~{report.estimated_rows:,}")
            if report.estimated_bytes:
                est.append(f"bytes~{report.estimated_bytes:,}")
            output.print(f"  [dim]Estimate: {', '.join(est)}[/dim]")
        return
    if report.estimated_rows or report.estimated_bytes:
        est = []
        if report.estimated_rows is not None:
            est.append(f"~{report.estimated_rows:,} rows")
        if report.estimated_bytes is not None:
            est.append(f"~{report.estimated_bytes:,} bytes")
        output.progress(f"  Pre-flight OK: {', '.join(est)}")
    else:
        output.progress(f"  Pre-flight OK: read-only verified (size estimate unavailable)")

    # ── Pre-launch plan card ──
    _render_plan_card(
        output,
        sql=sql,
        sql_source=sql_file or "stdin",
        metric=metric,
        iterations=iterations,
        runs=runs,
        server=mcp_cfg.url,
        safe_limit=safe_limit,
        query_timeout=mcp_cfg.timeout,
    )

    # ── Run MCP enhancement loop ──
    from .preflight import LongQueryAbort, NoDataDetected
    try:
        report = run_mcp_enhancement(
            client=client,
            sql=sql,
            metric_key=metric,
            max_iterations=iterations,
            verify_runs=runs,
            provider=provider,
            model=model,
            reasoning=reasoning,
            output=output,
            build_prompt=build_prompt,
            long_query_opt_in=long_query_opt_in,
            long_query_threshold_s=long_query_threshold_s,
            max_fallbacks=max_fallbacks,
            diagnose_only=diagnose_only,
            execution_policy=ExecutionPolicy(validated_safe_limit),
        )
    except LongQueryAbort as lqa:
        # Message already printed by run_mcp_enhancement.
        # If a directed report rode along, write it to disk.
        if getattr(lqa, "report_markdown", None):
            try:
                report_dir = Path.cwd() / "report"
                report_dir.mkdir(parents=True, exist_ok=True)
                report_name = f"trino-research-diagnose-{datetime.now().strftime('%Y%m%d-%H%M%S')}.md"
                report_path = report_dir / report_name
                report_path.write_text(lqa.report_markdown)
                output.progress(f"\n  Directed report saved: {report_path}")
            except Exception as exc:
                output.error(f"  Failed to save directed report: {exc}")
        return
    except NoDataDetected as nd:
        # No-data dispatch fired: write static-analysis report instead of EnhancementReport.
        try:
            report_dir = Path.cwd() / "report"
            report_dir.mkdir(parents=True, exist_ok=True)
            report_name = f"trino-research-nodata-{datetime.now().strftime('%Y%m%d-%H%M%S')}.md"
            report_path = report_dir / report_name
            report_path.write_text(nd.result.get("report_markdown", ""))
            output.progress(f"\n  Report saved: {report_path}")
        except Exception as exc:
            output.error(f"  Failed to save no-data report: {exc}")
        return

    # Save report markdown (same pattern as direct path)
    try:
        report_md = generate_report(report, step_trace=getattr(report, "step_trace", None) or None)
        report_name = f"trino-research-mcp-{datetime.now().strftime('%Y%m%d-%H%M%S')}.md"
        report_path = Path.cwd() / report_name
        report_path.write_text(report_md)
        output.progress(f"\n  Report saved: {report_path}")
    except Exception as exc:
        output.error(f"  Failed to save report: {exc}")
