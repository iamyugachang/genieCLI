"""trino_query — genieCLI skill: execute & validate SQL on local Trino.

Connection is profile-based (~/.config/genie/trino.json).
Switch profiles via /trino CLI command.
"""
from __future__ import annotations

import json
import time
import urllib.request
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from decimal import Decimal

from genie.core.arg import Arg
from genie.core.registry import BaseSkill

from .connection import get_active_profile


# ── Data classes ──────────────────────────────────────────────────────────────

@dataclass
class QueryMetrics:
    """Performance metrics from Trino cursor.stats + REST API."""
    cpu_time_ms: int = 0
    wall_time_ms: int = 0
    queued_time_ms: int = 0
    planning_time_ms: int = 0
    analysis_time_ms: int = 0
    elapsed_time_ms: int = 0
    peak_memory_bytes: int = 0
    spilled_bytes: int = 0
    physical_input_bytes: int = 0
    physical_written_bytes: int = 0
    internal_network_bytes: int = 0
    processed_rows: int = 0
    processed_bytes: int = 0
    total_splits: int = 0
    completed_splits: int = 0
    query_id: str = ""  # for post-run QueryInfo fetch; not part of metric comparison

    def to_json(self) -> dict:
        return {
            "cpu_time_ms": self.cpu_time_ms,
            "wall_time_ms": self.wall_time_ms,
            "queued_time_ms": self.queued_time_ms,
            "planning_time_ms": self.planning_time_ms,
            "analysis_time_ms": self.analysis_time_ms,
            "elapsed_time_ms": self.elapsed_time_ms,
            "peak_memory_bytes": self.peak_memory_bytes,
            "peak_memory_human": _human_bytes(self.peak_memory_bytes),
            "spilled_bytes": self.spilled_bytes,
            "physical_input_bytes": self.physical_input_bytes,
            "physical_input_human": _human_bytes(self.physical_input_bytes),
            "processed_rows": self.processed_rows,
            "total_splits": self.total_splits,
        }

    def summary_line(self) -> str:
        parts = [
            f"cpu={self.cpu_time_ms}ms",
            f"wall={self.wall_time_ms}ms",
            f"mem={_human_bytes(self.peak_memory_bytes)}",
            f"input={_human_bytes(self.physical_input_bytes)}",
            f"rows={self.processed_rows}",
            f"splits={self.total_splits}",
        ]
        if self.spilled_bytes > 0:
            parts.append(f"spill={_human_bytes(self.spilled_bytes)}")
        return " · ".join(parts)


@dataclass
class QueryResult:
    rows: list[dict]
    columns: list[str]
    duration_ms: int
    truncated: bool
    error: str | None = None
    query_id: str = ""
    metrics: QueryMetrics | None = None

    def to_json(self) -> dict:
        d = {
            "rows": self.rows,
            "columns": self.columns,
            "duration_ms": self.duration_ms,
            "truncated": self.truncated,
            "error": self.error,
            "row_count": len(self.rows),
            "query_id": self.query_id,
        }
        if self.metrics:
            d["metrics"] = self.metrics.to_json()
        return d


# ── Helpers ───────────────────────────────────────────────────────────────────

def _human_bytes(b: int) -> str:
    if b < 1024:
        return f"{b}B"
    if b < 1024 * 1024:
        return f"{b / 1024:.1f}KB"
    if b < 1024 * 1024 * 1024:
        return f"{b / (1024 * 1024):.1f}MB"
    return f"{b / (1024 * 1024 * 1024):.1f}GB"


def _clean_cell(v):
    """Convert non-JSON-serializable types."""
    if isinstance(v, Decimal):
        return float(v)
    if isinstance(v, (date, datetime)):
        return v.isoformat()
    if isinstance(v, timedelta):
        return str(v)
    if isinstance(v, bytes):
        return v.hex()
    return v


def _clean_row(row: tuple) -> list:
    return [_clean_cell(v) for v in row]


def _extract_metrics(stats: dict) -> QueryMetrics:
    """Extract QueryMetrics from Trino cursor.stats dict."""
    return QueryMetrics(
        cpu_time_ms=stats.get("cpuTimeMillis", 0),
        wall_time_ms=stats.get("wallTimeMillis", 0),
        queued_time_ms=stats.get("queuedTimeMillis", 0),
        planning_time_ms=stats.get("planningTimeMillis", 0),
        analysis_time_ms=stats.get("analysisTimeMillis", 0),
        elapsed_time_ms=stats.get("elapsedTimeMillis", 0),
        peak_memory_bytes=stats.get("peakMemoryBytes", 0),
        spilled_bytes=stats.get("spilledBytes", 0),
        physical_input_bytes=stats.get("physicalInputBytes", 0),
        physical_written_bytes=stats.get("physicalWrittenBytes", 0),
        internal_network_bytes=stats.get("internalNetworkInputBytes", 0),
        processed_rows=stats.get("processedRows", 0),
        processed_bytes=stats.get("processedBytes", 0),
        total_splits=stats.get("totalSplits", 0),
        completed_splits=stats.get("completedSplits", 0),
    )


# ── Skills ────────────────────────────────────────────────────────────────────

class TrinoQuerySkill(BaseSkill):
    """Execute SQL against Trino with performance metrics collection.

    Every query automatically captures: CPU time, wall time, memory usage,
    I/O bytes, splits, rows processed. Use this as an optimization baseline.
    """

    name = "trino_query"
    description = (
        "Execute SQL on Trino with performance metrics (CPU, memory, I/O, splits). "
        "Returns results + metrics baseline for optimization comparison."
    )
    group = "trino_query"
    args = [
        Arg("sql", "str", "SQL statement(s) to execute", required=True),
        Arg("catalog", "str", "Target catalog (default: from config)", required=False, default=""),
        Arg("schema", "str", "Target schema (default: from config)", required=False, default=""),
        Arg("limit", "int", "Maximum rows to return (default: 100)", required=False, default=100),
    ]

    def run(self, ctx, sql: str = "", catalog: str = "",
            schema: str = "", limit: int = 100) -> str:
        try:
            cfg = get_active_profile()
            conn = cfg.connect(catalog=catalog or None, schema=schema or None)
            cur = conn.cursor()
            t0 = time.monotonic()
            cur.execute(sql)
            t_ms = int((time.monotonic() - t0) * 1000)

            desc = cur.description or []
            cols = [c[0] for c in desc]
            rows = []
            for row in cur.fetchall():
                rows.append(dict(zip(cols, _clean_row(row))))
                if len(rows) >= limit:
                    rows = rows[:limit]
                    break

            # Collect metrics from cursor.stats
            stats = getattr(cur, 'stats', {}) or {}
            metrics = _extract_metrics(stats)
            query_id = getattr(cur, 'query_id', '') or ''

            conn.close()

            result = QueryResult(
                rows=rows, columns=cols, duration_ms=t_ms,
                truncated=len(rows) >= limit,
                query_id=query_id, metrics=metrics,
            )

            out = ctx.output
            if cols:
                out.progress(f"[{len(rows)} rows · {t_ms}ms]")
                for r in rows[:5]:
                    out.print("  " + "  ".join(str(r[c]) for c in cols))
                if len(rows) > 5:
                    out.print(f"  [dim]({len(rows)} rows returned)[/dim]")
            else:
                out.print(f"[green]OK[/green] ({t_ms}ms)")

            # Always show metrics
            out.print(f"  [dim]metrics: {metrics.summary_line()}[/dim]")

            return json.dumps(result.to_json(), ensure_ascii=False)

        except Exception as exc:
            return json.dumps({
                "error": str(exc),
                "rows": [], "columns": [],
                "duration_ms": 0, "truncated": False,
                "query_id": "", "metrics": None,
            }, ensure_ascii=False)


class TrinoExplainSkill(BaseSkill):
    """Run EXPLAIN on SQL and return the query plan."""

    name = "trino_explain"
    description = "Run EXPLAIN on SQL and return the query plan for performance analysis."
    group = "trino_query"
    args = [
        Arg("sql", "str", "SQL statement to EXPLAIN", required=True),
        Arg("catalog", "str", "Target catalog (default: from config)", required=False, default=""),
        Arg("schema", "str", "Target schema (default: from config)", required=False, default=""),
        Arg("type", "str", "EXPLAIN type (default: distributed)",
            required=False, default="distributed",
            choices=["logical", "distributed"]),
    ]

    def run(self, ctx, sql: str = "", catalog: str = "",
            schema: str = "", type: str = "distributed") -> str:
        explain_sql = f"EXPLAIN (TYPE {type.upper()}) {sql}"
        try:
            cfg = get_active_profile()
            conn = cfg.connect(catalog=catalog or None, schema=schema or None)
            cur = conn.cursor()
            cur.execute(explain_sql)
            plan_rows = cur.fetchall()
            conn.close()
            plan_text = "\n".join(r[0] for r in plan_rows) if plan_rows else ""
            out = ctx.output
            out.print(f"[dim]EXPLAIN {type}[/dim]")
            for line in plan_text.split("\n")[:20]:
                out.print("  " + line)
            if len(plan_text.split("\n")) > 20:
                out.print(f"  [dim]({len(plan_text.split(chr(10)))} lines truncated)[/dim]")
            return json.dumps({"plan": plan_text, "type": type, "sql": sql}, ensure_ascii=False)
        except Exception as exc:
            return json.dumps({"error": str(exc), "plan": ""}, ensure_ascii=False)


class TrinoSchemaSkill(BaseSkill):
    """Inspect Trino schema: list catalogs, schemas, tables, columns."""

    name = "trino_schema"
    description = (
        "Inspect Trino metadata: list catalogs, schemas, tables, or describe table columns. "
        "Use action='catalogs|schemas|tables|columns' to choose what to inspect."
    )
    group = "trino_query"
    args = [
        Arg("action", "str", "What to inspect: catalogs, schemas, tables, columns",
            required=True, choices=["catalogs", "schemas", "tables", "columns"]),
        Arg("catalog", "str", "Catalog name (for schemas/tables/columns)", required=False, default=""),
        Arg("schema", "str", "Schema name (for tables/columns)", required=False, default=""),
        Arg("table", "str", "Table name (for columns)", required=False, default=""),
    ]

    def run(self, ctx, action: str = "catalogs", catalog: str = "",
            schema: str = "", table: str = "") -> str:
        cfg = get_active_profile()
        cat = catalog or cfg.catalog
        sch = schema or cfg.schema

        sql_map = {
            "catalogs": "SHOW CATALOGS",
            "schemas": f"SHOW SCHEMAS FROM {cat}",
            "tables": f"SHOW TABLES FROM {cat}.{sch}",
            "columns": f"DESCRIBE {cat}.{sch}.{table}" if table else "SELECT 'error: table required'",
        }
        sql = sql_map.get(action, "SHOW CATALOGS")

        try:
            conn = cfg.connect(catalog=cat, schema=sch)
            cur = conn.cursor()
            cur.execute(sql)
            rows = cur.fetchall()
            conn.close()

            if action == "columns" and table:
                result = [{"column": r[0], "type": r[1], "extra": r[2], "comment": r[3]} for r in rows]
            else:
                result = [r[0] for r in rows]

            out = ctx.output
            for item in result[:20]:
                if isinstance(item, dict):
                    out.print(f"  {item['column']:<30} {item['type']}")
                else:
                    out.print(f"  {item}")
            if len(result) > 20:
                out.print(f"  [dim]({len(result)} items total)[/dim]")

            return json.dumps({"action": action, "result": result}, ensure_ascii=False)
        except Exception as exc:
            return json.dumps({"action": action, "error": str(exc), "result": []}, ensure_ascii=False)


def register(registry) -> None:
    registry.register(TrinoQuerySkill())
    registry.register(TrinoExplainSkill())
    registry.register(TrinoSchemaSkill())

    from .optimize import TrinoOptimizeSkill
    registry.register(TrinoOptimizeSkill())
