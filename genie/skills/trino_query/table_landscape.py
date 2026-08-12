"""Data-landscape collection for tables touched by a query.

Feeds Step A (diagnose) with the facts the CBO itself uses — per-column NDV,
null fraction, row counts — plus the table's physical shape (partition
columns) from SHOW CREATE TABLE, and partition-level row skew from Iceberg's
``$partitions`` metadata table when available.

Everything here is metadata-cost: SHOW STATS / SHOW CREATE TABLE do not scan
data, and the ``$partitions`` probe is a single aggregate over a metadata
table. No probe touches user data rows.

Column-stat fidelity/size policy: only columns actually referenced by the SQL
are shown (join keys and filter columns are what change rewrite decisions);
when references can't be resolved (e.g. SELECT *), fall back to the largest
columns by data size.

The executor is injected as ``execute_fn(sql) -> list[row]`` so tests can fake
it and the research loop can reuse its own connection handling. Rows may be
sequences or dicts; parsing is defensive and per-table failures degrade to an
error note, never an exception.
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any, Callable, Optional

DEFAULT_MAX_TABLES = 6
DEFAULT_MAX_COLUMNS = 8
DEFAULT_BUDGET_CHARS = 1800

ExecuteFn = Callable[[str], list]


# ── Touched-table extraction ──────────────────────────────────────────────────

def touched_tables(sql: str) -> list[str]:
    """Physical tables referenced by the SQL (CTE names excluded), deduped,
    qualified as far as the SQL itself qualifies them."""
    try:
        import sqlglot
        from sqlglot import exp
    except ImportError:
        return []
    try:
        statements = sqlglot.parse(sql, read="trino")
    except Exception:
        return []

    tables: list[str] = []
    seen: set[str] = set()
    for stmt in statements:
        if stmt is None:
            continue
        cte_names = {cte.alias_or_name.lower() for cte in stmt.find_all(exp.CTE)}
        for tbl in stmt.find_all(exp.Table):
            if tbl.name.lower() in cte_names and not tbl.db:
                continue
            parts = [p for p in (tbl.catalog, tbl.db, tbl.name) if p]
            fq = ".".join(parts).lower()
            if fq and fq not in seen:
                seen.add(fq)
                tables.append(fq)
    return tables


def referenced_columns(sql: str) -> set[str]:
    """Lower-cased column names referenced anywhere in the SQL."""
    try:
        import sqlglot
        from sqlglot import exp
    except ImportError:
        return set()
    try:
        statements = sqlglot.parse(sql, read="trino")
    except Exception:
        return set()
    cols: set[str] = set()
    for stmt in statements:
        if stmt is None:
            continue
        for col in stmt.find_all(exp.Column):
            if col.name:
                cols.add(col.name.lower())
    return cols


# ── Data classes ──────────────────────────────────────────────────────────────

@dataclass
class ColumnStats:
    name: str
    ndv: Optional[float] = None
    nulls_fraction: Optional[float] = None
    data_size: Optional[float] = None
    low: Optional[str] = None
    high: Optional[str] = None


@dataclass
class TableLandscape:
    table: str
    row_count: Optional[float] = None
    columns: list[ColumnStats] = field(default_factory=list)
    partition_columns: list[str] = field(default_factory=list)
    partition_count: Optional[int] = None
    partition_skew_ratio: Optional[float] = None  # max/avg partition rows
    stats_missing: bool = False
    error: Optional[str] = None


# ── Row helpers (SHOW STATS rows may arrive as sequences or dicts) ────────────

_SHOW_STATS_COLS = ("column_name", "data_size", "distinct_values_count",
                    "nulls_fraction", "row_count", "low_value", "high_value")


def _cell(row: Any, idx: int, key: str) -> Any:
    if isinstance(row, dict):
        return row.get(key)
    try:
        return row[idx]
    except (IndexError, TypeError):
        return None


def _num(v: Any) -> Optional[float]:
    if isinstance(v, (int, float)):
        return float(v)
    try:
        return float(v) if v not in (None, "") else None
    except (TypeError, ValueError):
        return None


# ── Collection ────────────────────────────────────────────────────────────────

_PARTITION_DDL_RE = re.compile(
    r"(?:partitioned_by|partitioning)\s*=\s*ARRAY\s*\[(.*?)\]",
    re.IGNORECASE | re.DOTALL,
)


def _parse_show_stats(rows: list) -> tuple[Optional[float], list[ColumnStats]]:
    row_count: Optional[float] = None
    cols: list[ColumnStats] = []
    for row in rows:
        name = _cell(row, 0, "column_name")
        if name is None:
            row_count = _num(_cell(row, 4, "row_count"))
            continue
        cols.append(ColumnStats(
            name=str(name),
            data_size=_num(_cell(row, 1, "data_size")),
            ndv=_num(_cell(row, 2, "distinct_values_count")),
            nulls_fraction=_num(_cell(row, 3, "nulls_fraction")),
            low=None if _cell(row, 5, "low_value") is None else str(_cell(row, 5, "low_value")),
            high=None if _cell(row, 6, "high_value") is None else str(_cell(row, 6, "high_value")),
        ))
    return row_count, cols


def _parse_partition_columns(ddl_rows: list) -> list[str]:
    if not ddl_rows:
        return []
    first = ddl_rows[0]
    if isinstance(first, dict):
        # Trino names the SHOW CREATE TABLE column 'Create Table' (DB-API
        # cursor.description keeps the space and capitals); match key
        # case/space-insensitively, falling back to the sole value of a
        # single-column row.
        ddl = next(
            (v for k, v in first.items()
             if isinstance(k, str) and k.replace(" ", "_").lower() == "create_table"),
            None,
        )
        if ddl is None and len(first) == 1:
            ddl = next(iter(first.values()))
    else:
        ddl = _cell(first, 0, "create_table")
    if not isinstance(ddl, str):
        return []
    m = _PARTITION_DDL_RE.search(ddl)
    if not m:
        return []
    return [p.strip().strip("'\"").lower() for p in m.group(1).split(",") if p.strip()]


def _partition_profile(execute_fn: ExecuteFn, table: str) -> tuple[Optional[int], Optional[float]]:
    """(partition_count, max/avg row skew) via Iceberg $partitions; None-pair
    when the connector has no such metadata table."""
    # Only the table part carries the $partitions suffix and needs quoting;
    # quoting the whole dotted name would make Trino resolve it as a
    # single-part identifier in the session catalog/schema.
    parts = table.split(".")
    parts[-1] = f'"{parts[-1]}$partitions"'
    ref = ".".join(parts)
    try:
        rows = execute_fn(
            f"SELECT count(*), max(record_count), avg(record_count) FROM {ref}"
        )
    except Exception:
        return None, None
    if not rows:
        return None, None
    count = _num(_cell(rows[0], 0, "_col0"))
    max_rows = _num(_cell(rows[0], 1, "_col1"))
    avg_rows = _num(_cell(rows[0], 2, "_col2"))
    skew = (max_rows / avg_rows) if (max_rows and avg_rows) else None
    return (int(count) if count is not None else None), skew


def collect_table_landscape(sql: str, execute_fn: ExecuteFn, *,
                            max_tables: int = DEFAULT_MAX_TABLES) -> list[TableLandscape]:
    """SHOW STATS + SHOW CREATE TABLE (+ $partitions profile) per touched table."""
    tables = touched_tables(sql)[:max_tables]
    ref_cols = referenced_columns(sql)
    result: list[TableLandscape] = []

    for table in tables:
        land = TableLandscape(table=table)
        try:
            stats_rows = execute_fn(f"SHOW STATS FOR {table}")
            if not stats_rows:
                # SHOW STATS on a real table always yields at least the
                # summary row; [] means a fail-open executor swallowed an
                # error. Degrade to the honest 'unavailable' note instead of
                # rendering a fabricated landscape.
                land.error = "SHOW STATS returned no rows (executor failure?)"
                result.append(land)
                continue
            row_count, cols = _parse_show_stats(stats_rows)
            land.row_count = row_count
            # Stats are "missing" when no column has an NDV — the CBO signal.
            land.stats_missing = bool(cols) and all(c.ndv is None for c in cols)
            if ref_cols:
                referenced = [c for c in cols if c.name.lower() in ref_cols]
                cols = referenced or cols
            cols.sort(key=lambda c: (c.data_size or 0), reverse=True)
            land.columns = cols[:DEFAULT_MAX_COLUMNS]
        except Exception as exc:
            land.error = f"SHOW STATS failed: {exc}"
            result.append(land)
            continue
        try:
            land.partition_columns = _parse_partition_columns(
                execute_fn(f"SHOW CREATE TABLE {table}"))
        except Exception:
            pass  # DDL is nice-to-have; stats already collected
        if land.partition_columns:
            land.partition_count, land.partition_skew_ratio = _partition_profile(
                execute_fn, table)
        result.append(land)
    return result


# ── Formatting ────────────────────────────────────────────────────────────────

def _fmt_num(v: Optional[float]) -> str:
    if v is None:
        return "?"
    for unit, div in (("B", 1e9), ("M", 1e6), ("K", 1e3)):
        if v >= div:
            return f"{v / div:.1f}{unit}"
    return f"{int(v)}"


def _column_line(c: ColumnStats) -> str:
    bits = [c.name]
    bits.append(f"NDV {_fmt_num(c.ndv)}")
    if c.nulls_fraction is not None and c.nulls_fraction > 0.01:
        bits.append(f"nulls {c.nulls_fraction:.0%}")
    if c.low is not None and c.high is not None:
        rng = f"{c.low}..{c.high}"
        if len(rng) <= 40:
            bits.append(rng)
    return " ".join(bits)


def format_table_landscape(landscapes: list[TableLandscape], *,
                           budget_chars: int = DEFAULT_BUDGET_CHARS) -> str:
    if not landscapes:
        return ""
    lines = ["[Table landscape — SHOW STATS, referenced columns only]"]
    for land in landscapes:
        if land.error:
            lines.append(f"{land.table} — unavailable ({land.error})")
            continue
        head = f"{land.table} — {_fmt_num(land.row_count)} rows"
        if land.partition_columns:
            head += f", partitioned by ({', '.join(land.partition_columns)})"
            if land.partition_count:
                head += f" [{land.partition_count} partitions"
                if land.partition_skew_ratio and land.partition_skew_ratio >= 2.0:
                    head += f", SKEW max/avg={land.partition_skew_ratio:.1f}"
                head += "]"
        if land.stats_missing:
            head += " — STATS MISSING (CBO blind; ANALYZE would help)"
        lines.append(head)
        if not land.stats_missing and land.columns:
            lines.append("  cols: " + "; ".join(_column_line(c) for c in land.columns))

    text = "\n".join(lines)
    if len(text) > budget_chars:
        cut = text[:budget_chars]
        text = cut[:cut.rfind("\n")] + "\n(landscape truncated)"
    return text


def table_landscape_block(sql: str, execute_fn: ExecuteFn, *,
                          budget_chars: int = DEFAULT_BUDGET_CHARS) -> str:
    """One-call convenience; '' when nothing is collectible."""
    try:
        return format_table_landscape(
            collect_table_landscape(sql, execute_fn), budget_chars=budget_chars)
    except Exception:
        return ""
