"""table_landscape — touched-table extraction, SHOW STATS parsing, formatting."""
from __future__ import annotations

from genie.skills.trino_query.table_landscape import (
    collect_table_landscape,
    format_table_landscape,
    table_landscape_block,
    touched_tables,
)

_SQL = """
WITH daily AS (
  SELECT user_id, count(*) AS n
  FROM hive.raw.events
  WHERE event_date >= DATE '2026-08-01'
  GROUP BY user_id
)
SELECT d.n, u.country
FROM daily d
JOIN hive.dim.users u ON u.user_id = d.user_id
"""


def test_touched_tables_excludes_ctes():
    tables = touched_tables(_SQL)
    assert "hive.raw.events" in tables
    assert "hive.dim.users" in tables
    assert not any(t.endswith("daily") for t in tables)


def test_touched_tables_bad_sql_degrades_to_empty():
    assert touched_tables("THIS IS NOT SQL ((((") == []


# ── Fake executor: dispatch by statement shape ────────────────────────────────

_STATS_EVENTS = [
    # (column_name, data_size, distinct_values_count, nulls_fraction, row_count, low, high)
    ("user_id", 9.6e9, 45_000_000.0, 0.0, None, None, None),
    ("event_date", 4.8e9, 730.0, 0.0, None, "2024-08-01", "2026-08-12"),
    ("payload", 8.0e11, None, 0.02, None, None, None),
    (None, None, None, None, 1.2e9, None, None),        # table summary row
]

_STATS_USERS_MISSING = [
    ("user_id", None, None, None, None, None, None),
    ("country", None, None, None, None, None, None),
    (None, None, None, None, None, None, None),
]

_DDL_EVENTS = [(
    "CREATE TABLE hive.raw.events (\n"
    "   user_id bigint,\n   event_date date,\n   payload varchar\n)\n"
    "WITH (\n   partitioned_by = ARRAY['event_date'],\n   format = 'PARQUET'\n)",
)]


def _fake_execute(sql: str) -> list:
    s = sql.strip()
    if s.startswith("SHOW STATS FOR hive.raw.events"):
        return _STATS_EVENTS
    if s.startswith("SHOW STATS FOR hive.dim.users"):
        return _STATS_USERS_MISSING
    if s.startswith("SHOW CREATE TABLE hive.raw.events"):
        return _DDL_EVENTS
    if s.startswith("SHOW CREATE TABLE hive.dim.users"):
        return [("CREATE TABLE hive.dim.users (user_id bigint, country varchar)",)]
    if "$partitions" in s:
        return [(730, 9_000_000.0, 1_500_000.0)]        # count, max, avg → skew 6.0
    raise AssertionError(f"unexpected sql: {sql}")


def test_collect_parses_stats_partitions_and_missing_stats():
    lands = collect_table_landscape(_SQL, _fake_execute)
    by_name = {l.table: l for l in lands}

    events = by_name["hive.raw.events"]
    assert events.row_count == 1.2e9
    assert events.partition_columns == ["event_date"]
    assert events.partition_count == 730
    assert abs(events.partition_skew_ratio - 6.0) < 0.01
    assert not events.stats_missing
    # Referenced-columns filter: payload is never referenced by the SQL.
    assert all(c.name != "payload" for c in events.columns)
    assert any(c.name == "user_id" and c.ndv == 45_000_000.0 for c in events.columns)

    users = by_name["hive.dim.users"]
    assert users.stats_missing


def test_collect_show_stats_failure_degrades_to_error_entry():
    def _always_fail(sql: str) -> list:
        raise RuntimeError("no permission")
    lands = collect_table_landscape("SELECT * FROM hive.raw.events", _always_fail)
    assert len(lands) == 1
    assert "SHOW STATS failed" in lands[0].error


def test_format_surfaces_signals():
    text = format_table_landscape(collect_table_landscape(_SQL, _fake_execute))
    assert "hive.raw.events — 1.2B rows" in text
    assert "partitioned by (event_date)" in text
    assert "SKEW max/avg=6.0" in text
    assert "STATS MISSING" in text          # hive.dim.users
    assert "NDV 45.0M" in text


def test_block_never_raises():
    def _boom(sql: str) -> list:
        raise RuntimeError("boom")
    assert table_landscape_block("not sql ((", _boom) == ""


def test_format_respects_budget():
    text = format_table_landscape(
        collect_table_landscape(_SQL, _fake_execute), budget_chars=80)
    assert len(text) <= 120
    assert "truncated" in text


def test_parse_partition_columns_handles_trino_dict_column_name():
    # The --direct executor builds dict rows from cursor.description, and
    # Trino names the SHOW CREATE TABLE output column 'Create Table' —
    # capitalized, with a space. Partition parsing must still find the DDL.
    def _execute(sql: str) -> list:
        s = sql.strip()
        if s.startswith("SHOW STATS"):
            return [dict(zip(
                ("column_name", "data_size", "distinct_values_count",
                 "nulls_fraction", "row_count", "low_value", "high_value"),
                row)) for row in _STATS_EVENTS]
        if s.startswith("SHOW CREATE TABLE"):
            return [{"Create Table": _DDL_EVENTS[0][0]}]
        if "$partitions" in s:
            return [{"_col0": 730, "_col1": 9_000_000.0, "_col2": 1_500_000.0}]
        raise AssertionError(f"unexpected sql: {sql}")

    lands = collect_table_landscape("SELECT user_id FROM hive.raw.events", _execute)
    assert lands[0].partition_columns == ["event_date"]
    assert lands[0].partition_count == 730


def test_partition_probe_quotes_only_the_table_part():
    # Quoting the whole dotted name as one identifier ("hive.raw.events$partitions")
    # makes Trino resolve it as a single-part table in the session schema —
    # the probe would always fail. Only the table part carries the quotes.
    seen: list[str] = []

    def _execute(sql: str) -> list:
        s = sql.strip()
        seen.append(s)
        if s.startswith("SHOW STATS"):
            return _STATS_EVENTS
        if s.startswith("SHOW CREATE TABLE"):
            return _DDL_EVENTS
        if "$partitions" in s:
            return [(730, 9_000_000.0, 1_500_000.0)]
        raise AssertionError(f"unexpected sql: {sql}")

    collect_table_landscape("SELECT user_id FROM hive.raw.events", _execute)
    probe = next(s for s in seen if "$partitions" in s)
    assert 'hive.raw."events$partitions"' in probe
    assert '"hive.raw.events$partitions"' not in probe


def test_collect_empty_stats_rows_is_error_not_fabricated_landscape():
    # A fail-open executor that swallows errors returns [] — that must render
    # as 'unavailable', never as a fabricated '? rows' landscape entry that
    # feeds the diagnose step false evidence.
    lands = collect_table_landscape("SELECT * FROM hive.raw.events", lambda sql: [])
    assert len(lands) == 1
    assert lands[0].error
    assert "unavailable" in format_table_landscape(lands)
