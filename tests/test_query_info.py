"""query_info — QueryInfo fetch guards + stage hotspot extraction/formatting.

Golden-fixture principle: every pathology the hotspot block exists to surface
(CPU concentration, task skew, spill) must survive extraction and formatting —
if a signal can silently vanish, the block is worse than no block.
"""
from __future__ import annotations

from unittest.mock import MagicMock

from genie.skills.trino_query.query_info import (
    extract_stage_hotspots,
    fetch_query_info,
    format_stage_hotspots,
    parse_duration_ms,
    parse_size_bytes,
)


def _stage(stage_id, cpu, *, blocked="0ms", in_size="0B", in_rows=0,
           out_rows=0, spilled="0B", tasks=None, subs=None, ops=None):
    return {
        "stageId": stage_id,
        "stageStats": {
            "totalCpuTime": cpu,
            "totalBlockedTime": blocked,
            "rawInputDataSize": in_size,
            "rawInputPositions": in_rows,
            "outputDataSize": "0B",
            "outputPositions": out_rows,
            "spilledDataSize": spilled,
            "operatorSummaries": ops or [],
        },
        "tasks": tasks or [],
        "subStages": subs or [],
    }


def _task(input_rows):
    return {"stats": {"rawInputPositions": input_rows}}


_QUERY_INFO = {
    "queryId": "20260812_000000_00001_test",
    "state": "FINISHED",
    "outputStage": _stage(
        "q.0", "1.0s",
        subs=[
            _stage(
                "q.1", "60.0s",
                in_size="840MB", in_rows=1_200_000, out_rows=12_000,
                blocked="8.0s",
                tasks=[_task(100), _task(100), _task(100), _task(900)],  # skew 3.0
                ops=[
                    {"operatorType": "TableScanOperator", "addInputCpu": "40.0s"},
                    {"operatorType": "LookupJoinOperator", "addInputCpu": "15.0s"},
                ],
            ),
            _stage(
                "q.2", "35.0s",
                in_size="200MB", in_rows=500_000, out_rows=400_000,
                spilled="2GB",
            ),
            # Cold stage below every threshold — must fold into the omitted line.
            _stage("q.3", "1.0s"),
        ],
    ),
}


# ── Parsers ───────────────────────────────────────────────────────────────────

def test_parse_duration_units():
    assert parse_duration_ms("1.5s") == 1500.0
    assert parse_duration_ms("250ms") == 250.0
    assert parse_duration_ms("2m") == 120_000.0
    assert parse_duration_ms(42) == 42.0
    assert parse_duration_ms("garbage") == 0.0


def test_parse_size_units():
    assert parse_size_bytes("1KB") == 1024.0
    assert parse_size_bytes("1.5GB") == 1.5 * 1024**3
    assert parse_size_bytes("10B") == 10.0
    assert parse_size_bytes(7) == 7.0
    assert parse_size_bytes("garbage") == 0.0


# ── Fetch guards ──────────────────────────────────────────────────────────────

def test_fetch_rejects_non_string_query_id():
    assert fetch_query_info(MagicMock()) is None
    assert fetch_query_info("") is None
    assert fetch_query_info(None) is None


def test_fetch_unreachable_coordinator_returns_none():
    from genie.skills.trino_query.connection import TrinoProfile
    profile = TrinoProfile(host="127.0.0.1", port=1, scheme="http", user="t")
    assert fetch_query_info("some_query_id", profile=profile, timeout_s=0.2) is None


# ── Extraction ────────────────────────────────────────────────────────────────

def test_hotspots_sorted_by_cpu_with_shares():
    hotspots = extract_stage_hotspots(_QUERY_INFO)
    assert [h.stage_id for h in hotspots][:2] == ["S1", "S2"]
    total = 1.0 + 60.0 + 35.0 + 1.0
    assert abs(hotspots[0].cpu_share - 60.0 / total) < 0.01


def test_task_skew_detected():
    h1 = next(h for h in extract_stage_hotspots(_QUERY_INFO) if h.stage_id == "S1")
    assert abs(h1.task_skew_ratio - 3.0) < 0.01   # max 900 / mean 300
    assert h1.task_count == 4


def test_spill_extracted():
    h2 = next(h for h in extract_stage_hotspots(_QUERY_INFO) if h.stage_id == "S2")
    assert h2.spilled_bytes == 2 * 1024**3


def test_empty_query_info_yields_no_hotspots():
    assert extract_stage_hotspots({}) == []
    assert format_stage_hotspots([]) == ""


# ── Formatting (fidelity: pathologies must survive) ───────────────────────────

def test_format_surfaces_skew_and_spill_and_omits_cold_stages():
    block = format_stage_hotspots(extract_stage_hotspots(_QUERY_INFO))
    assert "S1" in block and "SKEW max/avg=3.0" in block
    assert "S2" in block and "SPILL 2.0GB" in block
    assert "S3" not in block          # cold stage folded
    assert "omitted" in block         # …but visibly, never silently


def test_format_keeps_anomalous_stage_below_cpu_floor():
    info = {
        "queryId": "q", "outputStage": _stage(
            "q.0", "100.0s",
            subs=[_stage("q.9", "0.5s", spilled="1GB")],  # ~0.5% CPU but spills
        ),
    }
    block = format_stage_hotspots(extract_stage_hotspots(info))
    assert "S9" in block and "SPILL" in block


def test_format_respects_char_budget():
    info = {
        "queryId": "q",
        "outputStage": _stage(
            "q.0", "10.0s",
            subs=[_stage(f"q.{i}", "10.0s", in_rows=10**6) for i in range(1, 40)],
        ),
    }
    block = format_stage_hotspots(
        extract_stage_hotspots(info), budget_chars=400, max_stages=50,
        cpu_share_floor=0.0,
    )
    assert len(block) < 600
    assert "omitted" in block


def test_budget_cut_note_never_claims_no_spill_for_spilling_stages():
    # The char budget can cut stages that were KEPT because they spill/skew;
    # the omission note must not affirmatively claim 'no spill/skew' about
    # them — that steers the diagnosis away from the real bottleneck.
    info = {
        "queryId": "q",
        "outputStage": _stage(
            "q.0", "10.0s",
            subs=[_stage(f"q.{i}", "50.0s", in_rows=10**6) for i in range(1, 10)]
                 + [_stage("q.99", "0.5s", spilled="4GB")],
        ),
    }
    block = format_stage_hotspots(
        extract_stage_hotspots(info), budget_chars=300, max_stages=50,
        cpu_share_floor=0.0,
    )
    assert "omitted" in block
    assert "no spill/skew" not in block
    assert "spill/skew" in block   # the cut anomaly is called out, not hidden
