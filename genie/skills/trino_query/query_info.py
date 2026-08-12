"""Post-execution QueryInfo fetch + stage hotspot extraction.

After a measurement run, the coordinator exposes the full QueryInfo JSON at
``GET /v1/query/{query_id}`` — the same document the Web UI "JSON" tab shows.
This module fetches it (zero extra query execution) and distills the per-stage
runtime stats into a small ranked "hotspot" block for the iteration prompt.

Fetch must happen promptly after the run: the coordinator only keeps recent
queries in memory (query.max-history / query.min-expire-age).

All parsing is defensive — QueryInfo field spellings drift across Trino
versions, and a missing field must degrade to "no signal", never raise.
"""
from __future__ import annotations

import json
import re
import urllib.error
import urllib.request
from dataclasses import dataclass, field
from typing import Any, Optional

from .connection import TrinoProfile, get_active_profile

# Stages below this share of total CPU are summarized as one "omitted" line.
DEFAULT_CPU_SHARE_FLOOR = 0.05
DEFAULT_MAX_STAGES = 6
DEFAULT_BUDGET_CHARS = 1800

# Per-task input imbalance at or above this ratio (max/mean) is flagged as skew.
SKEW_RATIO_THRESHOLD = 2.0

_DURATION_RE = re.compile(r"^\s*([0-9.]+)\s*(ns|us|ms|s|m|h|d)\s*$")
_DURATION_MS = {"ns": 1e-6, "us": 1e-3, "ms": 1.0, "s": 1e3, "m": 6e4, "h": 3.6e6, "d": 8.64e7}

_SIZE_RE = re.compile(r"^\s*([0-9.]+)\s*([kKMGTP]?)i?B\s*$")
_SIZE_BYTES = {"": 1, "k": 1024, "K": 1024, "M": 1024**2, "G": 1024**3, "T": 1024**4, "P": 1024**5}


def parse_duration_ms(value: Any) -> float:
    """Parse a Trino duration ('1.23s', '45ms') or bare number into ms."""
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        m = _DURATION_RE.match(value)
        if m:
            return float(m.group(1)) * _DURATION_MS[m.group(2)]
    return 0.0


def parse_size_bytes(value: Any) -> float:
    """Parse a Trino data size ('1.5GB', '10B') or bare number into bytes."""
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        m = _SIZE_RE.match(value)
        if m:
            return float(m.group(1)) * _SIZE_BYTES[m.group(2)]
    return 0.0


def _human_bytes(b: float) -> str:
    for unit, div in (("GB", 1024**3), ("MB", 1024**2), ("KB", 1024)):
        if b >= div:
            return f"{b / div:.1f}{unit}"
    return f"{int(b)}B"


def _human_count(n: float) -> str:
    for unit, div in (("B", 1e9), ("M", 1e6), ("K", 1e3)):
        if n >= div:
            return f"{n / div:.1f}{unit}"
    return f"{int(n)}"


# ── Fetch ─────────────────────────────────────────────────────────────────────

def fetch_query_info(query_id: str, profile: Optional[TrinoProfile] = None,
                     timeout_s: float = 5.0) -> Optional[dict]:
    """Fetch QueryInfo JSON from the coordinator REST API.

    Returns None on any failure (endpoint blocked, query expired from history,
    auth mismatch) — callers treat that as "runtime detail unavailable".
    """
    # Strict type gates: query_id flows in from duck-typed measurement objects
    # (mocks in tests, adapters in prod) — anything but a plain string is "no id".
    if not query_id or not isinstance(query_id, str):
        return None
    try:
        cfg = profile or get_active_profile()
        url = f"{cfg.scheme}://{cfg.host}:{cfg.port}/v1/query/{query_id}"
        req = urllib.request.Request(url, headers={"X-Trino-User": str(cfg.user)})
        with urllib.request.urlopen(req, timeout=timeout_s) as resp:
            return json.load(resp)
    except Exception:
        # Fail-open like every other evidence collector: unreachable REST
        # endpoint, expired query, malformed profile → block simply absent.
        return None


# ── Stage extraction ──────────────────────────────────────────────────────────

@dataclass
class StageHotspot:
    """Runtime facts for one stage, distilled from QueryInfo."""
    stage_id: str
    cpu_ms: float = 0.0
    cpu_share: float = 0.0        # fraction of whole-query stage CPU
    blocked_ms: float = 0.0
    input_bytes: float = 0.0
    input_rows: float = 0.0
    output_bytes: float = 0.0
    output_rows: float = 0.0
    spilled_bytes: float = 0.0
    task_count: int = 0
    task_skew_ratio: float = 0.0  # max/mean per-task input rows; 0 = unknown
    operators: list[str] = field(default_factory=list)  # distinct operator types by CPU


def _stat(stats: dict, *keys: str) -> Any:
    for k in keys:
        if k in stats:
            return stats[k]
    return None


def _walk_stages(stage: Any) -> list[dict]:
    """Flatten the recursive outputStage tree into a list of stage dicts."""
    if not isinstance(stage, dict):
        return []
    found = [stage]
    for sub in stage.get("subStages") or []:
        found.extend(_walk_stages(sub))
    return found


def _all_stages(query_info: dict) -> list[dict]:
    """Stage dicts from either QueryInfo shape.

    Older Trino: recursive tree under ``outputStage``. Newer Trino (~447+,
    verified on 483): flat list under ``stages.stages``.
    """
    output_stage = query_info.get("outputStage")
    if output_stage:
        return _walk_stages(output_stage)
    stages = query_info.get("stages")
    if isinstance(stages, dict):
        flat = stages.get("stages")
        if isinstance(flat, list):
            return [s for s in flat if isinstance(s, dict)]
    if isinstance(stages, list):
        return [s for s in stages if isinstance(s, dict)]
    return []


def _short_stage_id(stage_id: Any) -> str:
    # "20250812_031500_00042_abcde.3" → "S3"
    if isinstance(stage_id, str) and "." in stage_id:
        return f"S{stage_id.rsplit('.', 1)[1]}"
    return f"S{stage_id}"


def _task_input_rows(task: dict) -> Optional[float]:
    stats = task.get("stats") or task.get("taskStats") or {}
    if not isinstance(stats, dict):
        return None
    v = _stat(stats, "rawInputPositions", "physicalInputPositions",
              "processedInputPositions")
    return float(v) if isinstance(v, (int, float)) else None


def _operator_cpu_ranking(stage_stats: dict) -> list[str]:
    """Top operator types of a stage by CPU, e.g. ['TableScanOperator', 'HashBuilderOperator']."""
    summaries = stage_stats.get("operatorSummaries")
    if not isinstance(summaries, list):
        return []
    by_type: dict[str, float] = {}
    for op in summaries:
        if not isinstance(op, dict):
            continue
        op_type = op.get("operatorType")
        if not isinstance(op_type, str):
            continue
        cpu = parse_duration_ms(_stat(op, "addInputCpu", "totalCpuTime") or 0)
        cpu += parse_duration_ms(op.get("getOutputCpu") or 0)
        cpu += parse_duration_ms(op.get("finishCpu") or 0)
        by_type[op_type] = by_type.get(op_type, 0.0) + cpu
    ranked = sorted(by_type.items(), key=lambda kv: kv[1], reverse=True)
    return [t for t, cpu in ranked[:3] if cpu > 0]


def extract_stage_hotspots(query_info: dict) -> list[StageHotspot]:
    """Distill QueryInfo into per-stage hotspots, sorted by CPU descending."""
    stages = _all_stages(query_info)
    hotspots: list[StageHotspot] = []

    for st in stages:
        stats = st.get("stageStats") or st.get("latestAttemptExecutionInfo", {}).get("stats") or {}
        if not isinstance(stats, dict):
            stats = {}
        h = StageHotspot(stage_id=_short_stage_id(st.get("stageId", "?")))
        h.cpu_ms = parse_duration_ms(_stat(stats, "totalCpuTime") or 0)
        h.blocked_ms = parse_duration_ms(_stat(stats, "totalBlockedTime") or 0)
        h.input_bytes = parse_size_bytes(_stat(
            stats, "rawInputDataSize", "physicalInputDataSize",
            "processedInputDataSize") or 0)
        h.input_rows = float(_stat(
            stats, "rawInputPositions", "physicalInputPositions",
            "processedInputPositions") or 0)
        h.output_bytes = parse_size_bytes(_stat(stats, "outputDataSize") or 0)
        h.output_rows = float(_stat(stats, "outputPositions") or 0)
        h.spilled_bytes = parse_size_bytes(_stat(stats, "spilledDataSize") or 0)
        h.operators = _operator_cpu_ranking(stats)

        tasks = st.get("tasks") or []
        per_task = [r for r in (_task_input_rows(t) for t in tasks if isinstance(t, dict))
                    if r is not None]
        h.task_count = len(per_task) or (len(tasks) if isinstance(tasks, list) else 0)
        if len(per_task) >= 2:
            mean = sum(per_task) / len(per_task)
            if mean > 0:
                h.task_skew_ratio = max(per_task) / mean

        hotspots.append(h)

    total_cpu = sum(h.cpu_ms for h in hotspots)
    if total_cpu > 0:
        for h in hotspots:
            h.cpu_share = h.cpu_ms / total_cpu
    hotspots.sort(key=lambda h: h.cpu_ms, reverse=True)
    return hotspots


# ── Formatting ────────────────────────────────────────────────────────────────

def _hotspot_line(h: StageHotspot) -> str:
    parts = [
        f"{h.stage_id}  cpu {h.cpu_share:.0%} ({h.cpu_ms / 1000.0:.1f}s)",
        f"in {_human_bytes(h.input_bytes)}/{_human_count(h.input_rows)} rows",
        f"out {_human_count(h.output_rows)} rows",
    ]
    if h.blocked_ms >= 1000:
        parts.append(f"blocked {h.blocked_ms / 1000.0:.1f}s")
    if h.spilled_bytes > 0:
        parts.append(f"SPILL {_human_bytes(h.spilled_bytes)}")
    if h.task_skew_ratio >= SKEW_RATIO_THRESHOLD:
        parts.append(f"SKEW max/avg={h.task_skew_ratio:.1f} ({h.task_count} tasks)")
    if h.operators:
        parts.append(f"top-ops: {', '.join(h.operators)}")
    return "  ".join(parts)


def format_stage_hotspots(hotspots: list[StageHotspot], *,
                          max_stages: int = DEFAULT_MAX_STAGES,
                          cpu_share_floor: float = DEFAULT_CPU_SHARE_FLOOR,
                          budget_chars: int = DEFAULT_BUDGET_CHARS) -> str:
    """Render ranked hotspot lines under a hard char budget.

    Anomalous stages (spill or skew) are always kept even below the CPU floor —
    they are exactly the signal this block exists to surface.
    """
    if not hotspots:
        return ""
    keep, dropped = [], 0
    for i, h in enumerate(hotspots):
        anomalous = h.spilled_bytes > 0 or h.task_skew_ratio >= SKEW_RATIO_THRESHOLD
        if (i < max_stages and h.cpu_share >= cpu_share_floor) or anomalous:
            keep.append(h)
        else:
            dropped += 1

    lines = ["[Runtime stage hotspots — measured, per-stage]"]
    shown = 0
    for h in keep:
        line = _hotspot_line(h)
        if sum(len(x) + 1 for x in lines) + len(line) > budget_chars:
            break
        lines.append(line)
        shown += 1
    omitted = len(hotspots) - shown
    if omitted > 0:
        # Stages cut by the char budget were kept on merit (high CPU or
        # spill/skew) — the 'below floor, no spill/skew' claim only holds
        # when nothing was budget-cut.
        budget_cut = keep[shown:]
        anomalous_cut = sum(
            1 for h in budget_cut
            if h.spilled_bytes > 0 or h.task_skew_ratio >= SKEW_RATIO_THRESHOLD)
        if anomalous_cut:
            lines.append(
                f"({omitted} more stages omitted — char budget; "
                f"{anomalous_cut} of them WITH spill/skew)")
        elif budget_cut:
            lines.append(f"({omitted} more stages omitted — char budget)")
        else:
            lines.append(f"({omitted} more stages omitted — each below {cpu_share_floor:.0%} CPU, no spill/skew)")
    return "\n".join(lines)


def stage_hotspot_block(query_id: str, profile: Optional[TrinoProfile] = None) -> str:
    """One-call convenience: fetch QueryInfo and render the hotspot block.

    Returns "" when the coordinator API is unreachable or the query has
    already expired from history — callers simply omit the block.
    """
    info = fetch_query_info(query_id, profile=profile)
    if not info:
        return ""
    return format_stage_hotspots(extract_stage_hotspots(info))
