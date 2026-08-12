"""Compact, LLM-readable skeleton of a Trino EXPLAIN (FORMAT JSON) plan.

``plan_signature`` reduces a plan to a hashable structural fingerprint for
equivalence checks; this module reduces the same tree to a *readable* text
skeleton for prompt injection. It keeps every field that carries optimization
signal — operator kind, join type + distribution, join criteria, scanned
tables, filter predicates, aggregation phase/functions, exchange placement,
per-node row/byte estimates — and drops the volatile noise that dominates raw
EXPLAIN output (symbol assignments, layouts, dynamic-filter ids, formatting).

Budgeting: the renderer is bounded by ``max_lines``. When a full render
exceeds the budget it degrades in two deterministic steps rather than blindly
truncating the tail:

1. compact mode — keep only decision nodes (joins, scans, aggregates,
   exchanges, sort/limit/window/set-ops), note how many low-signal nodes
   were folded;
2. hard truncation — if compact still exceeds the budget, cut and note how
   many nodes were omitted.

Pure and deterministic; returns "" for unusable input so callers can gate
prompt injection on truthiness (same convention as
``format_directions_for_prompt``).
"""
from __future__ import annotations

import json
import math
import re
from typing import Any, Optional, Union

from .plan_signature import _TABLE_RE, _extract_table

_MAX_PLAN_DEPTH = 50            # matches pre_execution_diagnosis recursion guard
DEFAULT_MAX_LINES = 80

# Bracket tokens worth keeping from node names like "Join[INNER, PARTITIONED]".
_BRACKET_KEYWORDS = {
    "INNER", "LEFT", "RIGHT", "FULL", "SEMI", "CROSS",
    "PARTITIONED", "REPLICATED", "REPARTITION", "REPLICATE",
    "PARTIAL", "FINAL", "SINGLE", "INTERMEDIATE",
    "STREAMING", "GATHER", "HASH", "ROUND_ROBIN", "SCALED",
}

# Both spellings appear in the wild: fixtures/older output use "Join" with a
# type in the descriptor; live Trino (e.g. 467) emits typed names like
# "InnerJoin" with {"criteria", "distribution"} in the descriptor.
_JOIN_OPS = {
    "Join", "SemiJoin", "CrossJoin", "HashJoin", "MergeJoin",
    "InnerJoin", "LeftJoin", "RightJoin", "FullJoin",
}

# Decision nodes kept in compact mode. Everything else (Project, Filter-only
# shaping, Values, …) folds away when the budget forces it.
_HIGH_SIGNAL_OPS = _JOIN_OPS | {
    "Output", "TableScan", "ScanFilter", "ScanProject", "ScanFilterProject",
    "Aggregate", "GroupId", "Exchange", "RemoteExchange", "LocalExchange",
    "RemoteSource", "RemoteMerge", "LocalMerge",
    "Sort", "PartialSort", "TopN", "PartialTopN", "Limit", "Window", "Unnest",
    "Union", "Intersect", "Except", "Distinct", "MarkDistinct", "RowNumber",
}

_PREDICATE_MAX_CHARS = 100


def _fmt_count(value: float) -> str:
    """1234567.0 → '1.2M' (deterministic, one decimal)."""
    for threshold, suffix in ((1e12, "T"), (1e9, "B"), (1e6, "M"), (1e3, "K")):
        if abs(value) >= threshold:
            return f"{value / threshold:.1f}{suffix}"
    return f"{value:.0f}"


def _fmt_bytes(value: float) -> str:
    for threshold, suffix in ((1024**4, "TB"), (1024**3, "GB"),
                              (1024**2, "MB"), (1024, "KB")):
        if abs(value) >= threshold:
            return f"{value / threshold:.1f}{suffix}"
    return f"{value:.0f}B"


def _safe_number(val: object) -> Optional[float]:
    """Finite non-bool number or None (bool is an int subclass — reject)."""
    if val is None or isinstance(val, bool) or not isinstance(val, (int, float)):
        return None
    if not math.isfinite(val):
        return None
    return float(val)


def _node_estimates(node: dict) -> tuple[Optional[float], Optional[float]]:
    estimates = node.get("estimates") or []
    if not isinstance(estimates, list):
        return None, None
    est = next((e for e in estimates if isinstance(e, dict)), None)
    if est is None:
        return None, None
    return _safe_number(est.get("outputRowCount")), _safe_number(est.get("outputSizeInBytes"))


def _bracket_keywords(name: str) -> list[str]:
    """Signal-bearing tokens from a name bracket, in order, deduped."""
    start = name.find("[")
    if start == -1:
        return []
    bracket = name[start + 1:].rstrip("]")
    out: list[str] = []
    seen: set[str] = set()
    for tok in re.split(r"[,\s]+", bracket):
        tok = tok.strip()
        if not tok:
            continue
        if _TABLE_RE.fullmatch(tok):
            low = tok.lower()
            if low not in seen:
                seen.add(low)
                out.append(low)
            continue
        # LEFT_PARTITIONED-style compounds: '_' splits keywords, but only
        # after the table check above (table names may contain underscores).
        for sub in tok.split("_"):
            up = sub.upper()
            if up in _BRACKET_KEYWORDS and up not in seen:
                seen.add(up)
                out.append(up)
    return out


def _truncate(text: str) -> str:
    text = " ".join(text.split())
    if len(text) > _PREDICATE_MAX_CHARS:
        return text[:_PREDICATE_MAX_CHARS - 1] + "…"
    return text


def _descriptor_str(desc: Any, *keys: str) -> Optional[str]:
    if not isinstance(desc, dict):
        return None
    for key in keys:
        val = desc.get(key)
        if isinstance(val, str) and val.strip():
            return _truncate(val)
    return None


def _base_op(node: dict) -> str:
    name = node.get("name") or node.get("operator") or node.get("type") or ""
    if not isinstance(name, str):
        name = str(name)
    return name.split("[", 1)[0].strip() or "?"


def _extract_table_normalized(desc: Any) -> Optional[str]:
    """catalog.schema.table from a descriptor, covering live-Trino spellings.

    Live Trino emits e.g. "iceberg:test_schema.t$data@<snapshot>" in the
    descriptor's ``table`` field — connector prefix with ':' and an internal
    ``$data@…`` suffix. Normalize those before falling back to the
    plan_signature regex used for fixture-style descriptors.
    """
    if isinstance(desc, dict):
        raw = desc.get("table")
        if isinstance(raw, str) and raw.strip():
            cleaned = raw.split("$", 1)[0].split("@", 1)[0].replace(":", ".")
            m = _TABLE_RE.search(cleaned)
            if m:
                return m.group(1).lower()
    return _extract_table(desc)


def _node_line(node: dict) -> str:
    name = node.get("name") if isinstance(node.get("name"), str) else ""
    base = _base_op(node)
    desc = node.get("descriptor") or node.get("details") or node.get("operatorAttributes")

    bracket_items = _bracket_keywords(name or "")
    table = _extract_table_normalized(desc)
    if table and table not in bracket_items:
        bracket_items.append(table)
    if isinstance(desc, dict):
        dist = desc.get("distributionType") or desc.get("distribution")
        if isinstance(dist, str) and dist.strip():
            up = dist.strip().upper()
            if up not in bracket_items:
                bracket_items.append(up)
        if base == "Aggregate":
            phase = desc.get("type")
            if isinstance(phase, str) and phase.strip():
                up = phase.strip().upper()
                if up not in bracket_items:
                    bracket_items.append(up)

    parts = [f"{base}[{', '.join(bracket_items)}]" if bracket_items else base]

    if base in _JOIN_OPS:
        criteria = _descriptor_str(desc, "criteria")
        if criteria:
            parts.append(f"on {criteria}")
    else:
        predicate = _descriptor_str(desc, "filterPredicate", "predicate")
        if predicate:
            parts.append(f"pred={predicate}")
    if base == "Aggregate":
        keys = _descriptor_str(desc, "keys")
        if keys:
            parts.append(f"keys={keys}")
        funcs = _descriptor_str(desc, "functions", "aggregations")
        if funcs:
            parts.append(f"funcs={funcs}")
    if base in {"RemoteSource", "RemoteMerge"}:
        sources = _descriptor_str(desc, "sourceFragmentIds")
        if sources:
            parts.append(f"from={sources}")

    rows, bytes_ = _node_estimates(node)
    est_bits = []
    if rows is not None:
        est_bits.append(f"{_fmt_count(rows)} rows")
    if bytes_ is not None:
        est_bits.append(_fmt_bytes(bytes_))
    if est_bits:
        parts.append(f"~{', '.join(est_bits)}")

    return " ".join(parts)


def _roots(plan: Union[dict, list]) -> list[tuple[Optional[str], Any]]:
    """Normalize the top-level shape into (label, root-node) pairs.

    Handles: a single plan tree, a list of trees, and the distributed-plan
    fragment map ({"0": {...}, "1": {...}}) some Trino versions emit.
    """
    if isinstance(plan, list):
        return [(None, p) for p in plan if p is not None]
    if isinstance(plan, dict):
        node_keys = ("name", "operator", "type", "children", "inputs")
        if not any(k in plan for k in node_keys):
            fragments = [(k, v) for k, v in plan.items() if isinstance(v, dict)]
            if fragments:
                # Fragment ids are numeric strings — sort numerically so
                # Fragment 10 doesn't render between Fragment 1 and 2.
                def _frag_key(k) -> tuple:
                    s = str(k)
                    return (0, int(s), "") if s.isdigit() else (1, 0, s)
                return [(f"Fragment {k}", v) for k, v in sorted(fragments, key=lambda kv: _frag_key(kv[0]))]
            return []   # no node keys, no fragments — nothing renderable
        return [(None, plan)]
    return []


def _collect(node: Any, depth: int, out: list[tuple[int, str, bool]]) -> None:
    """Depth-first walk → (depth, line, is_high_signal) triples."""
    if depth > _MAX_PLAN_DEPTH or not isinstance(node, dict):
        return
    base = _base_op(node)
    out.append((depth, _node_line(node), base in _HIGH_SIGNAL_OPS))
    children = node.get("children") or node.get("inputs") or []
    if isinstance(children, dict):
        children = [children]
    if not isinstance(children, list):
        return
    for child in children:
        if child is not None:
            _collect(child, depth + 1, out)


def render_plan_skeleton(
    plan: Union[str, dict, list, None],
    *,
    max_lines: int = DEFAULT_MAX_LINES,
) -> str:
    """Render an EXPLAIN (FORMAT JSON) plan as a bounded text skeleton.

    Returns "" when the input is unusable (None, parse error, or a shape with
    no recognizable plan nodes) so callers can gate injection on truthiness.
    Output never exceeds ``max_lines`` lines.
    """
    if plan is None:
        return ""
    if isinstance(plan, str):
        try:
            plan = json.loads(plan)
        except (json.JSONDecodeError, TypeError):
            return ""
    if not isinstance(plan, (dict, list)):
        return ""

    max_lines = max(3, int(max_lines))

    entries: list[tuple[int, str, bool]] = []
    try:
        for label, root in _roots(plan):
            if label is not None:
                entries.append((0, label, True))
                _collect(root, 1, entries)
            else:
                _collect(root, 0, entries)
    except Exception:
        return ""
    if not entries:
        return ""

    lines = ["  " * depth + line for depth, line, _ in entries]

    if len(lines) > max_lines:
        # Compact mode: decision nodes only, re-indented by kept-ancestor depth.
        folded = sum(1 for _, _, high in entries if not high)
        kept: list[str] = []
        depth_map: dict[int, int] = {}   # original depth → compact depth
        for depth, line, high in entries:
            if not high:
                continue
            parent_depths = [d for d in depth_map if d < depth]
            compact_depth = (depth_map[max(parent_depths)] + 1) if parent_depths else 0
            depth_map = {d: c for d, c in depth_map.items() if d < depth}
            depth_map[depth] = compact_depth
            kept.append("  " * compact_depth + line)
        lines = kept
        if folded:
            lines.append(f"(plan condensed: {folded} low-signal node(s) folded)")

    if len(lines) > max_lines:
        omitted = len(lines) - (max_lines - 1)
        lines = lines[:max_lines - 1]
        lines.append(f"… (+{omitted} more plan node(s) omitted)")

    return "\n".join(lines)


def repeated_subtree_note(
    plan: Union[str, dict, list, None],
    *,
    min_nodes: int = 3,
) -> str:
    """One-line note when the optimizer planned identical subtrees repeatedly.

    Repeated subplans are the signature of an inlined CTE (Trino inlines WITH
    relations where referenced) — the strongest single "step materialization"
    signal a plan can carry, and one the skeleton's per-node lines don't
    surface because the copies render far apart. Returns "" when there is no
    repetition (or unusable input) so callers gate injection on truthiness.
    """
    from .plan_signature import _node_sig

    if plan is None:
        return ""
    if isinstance(plan, str):
        try:
            plan = json.loads(plan)
        except (json.JSONDecodeError, TypeError):
            return ""
    if not isinstance(plan, (dict, list)):
        return ""

    def _count_nodes(node: dict) -> int:
        children = node.get("children") or node.get("inputs") or []
        if isinstance(children, dict):
            children = [children]
        if not isinstance(children, list):
            children = []
        return 1 + sum(_count_nodes(c) for c in children if isinstance(c, dict))

    counts: dict[Any, tuple[int, str]] = {}

    def _tally(node: Any, depth: int) -> None:
        if depth > _MAX_PLAN_DEPTH or not isinstance(node, dict):
            return
        if _count_nodes(node) >= min_nodes:
            try:
                sig = _node_sig(node)
            except Exception:
                sig = None
            if sig is not None:
                n, label = counts.get(sig, (0, ""))
                counts[sig] = (n + 1, label or _node_line(node))
        children = node.get("children") or node.get("inputs") or []
        if isinstance(children, dict):
            children = [children]
        if isinstance(children, list):
            for child in children:
                _tally(child, depth + 1)

    try:
        for _, root in _roots(plan):
            _tally(root, 0)
    except Exception:
        return ""

    repeated = sorted(
        ((n, label) for n, label in counts.values() if n > 1), reverse=True
    )
    if not repeated:
        return ""
    tops = "; ".join(f"×{n} {label}" for n, label in repeated[:3])
    return (
        f"! {len(repeated)} identical subtree(s) planned multiple times — "
        f"likely inlined CTE re-planned per reference: {tops}"
    )


__all__ = ["render_plan_skeleton", "repeated_subtree_note", "DEFAULT_MAX_LINES"]
