#!/usr/bin/env python3
"""Build one-page HTML doc site from genieCLI docs/ markdown set."""
import html as H
import re
import pathlib

DOCS = pathlib.Path(__file__).resolve().parent.parent / "docs"
OUT = pathlib.Path(__file__).resolve().parent.parent / "docs" / "handbook.html"

# (file, anchor, nav label, group, badge)
PLAN = [
    ("overview.md", "overview", "導讀與詞彙", "導讀", "overview"),
    ("user-stories.md", "user-stories", "User Stories", "產品視角", "use-case"),
    ("use-cases.md", "use-cases", "功能入口", "產品視角", "use-case"),
    ("architecture.md", "architecture", "系統架構", "系統視角", "architecture"),
    ("class-diagram.md", "class-diagram", "核心類別圖", "系統視角", "class"),
    ("flows/trino-research-direct.md", "flow-direct", "研究管線 --direct", "核心流程", "sequence"),
    ("flows/trino-research-mcp.md", "flow-mcp", "研究管線 MCP", "核心流程", "sequence"),
    ("flows/write-analysis.md", "flow-write", "寫入型離線分析", "核心流程", "sequence"),
    ("modules/core.md", "mod-core", "genie/core 合約", "模組合約", "class"),
    ("modules/runtime.md", "mod-runtime", "autoresearch 引擎", "模組合約", "sequence"),
    ("db-schema.md", "db-schema", "e2e 測試 Schema", "資料", "db-schema"),
]

LINKMAP = {p: a for p, a, *_ in PLAN}

CITE = re.compile(r"（([\w/.\-]+\.(?:py|sql|md)):([\d\-、,]+)）")


def inline(s: str) -> str:
    s = H.escape(s, quote=False)
    s = re.sub(r"`([^`]+)`", r"<code>\1</code>", s)
    s = re.sub(r"\*\*([^*]+)\*\*", r"<strong>\1</strong>", s)
    s = CITE.sub(lambda m: f'<code class="cite">{m.group(1)}:{m.group(2)}</code>', s)
    # cross-doc references like flows/trino-research-direct.md → anchor link
    for path, anchor in LINKMAP.items():
        s = s.replace(path, f'<a href="#{anchor}">{path}</a>')
    return s


def convert(md: str) -> str:
    lines = md.splitlines()
    out, i, n = [], 0, len(lines)
    para: list[str] = []

    def flush():
        if para:
            out.append(f"<p>{inline(' '.join(para))}</p>")
            para.clear()

    while i < n:
        line = lines[i]
        if line.startswith("```"):
            flush()
            lang = line[3:].strip()
            block = []
            i += 1
            while i < n and not lines[i].startswith("```"):
                block.append(lines[i])
                i += 1
            i += 1
            body = H.escape("\n".join(block), quote=False)
            if lang == "mermaid":
                out.append(f'<div class="diagram"><pre class="mermaid">{body}</pre></div>')
            else:
                out.append(f'<div class="codewrap"><pre><code>{body}</code></pre></div>')
            continue
        if line.startswith("|") and i + 1 < n and re.match(r"^\|[\s:|-]+\|$", lines[i + 1] or ""):
            flush()
            header = [c.strip() for c in line.strip("|").split("|")]
            i += 2
            rows = []
            while i < n and lines[i].startswith("|"):
                rows.append([c.strip() for c in lines[i].strip("|").split("|")])
                i += 1
            t = ["<div class='tablewrap'><table><thead><tr>"]
            t += [f"<th>{inline(c)}</th>" for c in header]
            t.append("</tr></thead><tbody>")
            for r in rows:
                t.append("<tr>" + "".join(f"<td>{inline(c)}</td>" for c in r) + "</tr>")
            t.append("</tbody></table></div>")
            out.append("".join(t))
            continue
        m = re.match(r"^(#{1,4})\s+(.*)$", line)
        if m:
            flush()
            lvl = len(m.group(1))
            if lvl == 1:
                i += 1
                continue  # doc title handled by section header
            out.append(f"<h{lvl+1}>{inline(m.group(2))}</h{lvl+1}>")
            i += 1
            continue
        m = re.match(r"^(\d+)\.\s+(.*)$", line)
        if m:
            flush()
            items = []
            start = int(m.group(1))
            while i < n and (mm := re.match(r"^(\d+)\.\s+(.*)$", lines[i])):
                items.append(f"<li>{inline(mm.group(2))}</li>")
                i += 1
            out.append(f"<ol class='rules' start='{start}'>" + "".join(items) + "</ol>")
            continue
        if re.match(r"^[-*]\s+", line):
            flush()
            items = []
            while i < n and re.match(r"^[-*]\s+", lines[i]):
                items.append(f"<li>{inline(re.sub(r'^[-*]\\s+', '', lines[i]))}</li>")
                i += 1
            out.append("<ul>" + "".join(items) + "</ul>")
            continue
        if not line.strip():
            flush()
            i += 1
            continue
        para.append(line.strip())
        i += 1
    flush()
    return "\n".join(out)


sections, nav_groups, cur = [], [], None
for path, anchor, label, group, badge in PLAN:
    md = (DOCS / path).read_text()
    title = re.match(r"^#\s+(.*)$", md.splitlines()[0]).group(1)
    body = convert(md)
    sections.append(
        f'<section id="{anchor}">'
        f'<header class="sec-head"><span class="badge b-{badge}">{badge}</span>'
        f"<h2>{H.escape(title)}</h2>"
        f'<code class="src">docs/{path}</code></header>'
        f"{body}</section>"
    )
    if cur is None or cur[0] != group:
        cur = (group, [])
        nav_groups.append(cur)
    cur[1].append((anchor, label))

nav = []
for group, items in nav_groups:
    nav.append(f'<div class="nav-group"><div class="nav-label">{group}</div>')
    for anchor, label in items:
        nav.append(f'<a href="#{anchor}">{label}</a>')
    nav.append("</div>")

page = f"""<!doctype html>
<html lang="zh-Hant">
<head>
<meta charset="utf-8">
<title>GenieCLI Handbook</title>
<meta name="viewport" content="width=device-width, initial-scale=1">
<style>
:root {{
  --paper:#F6F8F9; --panel:#FFFFFF; --ink:#1C2733; --muted:#5C6B78;
  --accent:#0E7C86; --accent-ink:#0A5E66; --line:#DCE4E9; --code-bg:#ECF1F4;
  --cite-bg:#E2F0F1; --badge-ink:#FFFFFF; --shadow:0 1px 2px rgba(28,39,51,.06);
}}
:root:not([data-theme="light"]) {{}}
@media (prefers-color-scheme: dark) {{
  :root:not([data-theme="light"]) {{
    --paper:#12161C; --panel:#181E26; --ink:#D9E0E7; --muted:#8A97A3;
    --accent:#4FC3CE; --accent-ink:#7BD4DD; --line:#2A333D; --code-bg:#1F2731;
    --cite-bg:#173238; --badge-ink:#0E1418; --shadow:0 1px 2px rgba(0,0,0,.4);
  }}
}}
:root[data-theme="dark"] {{
  --paper:#12161C; --panel:#181E26; --ink:#D9E0E7; --muted:#8A97A3;
  --accent:#4FC3CE; --accent-ink:#7BD4DD; --line:#2A333D; --code-bg:#1F2731;
  --cite-bg:#173238; --badge-ink:#0E1418; --shadow:0 1px 2px rgba(0,0,0,.4);
}}
* {{ box-sizing:border-box; }}
body {{
  margin:0; background:var(--paper); color:var(--ink);
  font-family:"Noto Sans TC","PingFang TC","Microsoft JhengHei",system-ui,sans-serif;
  font-size:16px; line-height:1.75;
}}
code, pre, .src {{ font-family:ui-monospace,"Cascadia Code",Consolas,Menlo,monospace; }}
.layout {{ display:flex; min-height:100vh; }}
nav {{
  width:248px; flex:none; border-right:1px solid var(--line);
  padding:28px 20px 40px; position:sticky; top:0; height:100vh; overflow-y:auto;
  background:var(--panel);
}}
.brand {{ margin-bottom:6px; }}
.brand .g {{ color:var(--accent); font-weight:700; letter-spacing:.02em; font-size:19px; }}
.brand .sub {{ color:var(--muted); font-size:12.5px; margin-top:2px; line-height:1.5; }}
.meta {{ font-size:11.5px; color:var(--muted); border-top:1px solid var(--line);
  margin-top:10px; padding-top:10px; line-height:1.7; }}
.meta code {{ font-size:10.5px; background:var(--code-bg); padding:1px 4px; border-radius:3px; }}
.nav-group {{ margin-top:18px; display:flex; flex-direction:column; gap:2px; }}
.nav-label {{ font-size:11px; letter-spacing:.14em; color:var(--muted);
  text-transform:uppercase; margin-bottom:4px; }}
nav a {{ color:var(--ink); text-decoration:none; font-size:14px; padding:4px 8px;
  border-radius:5px; border-left:2px solid transparent; }}
nav a:hover {{ background:var(--code-bg); }}
nav a:focus-visible, main a:focus-visible {{ outline:2px solid var(--accent); outline-offset:2px; }}
main {{ flex:1; min-width:0; padding:44px clamp(20px,5vw,72px) 96px; }}
main .inner {{ max-width:78ch; }}
section {{ margin-bottom:72px; scroll-margin-top:24px; }}
.sec-head {{ display:flex; align-items:baseline; gap:12px; flex-wrap:wrap;
  border-bottom:2px solid var(--line); padding-bottom:10px; margin-bottom:18px; }}
.sec-head h2 {{ margin:0; font-size:26px; line-height:1.3; text-wrap:balance; }}
.src {{ color:var(--muted); font-size:11.5px; margin-left:auto; }}
.badge {{ font-size:10.5px; letter-spacing:.1em; text-transform:uppercase;
  padding:2px 8px; border-radius:99px; background:var(--accent); color:var(--badge-ink);
  font-weight:600; flex:none; position:relative; top:-3px; }}
h3 {{ font-size:19px; margin:34px 0 10px; }}
h4 {{ font-size:16px; margin:26px 0 8px; }}
h3::before {{ content:"§ "; color:var(--accent); }}
p {{ margin:0 0 14px; }}
strong {{ font-weight:700; }}
code {{ background:var(--code-bg); padding:1.5px 5px; border-radius:4px; font-size:13.2px; }}
code.cite {{ background:var(--cite-bg); color:var(--accent-ink); font-size:12px; white-space:nowrap; }}
a {{ color:var(--accent-ink); }}
ol.rules {{ padding-left:0; margin:0 0 16px; list-style:none; counter-reset:rule; }}
ol.rules[start="2"] {{ counter-reset:rule 1; }}
ol.rules li {{ counter-increment:rule; position:relative; padding:8px 12px 8px 46px;
  border-left:3px solid var(--line); margin-bottom:8px; background:var(--panel);
  border-radius:0 6px 6px 0; box-shadow:var(--shadow); }}
ol.rules li::before {{ content:counter(rule); position:absolute; left:12px; top:9px;
  color:var(--accent); font-weight:700; font-variant-numeric:tabular-nums; font-size:14px; }}
ul {{ padding-left:22px; margin:0 0 14px; }}
ul li {{ margin-bottom:6px; }}
.diagram {{ overflow-x:auto; background:var(--panel); border:1px solid var(--line);
  border-radius:8px; padding:18px 16px; margin:0 0 18px; }}
.diagram pre.mermaid {{ margin:0; background:transparent; display:flex; justify-content:center; }}
.codewrap {{ overflow-x:auto; margin:0 0 16px; }}
.codewrap pre {{ margin:0; background:var(--code-bg); border:1px solid var(--line);
  border-radius:8px; padding:14px 16px; font-size:13px; line-height:1.6; }}
.codewrap code {{ background:transparent; padding:0; }}
.tablewrap {{ overflow-x:auto; margin:0 0 18px; border:1px solid var(--line); border-radius:8px; }}
table {{ border-collapse:collapse; width:100%; font-size:14px; background:var(--panel); }}
th {{ text-align:left; font-size:12px; letter-spacing:.06em; text-transform:uppercase;
  color:var(--muted); border-bottom:2px solid var(--line); padding:9px 14px; white-space:nowrap; }}
td {{ border-bottom:1px solid var(--line); padding:9px 14px; vertical-align:top; }}
tr:last-child td {{ border-bottom:none; }}
@media (max-width: 880px) {{
  .layout {{ flex-direction:column; }}
  nav {{ width:100%; height:auto; position:static; border-right:none;
    border-bottom:1px solid var(--line); }}
  .src {{ margin-left:0; }}
}}
@media (prefers-reduced-motion: no-preference) {{
  html {{ scroll-behavior:smooth; }}
}}
</style>
</head>
<body>
<div class="layout">
<nav>
  <div class="brand"><div class="g">GenieCLI Handbook</div>
  <div class="sub">AI 輔助 Trino SQL 調校 CLI 的一站式技術文件</div></div>
  {''.join(nav)}
  <div class="meta">由 doc-align 對齊維護<br>對齊基準 <code>d086db8</code><br>文件 commit <code>d7bcb0f</code> · 2026-08-14</div>
</nav>
<main><div class="inner">
{''.join(sections)}
</div></main>
</div>
<script type="module">
  const dark = document.documentElement.dataset.theme === "dark" ||
    (document.documentElement.dataset.theme !== "light" &&
     window.matchMedia("(prefers-color-scheme: dark)").matches);
  try {{
    const {{ default: mermaid }} = await import("https://cdn.jsdelivr.net/npm/mermaid@11/dist/mermaid.esm.min.mjs");
    mermaid.initialize({{ startOnLoad: true, theme: dark ? "dark" : "default" }});
  }} catch (e) {{
    document.querySelectorAll("pre.mermaid").forEach((el) => {{
      el.style.whiteSpace = "pre";
      el.insertAdjacentHTML("beforebegin",
        "<p style='color:var(--muted);font-size:12.5px;margin:0 0 6px'>（離線模式：Mermaid 圖以原始碼顯示；連網後重新整理即可渲染）</p>");
    }});
  }}
</script>
</body>
</html>
"""
OUT.write_text(page)
print(f"wrote {OUT} ({len(page)//1024} KB), sections={len(sections)}")
