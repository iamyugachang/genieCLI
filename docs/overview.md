# GenieCLI 文件導讀

## 系統目的

GenieCLI 是以 LLM 輔助調校 Trino SQL 的命令列工具：先整理 SQL 結構、EXPLAIN 與可取得的執行證據，再請模型提出候選改寫，並以實測數據（而非模型自評）決定是否採納；同時支援 Oracle→Trino 遷移輔助。三個安全承諾貫穿設計：先檢查再執行（read-only preflight）、候選必須驗證（失敗候選不取代基準 SQL）、路徑不偷換（MCP 不可達即明確失敗）。

## Domain 詞彙表

| 詞彙 | 一行定義 |
|---|---|
| direct 路徑 | `--direct`：以 trino Python driver 直連叢集執行研究管線 |
| MCP 路徑 | 預設路徑：SQL 執行經 MCP server（JSON-RPC over HTTP）轉發到 Trino |
| baseline | 原始 SQL 的實測基準（median of N runs），所有候選與之比較 |
| candidate | LLM 提出的一次改寫；須通過等價閘門與指標改善才被採納 |
| metric | 優化目標指標（cpu_time_ms、wall_time_ms、physical_input_bytes…），越低越好 |
| preflight | 執行前安全檢查：read-only 驗證＋EXPLAIN 可行性，拒絕即中止 |
| preflight decision | baseline 量測後的路由決策：diagnose-only／no-data／failure／long-query／plan-cost／standard |
| long-query gate | baseline 過慢時的閘門；`--no-long-query` 下改出 directed report 不迭代 |
| plan-cost loop | EXPLAIN 成本估計可用時的替代迴圈（以計畫成本篩選候選） |
| stepwise A/B/C | 迭代內三步：Step A 診斷 → Step B 選策略 → Step C 改寫（含證據注入） |
| seed decompose | v48：迭代前先嘗試拆解重組原始 SQL 選出更好的起點 |
| directions | 零成本診斷產出的排序建議（OptimizationDirection），餵進 LLM prompt |
| rule gate | 靜態規則與 directions 的彙總摘要，同樣注入 prompt |
| write-analysis | 寫入型 SQL 的離線 advisory 分析：不執行、不驗證、只建議 |
| safe-limit | `--safe-limit n`：外層包 LIMIT n（會改變語意，需明示） |
| L1／L2／L3 | 候選證據三層：靜態／計畫結構／執行等價，彙總為 ship_status |
| profile | Trino 直連連線設定（`/trino use|add|remove`），存 `~/.config/genie/trino.json` |
| provider | LLM 後端抽象：tgenie（內部）／openai 相容／anthropic |
| skill | 註冊給模型的工具（BaseSkill），tier 決定載入的模型等級 |
| session | REPL 對話存檔（JSON，`sessions/`），支援 undo／redo／branch／compact |
| autoresearch | `/autoresearch`：LLM 自主迭代改檔案，git checkpoint 保護、指標裁決 |
| checkpoint | autoresearch 每迭代的 git commit 快照；失敗 `reset --hard` 回 original_head |
| guard | autoresearch 選配的守門命令（如 linter）；非零退出即整步還原 |
| journal | autoresearch 的 TSV 逐迭代紀錄檔 |
| ContextManager | 對話歷史修剪器：70% 觸發、保留 system＋最後 4 則 |

## 文件導讀順序

1. **overview.md**（本文件）— 系統目的與詞彙。
2. **user-stories.md** — 產品行為的功能覆蓋：11 則故事＋驗收要點（PM／產品視角從這裡）。
3. **architecture.md** — 模組分層、資料流、外部依賴；找「要改哪個 component」從這裡開始。
4. **use-cases.md** — 所有功能入口與觸發方式。
5. **flows/trino-research-direct.md** — 核心研究管線全流程（路由狀態圖、迭代、報告）。
6. **flows/trino-research-mcp.md** — MCP 路徑差異：連線層、可達性、證據覆蓋。
7. **flows/write-analysis.md** — 寫入型 SQL 的離線分析與安全契約。
8. **class-diagram.md** — 可抽換點：Provider／BaseSkill／McpClient／Sink。
9. **modules/core.md** — 共用基礎層的合約與不變量（registry／config／context 修剪）。
10. **modules/runtime.md** — autoresearch 迭代引擎（checkpoint／guard／journal）。
11. **db-schema.md** — e2e 測試環境的種子資料表。
