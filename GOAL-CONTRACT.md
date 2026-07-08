# Servitor-Workflows Follow-up Plan / Goal Contract

Last updated: 2026-07-09 CST after P1.5 (codex provider) + P3 (compare_runs/supervise tests) + providers.py refactor into providers/ package.

## Purpose

`servitor-workflows` is the Python dynamic-workflow layer on top of `servitor` transport. The first port is implemented; the next phase is hardening, not feature sprawl.

Primary objective for the next agent: make real provider execution boring and observable. 展示层/HTML viewer 暂不重要；真实 provider 端到端跑通、状态可信、agent-dispatch 路由变薄才重要。

## Current verified state

- Package path: `D:\AgentWork\tools\servitor-workflows`.
- `pip install -e .` installed for both `servitor` and `servitor-workflows`; no PYTHONPATH needed.
- `python -m pytest -q` in `D:\AgentWork\tools\servitor-workflows` passed on 2026-07-08: `53 passed in 0.88s`.
- `servitor-workflows --help` (CLI on PATH) exposes: `run`, `summarize`, `map`, `status`, `compare`, `supervise`.
- `servitor agents list --json` shows captured providers: `claude`, `codebuddy`, `agy-tui`, `pi`. The retired launch-only `agy` provider has been fully removed from `providers.py` (2026-07-09).
- Real provider smoke verified:
  - `pi` single-provider: run_dir `20260708T083147570427Z`, `provider=pi, ok=true, result="pong"`.
  - `codebuddy` + `pi` multi-provider: journal `multi_provider_pong.workflow.result.json` = `{"codebuddy": "codebuddy", "pi": "pi"}`.
  - Journal replay with `--resume` uses cached results.
- `servitor-workflows run` resolves default provider: explicit `--agent` > workflow meta > captured preference `pi` > `codebuddy` > `claude` > `agy-tui`. Chosen provider recorded in `.meta.json`, `.events.jsonl`, journal entries.
- `servitor advisor` subcommand added (2026-07-09): controller-side advisor with `--check` audit, call counter in `~/.servitor/advisor_state.json`, `advisor` role preset.
- `agent-dispatch` SKILL.md slimmed from 238 to 68 lines (2026-07-09); transport/provider details in `references/servitor-transport-details.md`.
- User model preference: GLM-5.2 (`newapi/glm-5.2`) executor, GPT-5.5 (`newapi/gpt-5.5`) advisor. Claude channels closed.
- `compare_runs.py` and `supervise.py` exist and are wired into CLI, but do not yet have dedicated tests (P3).
- All 5 tools pushed to private GitHub repos under `L1uYun/`.

- `servitor run --agent codex` registered (2026-07-09): uses `codex exec --dangerously-bypass-approvals-and-sandbox --output-last-message <file>`. Real smoke returned `pong` (run_dir `20260708T204106868146Z`).
- `providers.py` refactored into `providers/` package (2026-07-09): base, extractors, model_discovery, utility, agy_tui, execution, registrations. Adding a new provider now requires only a registration call + optional model discovery function.
- P3 tests added (2026-07-09): `test_compare_runs.py` (8 tests) + `test_supervise.py` (4 tests). Total: 65 passed.

## User decisions to preserve

- Stay in the Python ecosystem. Do not switch this port to TypeScript.
- Do not fully rewrite the system; keep the 1:1 upstream concept, translated into Python and adapted to local `servitor`.
- Tutti's useful core here is shared workspace state/status, not a GUI. We are already in `D:\AgentWork`; use run dirs, journals, sidecars, and local state files before inventing a display layer.
- `SKILL.md` should be a routing decision table, not an implementation manual. Thick transport details belong in `references/`.
- `hy3-preview` free-window note is still current as of 2026-07-08; do not delete it as expired until after 2026-07-21 or fresh evidence says otherwise.
- User preference added 2026-07-08: do not recommend `codebuddy` by default. Prefer `pi`; if `codebuddy` is used, prefer third-party relay models from `~/.codebuddy/models.json` over CodeBuddy built-in models.
- HTML viewer / `view-run.js` parity is deferred. ASCII map and text status are enough for now.

## Write boundaries

Allowed for the next slices:

- `D:\AgentWork\tools\servitor-workflows\**`
- `D:\AgentWork\tools\servitor\servitor\providers.py` and matching tests/docs only for adding the `codex` provider or narrow provider-surface fixes.
- `C:\Users\84618\.codex\skills\agent-dispatch\SKILL.md` and `C:\Users\84618\.codex\skills\agent-dispatch\references\**` for route-table slimming.
- `D:\AgentWork\.gitignore` only after a dry-run proves the needed `tools/servitor-workflows` source files can be exposed without generated journals/caches/logs.

Forbidden unless the user explicitly expands scope:

- New daemon, queue service, scheduler, web server, long-running supervisor, or GUI.
- New provider credentials, NewAPI routing changes, ccSwitch/CodexPlusPlus config edits, or unrelated skill rewrites.
- Pushing orchestration logic into `servitor`; `servitor` remains transport-only.
- Treating a shell timeout as proof that an agent is dead without reading provider metadata/result state.

## Priority ledger

### P0 — Real provider execution confidence — COMPLETE

Goal: prove real workflows can run, be replayed, and expose true state without guessing.

Verified 2026-07-08: 53 passed, pi single-provider pong, multi-provider codebuddy+pi, journal replay cached.

Tasks:

1. Baseline checks.
   - Run from `D:\AgentWork\tools\servitor-workflows`.
   - Set `PYTHONPATH=D:\AgentWork\tools\servitor-workflows;D:\AgentWork\tools\servitor` if editable installs are not active.
   - Verify `python -m pytest -q`.
   - Verify `python -m servitor_workflows --help`.
   - Verify `python -m servitor agents list --json`.

2. Single-provider real smoke.
   - Use `codebuddy` first because prior evidence shows it returned `pong` in about 14-15s.
   - Command shape:
     ```powershell
     cd D:\AgentWork\tools\servitor-workflows
     $env:PYTHONPATH = "D:\AgentWork\tools\servitor-workflows;D:\AgentWork\tools\servitor"
     python -m servitor_workflows run examples/hello_smoke.workflow.py --agent codebuddy --fresh --json
     ```
   - Acceptance: command exits 0, result contains `pong`, journal/event/result sidecars are written, and the final report names the exact journal path.

3. Journal replay smoke.
   - Re-run the same workflow with `--resume`.
   - Acceptance: result comes from the journal cache; no new real provider call is needed for the cached agent step. Verify via events/journal timestamps or servitor run-dir count, not by intuition.

4. Multi-agent real smoke.
   - Create the smallest scratch workflow under `D:\AgentWork\_tmp\workflow-port\real-provider-smoke\` unless a durable example is clearly justified.
   - Use two captured providers when available, preferably `codebuddy` + `pi`; fall back to `codebuddy` + `agy-tui` only if `pi` is unavailable; use `claude` only with a longer timeout and after checking the previous 120s shell timeout was not a provider failure.
   - Acceptance: at least two real agent invocations complete, or one completes and the other has a provider-level failure reason backed by `metadata.json` / `servitor result`, not a controller guess.

5. Timeout/state semantics.
   - For every slow or failed real run, capture `run_dir`, `failure_reason`, `exit_code`, `stderr.txt`, `metadata.json`, and `servitor result <run_dir> --json`.
   - Distinguish:
     - shell command timeout: wrapper stopped waiting;
     - `wait_elapsed`: servitor wait window elapsed;
     - `stalled`: workflow/session has no observed activity past threshold;
     - provider failure: provider metadata says failed;
     - still running: provider process/run dir has not completed.
   - Acceptance: no final report says "agent timed out/dead" unless provider evidence supports that claim.

6. Shared state / Tutti-core hardening.
   - Use existing `.workflow-journal/*.events.jsonl`, `.meta.json`, `.result.json`, servitor run dirs, and `agent.status()` / `agent.stalled()` as the source of truth.
   - Add only the smallest missing state field if a real run proves another agent cannot infer `working`, `idle`, `stalled`, `failed`, or `completed`.
   - Do not build `view`/HTML. Text/JSON status is enough.

### P0.5 — Durability and Git provenance — COMPLETE

Goal: make sure the project can be carried forward safely.

Current evidence: root Git currently ignores `tools/servitor-workflows/` because `D:\AgentWork\.gitignore` allows only selected paths under `tools/`.

Tasks:

1. Run:
   ```powershell
   git -C D:\AgentWork status --short --ignored -- tools/servitor-workflows
   git -C D:\AgentWork check-ignore -v tools/servitor-workflows/GOAL-CONTRACT.md tools/servitor-workflows/README.md
   ```
2. If the user wants this package tracked, update `.gitignore` narrowly:
   - expose source, tests, examples, README, pyproject, and this plan;
   - keep `.workflow-journal/`, `.pytest_cache/`, `__pycache__/`, provider run logs, and scratch outputs ignored.
3. Before staging, run `git add --dry-run` and `git status --ignored` to catch generated artifacts.

Acceptance: future agent can tell which changed files are source/docs/tests versus generated runtime state.

### P1 — Slim `agent-dispatch` skill into a route table — COMPLETE

Goal: reduce model cognitive entropy. The skill should route decisions; implementation details should move to references.

Current evidence: `C:\Users\84618\.codex\skills\agent-dispatch\SKILL.md` is about 21KB / 238 lines and mixes routing, provider details, failure taxonomy, long examples, and temporary model notes.

Tasks:

1. Rewrite in place, not as an additive append.
2. Keep `SKILL.md` to:
   - route selection: Direct vs `/plan-execute-audit` vs `servitor run` vs `servitor batch` vs `servitor-workflows`;
   - dispatch heuristic;
   - model-tier policy without long hardcoded model lists;
   - worker contract;
   - failure-handling decision points;
   - references routing.
3. Move details into `references/servitor-transport-details.md` or sharpen an existing reference if one already owns it:
   - command examples;
   - provider-specific prompt transport;
   - JSON/schema/check behavior;
   - failure taxonomy details;
   - CodeBuddy/pi/agy notes;
   - temporary `hy3-preview` note with date boundary.
4. Preserve the correction: `hy3-preview` is not expired on 2026-07-08; note reassess after 2026-07-21.
5. Acceptance:
   - `SKILL.md` target size about 6-8KB;
   - route decisions are readable in the first screen;
   - no loss of essential operational details because references carry them;
   - `augury doctor` still passes after edit.

### P1.5 — Add Codex as a `servitor` subagent / 灵体 — COMPLETE

Goal: allow `servitor run --agent codex ...` so Codex itself can be invoked as a bounded local worker when appropriate.

Reference to inspect before implementation: `https://github.com/steipete/agent-scripts/blob/main/skills/codex-first/SKILL.md`.

Tasks:

1. Inspect the local Codex CLI first:
   ```powershell
   codex --help
   codex -p --help
   ```
2. Register the smallest `ProviderSpec` in `D:\AgentWork\tools\servitor\servitor\providers.py`.
   - Expected shape to verify, not assume: `codex -p --output-format json`.
   - Prefer stdin or prompt-file transport for long prompts.
   - Do not edit Codex auth/config.
3. Add tests for registration and result extraction using fake/subprocess-controlled output.
4. Run a real smoke only after tests:
   ```powershell
   python -m servitor run --agent codex --prompt "reply with exactly pong" --cwd D:\AgentWork --wait 120 --json
   ```
5. Update `servitor` docs and `agent-dispatch` route table only if the provider works.

Acceptance: `codex` appears in `servitor agents list --json`, a real bounded prompt can complete, and failures produce the same `failure_reason` contract as other captured providers.

### P2 — Simplify workflow parameters — implemented 2026-07-08

Goal: users and future agents should not guess provider/model/effort knobs for every run.

Implemented behavior:

1. Deterministic default provider resolution for `servitor-workflows run`:
   - first use explicit `--agent`;
   - else use workflow/meta default if present;
   - else choose a captured provider by stable policy: `pi`, then `codebuddy`, then `claude`, then `agy-tui`, then first captured provider.
2. The chosen provider is recorded in CLI meta sidecar, runtime default event, per-agent events, and journal metadata.
3. `--agent`, `--model`, `--pin-model`, `--effort`, `--pin-effort` remain overrides.
4. `README.md` and `agent-dispatch` references are updated to prefer `pi`.

Acceptance: this command works without requiring the caller to know provider details:

```powershell
python -m servitor_workflows run examples/hello_smoke.workflow.py --fresh --json
```

### P3 — Tests for existing lower-priority modules — COMPLETE

Goal: protect code already ported before adding more surface.

Tasks:

1. Add `tests/test_compare_runs.py` for `collect_comparison()` and `render_comparison_text()` using fake journal/run dirs.
2. Add `tests/test_supervise.py` for bounded `max_rounds` behavior and text emission without a real infinite loop.
3. Keep tests fake/offline; no provider calls in unit tests.

Acceptance: `python -m pytest -q` passes and test count increases.

### P4 — HTML viewer deferred

Goal: do not spend current effort on display.

State:

- ASCII map and text fleet status already exist.
- HTML viewer is a later enhancement only after real provider confidence and route-table slimming.

Resume only when:

- real provider smoke/replay/multi-agent status is boring;
- `agent-dispatch` is slim;
- user explicitly wants a visual run viewer.

Potential future task:

- Assemble `viewer.py` from existing `viewer_assets/viewer.css` + `viewer.js` + run model JSON.
- Keep it static/offline; no web server.

## Suggested next execution order

1. P0 baseline + single-provider real smoke.
2. P0 journal replay + multi-agent real smoke.
3. P0 timeout/state semantics fixes if the smoke exposes false timeout/stall inference.
4. P1 slim `agent-dispatch` while preserving current provider facts.
5. P1.5 add `codex` provider, if still useful.
6. P3 tests for `compare_runs` / `supervise`.
7. P4 HTML viewer only on explicit request.

## Stop conditions

Stop and report before editing further if:

- a provider requires credential/auth/config changes;
- a command timeout occurs but provider state cannot be read;
- Codex provider behavior cannot be verified from local `codex --help`;
- `.gitignore` changes would expose caches, journals, credentials, or provider run logs;
- implementing a display layer becomes the only way to proceed, because display is currently out of scope.

## Resume prompt

```text
从 D:\AgentWork 开始，先读取 D:\AgentWork\AGENTS.md，再读取 D:\AgentWork\tools\servitor-workflows\GOAL-CONTRACT.md。按该文件的 P0→P1 顺序继续 servitor-workflows 后续工作：真实 provider 端到端跑通优先，展示层/HTML viewer 暂不做。

当前最小目标：完成 P0 baseline、codebuddy 单 provider 真跑、journal replay、多 provider 真跑或给出 provider 级失败证据；不要把 shell timeout 当成 agent 死亡。所有完成声明都要附命令、journal/run_dir、metadata/result 证据。

之后再做 agent-dispatch SKILL.md 瘦身：把 SKILL.md 保持为路由决策表，把 transport/provider 细节搬进 references；保留 hy3-preview 截至 2026-07-08 仍未过期、约到 2026-07-21 后再复核的事实。不要做 HTML viewer，除非我重新要求。
```
