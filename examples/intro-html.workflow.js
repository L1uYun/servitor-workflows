export const meta = {
  name: "servitor-intro-html",
  description: "Generate a self-contained newcomer HTML intro to servitor + servitor-workflows from their READMEs (parallel section assembly)",
  contract: "workflow",
  boundary: {
    // Repo-relative: this script lives in examples/, so ".." is the
    // servitor-workflows repo and "../.." is tools/ (covers the servitor README).
    // Agent calls default their cwd to the script's parent (examples/), which
    // must fall inside readPaths or the boundary audit rejects every call.
    readPaths: ["../.."],
    writePaths: ["../docs"],
    network: "allow",
    environment: { allow: ["PART", "HEAD", "FOOT", "BODY"] },
  },
  capabilities: {
    providers: [
      { agent: "pi", model: "newapi/glm-5.2", capabilities: ["reasoning"], maxEffort: "high", contextTokens: 400000 },
    ],
    roles: {
      maker: { requires: ["reasoning"], effort: "high", contextTokens: 200000 },
    },
  },
};

// ---------------------------------------------------------------------------
phase("gather");
// Read both READMEs once; the text is passed verbatim to every section writer so
// no section may invent a fact the READMEs do not contain. Paths are relative to
// the script's parent dir (examples/), which is the default command cwd.
const readmes = await command("pwsh", [
  "-NoProfile", "-Command",
  "$ErrorActionPreference='Stop';" +
  "$a=Get-Content -LiteralPath '../../servitor/README.md' -Raw -Encoding utf8;" +
  "$b=Get-Content -LiteralPath '../README.md' -Raw -Encoding utf8;" +
  "Write-Output '===== SERVITOR README =====';" +
  "Write-Output $a;" +
  "Write-Output '===== SERVITOR-WORKFLOWS README =====';" +
  "Write-Output $b"
], { label: "read-readmes", timeoutSeconds: 60 });
if (readmes.exitCode !== 0 || readmes.timedOut) {
  throw new Error("gather failed: " + (readmes.stderr || readmes.stdout));
}
const SOURCE = String(readmes.stdout);

// Shared preamble every section writer sees.
const PREAMBLE = [
  "你是一位技术文档作者，正在为完全没接触过 servitor 与 servitor-workflows 的人撰写一份自包含 HTML 介绍的一个片段。",
  "下面是两个工具的权威 README 原文。你只能使用其中出现的事实；不得发明任何产品功能、CLI 子命令、选项、路径、文件名或行为。README 没写的就不要写。",
  "代码、命令、路径、选项名、技术术语保留英文原文；讲解用中文。",
  "只输出 JSON 对象 {\"html\": \"...\"}，html 字段是该片段的 HTML 字符串（不要 <!DOCTYPE>、不要 <html>/<head>/<body> 包裹，那是 frame 的工作；你只产出正文片段）。不要加代码围栏，不要加解释文字。",
  "",
  "===== README 原文 =====",
  SOURCE,
  "",
].join("\n");

// Frame: produces the document shell — head/CSS, hero, intro paragraph, TOC,
// and the closing foot. Everything else is a body fragment.
const FRAME_BRIEF = [
  PREAMBLE,
  "你的片段是整个 HTML 文档的「外壳」：返回 {\"head\": \"...\", \"foot\": \"...\"}。",
  "head 字段：从 <!DOCTYPE html> 开始，包含 <html lang=\"zh-CN\">、<head>（<meta charset>、<meta viewport>、<title>、内联 <style>）、<body> 开始标签、顶部 hero 区、一两句话的开篇介绍（讲清这两个工具是什么、解决什么问题）、以及一个可点击的目录 <nav> 指向各章节锚点。",
  "foot 字段：文档结尾，闭合 </body></html>。",
  "CSS 要求：内联在 <style> 里；浅色背景、清晰排版、代码块有背景色与等宽字体、目录与章节有合理间距；不依赖任何外部资源——无 CDN、无外链字体、无外链 JS、无外链图片。目录锚点要对齐下面这些章节 id：what-is / relationship / primitives / control-flow / example / persistence / budget-boundary / cli / design。",
  "head 与 foot 合计控制在 4-6KB。只返回 {\"head\": \"...\", \"foot\": \"...\"}。",
].join("\n");

// Body sections. Each returns {"html": "<section ...>...</section>"}.
// id must match the TOC anchor in the frame.
const SECTIONS = [
  {
    id: "what-is",
    brief: "章节「这两个工具是什么」。用通俗但准确的话讲：servitor 是什么（Rust 传输层，一次请求→一个 provider 进程→持久证据→小型 typed 结果）；servitor-workflows 是什么（构建在 servitor 之上的 Rust 动态工作流运行时）。它们各自解决什么问题。1.5-2.5KB。",
  },
  {
    id: "relationship",
    brief: "章节「职责边界：谁拥有什么」。用一个 HTML <table> 对比 servitor 与 servitor-workflows 各自拥有什么、刻意不拥有什么（如：servitor 拥有 agent 进程提交/检查/取消/输出；workflows 拥有动态工作流/并发/命令/人机 gate/持久化/replay/取消/预算/边界审计；servitor 刻意不做多步执行/fan-out/角色/接受/replay；workflows 不含 provider 适配器/模型路由/兜底/token 预算停跑/Git/CI/部署/UI）。2-3KB。",
  },
  {
    id: "primitives",
    brief: "章节「三个执行原语」。讲 agent / command / gate 各自作用：agent(prompt, options?) 提交一次模型调用并返回 schema 校验后的 JSON；command(program, args?, options?) 运行一个本地命令返回结构化 CommandResult；gate(question, options?) 暂停等人决策。给出每个原语的典型用法代码片段（取自 README 的真实选项名）。强调 JS 没有直接文件系统/进程 API，外部工作必须穿过这三类边界。2-3KB。",
  },
  {
    id: "control-flow",
    brief: "章节「控制流原语」。讲 pipeline(items, ...stages)、parallel(promises)、retry(fn, options?)、gate、supersede(options)、workflow(path, args?, options?)（子工作流）。每个给一句话作用 + 来自 README 的真实选项（如 retry 的 maxAttempts/delayMs/backoff/wallTimeSeconds/nonRetryable；supersede 的 reason/evidence/newContract；workflow 的子 run 独立 journal/共享预算/子只能收紧边界不能放宽）。2.5-3.5KB。",
  },
  {
    id: "example",
    brief: "章节「一个真实可运行的示例」。直接引用 README 里的 audit-routes 示例代码（meta.contract:\"workflow\"、phase、agent 带 schema、pipeline），以及真实运行命令 servitor-workflows run D:\\AgentWork\\tools\\servitor-workflows\\examples\\dynamic.workflow.js。代码块必须可复制。1.5-2.5KB。",
  },
  {
    id: "persistence",
    brief: "章节「持久化与恢复」。讲每个 run 的文件结构（workflow.js/state.json/journal.jsonl/events.jsonl/budget.jsonl/boundary.jsonl/run-summary.html/pause.request/cancel.request）；journal replay 的确定性假设（脚本每次产生相同调用输入，故 Date.now/Math.random/argless new Date/Date() 被禁）；resume 不重提交已结算调用、重跑失败调用；watch 从 events.jsonl 重建整棵树无内存状态；状态机 running|waiting_human|pausing|paused|cancelling|succeeded|failed|cancelled|superseded。2.5-3.5KB。",
  },
  {
    id: "budget-boundary",
    brief: "章节「预算与边界审计」。预算：max_calls 是整棵树的硬上限（默认1000，CLI --max-calls）、meta.moneyCap 可选 cents 仅在显式提供时成硬限、token 仅归因永不停跑、 reservation→settlement→release 崩溃不双扣。边界审计：boundary 声明 readPaths/writePaths/network/environment/isolation，host 对每个命令审计并记入 boundary.jsonl，未声明写/环境越权/子放宽权限即违规并阻断成功；强调这是审计边界非 OS 沙箱；journal 永不存凭据值，密钥走 allowlist 环境变量并脱敏。2.5-3.5KB。",
  },
  {
    id: "cli",
    brief: "章节「CLI 速查」。分两块列出主要命令：servitor（submit/get/list/gc/cancel/inspect/inspect capabilities/doctor/schema/completions/image）与 servitor-workflows（run/check/resume/get/list/approve/reject/pause/cancel/supersede/inspect/watch/schema）。用 README 里的真实命令与标志（如 run 的 --args/--max-parallel/--max-calls/--detach；get 的 --wait/--timeout-seconds；输出模式 --output json|human|quiet|jsonl；退出码 0/1/2/3/4）。用 <pre><code> 或列表呈现。2.5-3.5KB。",
  },
  {
    id: "design",
    brief: "章节「设计理念」。讲为什么用沙箱 JS（Boa 内嵌，普通循环/分支/动态 fan-out 比静态 DAG 清晰，无需 Node/Deno/V8/Python）而非静态 DAG；为什么传输层只做一次调用（servitor 刻意停在 one durable provider call，多步/replay/接受交给 workflows）；为什么 maker/checker 用独立子 run 实现结构独立（reviewer 作为 child workflow 有独立 run/journal/只读边界，maker 输出永不在同一 run 自评）；能力路由显式不静默替换。1.5-2.5KB。",
  },
];

// ---------------------------------------------------------------------------
phase("compose");
// parallel(): every section writer runs concurrently. Each is an independent
// cold call (noContinuation) so no session leaks between fragments, and each
// output is small enough to stay well under the provider deadline.
const thunks = [
  () => agent(FRAME_BRIEF, { label: "frame", role: "maker", timeoutSeconds: 300, noContinuation: true,
    schema: { type: "object", required: ["head", "foot"], properties: { head: { type: "string" }, foot: { type: "string" } } } }),
  ...SECTIONS.map((s) => () =>
    agent(PREAMBLE + "\n你的章节 id=\"" + s.id + "\"。" + s.brief + " 返回 {\"html\": \"<section id=\\\"" + s.id + "\\\"...>...</section>\"}。只返回该 JSON。",
      { label: "sec:" + s.id, role: "maker", timeoutSeconds: 300, noContinuation: true,
        schema: { type: "object", required: ["html"], properties: { html: { type: "string" } } } })),
];
const parts = await parallel(thunks);
// parallel() resolves a failed/rejected thunk to null in place. Surface
// exactly which fragments failed instead of null-derefing on assembly.
if (!parts[0]) {
  const failed = thunks.map((_, i) => parts[i] == null ? String(SECTIONS[i - 1]?.id ?? "frame") : null)
    .filter(Boolean).join(", ");
  throw new Error("frame section returned null; failed fragments: " + failed);
}
const failedSecs = SECTIONS.filter((s, i) => parts[i + 1] == null).map(s => s.id);
if (failedSecs.length) {
  throw new Error("sections returned null: " + failedSecs.join(", "));
}
const frame = parts[0];
const sectionHtml = parts.slice(1).map((p) => String(p.html)).join("\n");
const full = String(frame.head) + "\n" + sectionHtml + "\n" + String(frame.foot);

// ---------------------------------------------------------------------------
phase("write");
const write = await command("pwsh", [
  "-NoProfile", "-Command",
  "$ErrorActionPreference='Stop';" +
  "$p='../docs/servitor-intro.html';" +
  "$dir=Split-Path -Parent $p; if($dir -and !(Test-Path -LiteralPath $dir)){ New-Item -ItemType Directory -Force -Path $dir | Out-Null };" +
  "Set-Content -LiteralPath $p -Value $env:BODY -Encoding utf8 -NoNewline;" +
  "'bytes:' + (Get-Item -LiteralPath $p).Length"
], { label: "write-html", timeoutSeconds: 60, env: { BODY: full } });
if (write.exitCode !== 0 || write.timedOut) {
  throw new Error("write failed: " + (write.stderr || write.stdout));
}

return {
  summary: "generated servitor + servitor-workflows newcomer intro HTML (parallel section assembly)",
  path: "../docs/servitor-intro.html",
  bytes: write.stdout.trim(),
  sectionCount: SECTIONS.length,
};
