//! Machine run-summary HTML only; delivery reports are skill-owned (karma #451).
use crate::error::WorkflowError;
use crate::model::{CallState, JournalEntry, RunState, RunStatus};
use crate::store::WorkflowStore;
use std::fmt::Write as _;
use std::path::PathBuf;

pub fn write(store: &WorkflowStore, state: &RunState) -> Result<PathBuf, WorkflowError> {
    let journal = store.journal_index(&state.run_id)?;
    let mut entries = journal.values().collect::<Vec<_>>();
    entries.sort_by_key(|entry| entry.at);
    let html = render(state, &entries)?;
    let path = store.run_summary_path(&state.run_id);
    std::fs::write(&path, html).map_err(|source| WorkflowError::Write {
        path: path.clone(),
        source,
    })?;
    Ok(path)
}

fn render(state: &RunState, entries: &[&JournalEntry]) -> Result<String, WorkflowError> {
    let succeeded = entries
        .iter()
        .filter(|entry| entry.state == CallState::Succeeded)
        .count();
    let failed = entries
        .iter()
        .filter(|entry| entry.state == CallState::Failed)
        .count();
    let result_json = match &state.result {
        Some(value) => serde_json::to_string_pretty(value)?,
        None => "null".to_owned(),
    };
    let approved = result_json.contains("VERDICT=APPROVED");
    let verdict = match state.status {
        RunStatus::Succeeded if approved => "执行成功，语义检查通过",
        RunStatus::Succeeded => "执行成功",
        RunStatus::Failed => "执行失败，需要处理错误后继续",
        RunStatus::Cancelled => "执行已取消",
        _ => "执行尚未结束",
    };
    let phase = state.phase.as_deref().unwrap_or("未声明");
    let status = status_name(&state.status);
    let error = state.error.as_deref().unwrap_or("无");
    let mut rows = String::new();
    let mut technical_rows = String::new();
    for (index, entry) in entries.iter().enumerate() {
        let transport = entry.transport_run_id.as_deref().unwrap_or("—");
        write!(
            rows,
            "<tr><td>{}</td><td>{}</td><td>{}</td><td>{}</td></tr>",
            index + 1,
            escape(&entry.label),
            escape(call_kind(entry)),
            escape(call_state(entry)),
        )
        .map_err(|_| WorkflowError::Invariant("failed to render report row".to_owned()))?;
        write!(
            technical_rows,
            "<tr><td>{}</td><td><code>{}</code></td><td><code>{}</code></td></tr>",
            escape(&entry.label),
            escape(&entry.key),
            escape(transport),
        )
        .map_err(|_| WorkflowError::Invariant("failed to render technical row".to_owned()))?;
    }
    if rows.is_empty() {
        rows.push_str(
            "<tr><td colspan=\"4\" class=\"muted\">这个 workflow 没有外部调用。</td></tr>",
        );
        technical_rows
            .push_str("<tr><td colspan=\"3\" class=\"muted\">没有技术调用标识。</td></tr>");
    }

    Ok(format!(
        r#"<!doctype html>
<html lang="zh-CN">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Workflow 运行摘要 · {name}</title>
<style>
:root{{--paper:#fffffb;--soft:#f4f5f2;--ink:#111;--muted:#6b6b64;--line:#d9d9d1;--strong:#202020;--accent:#315d7c;--ok:#2f5b45;--bad:#8a3d35}}
*{{box-sizing:border-box}} body{{margin:0;background:var(--soft);color:var(--ink);font:15px/1.7 system-ui,"Segoe UI","Microsoft YaHei",sans-serif}}
.shell{{max-width:1080px;margin:0 auto;padding:48px 28px 80px}} header{{border-top:5px solid var(--strong);padding:28px 0 24px;border-bottom:1px solid var(--line)}}
.eyebrow{{color:var(--muted);font-size:12px;letter-spacing:.08em;text-transform:uppercase}} h1{{font-size:34px;line-height:1.2;margin:8px 0 10px;letter-spacing:-.02em}} .lead{{font-size:19px;max-width:760px;margin:0}}
.chips{{display:flex;gap:8px;flex-wrap:wrap;margin-top:18px}} .chip{{border:1px solid var(--line);background:var(--paper);padding:3px 9px;border-radius:999px;font-size:12px}}
main{{display:grid;grid-template-columns:minmax(0,1fr) 260px;gap:24px;margin-top:24px}} .stack{{display:grid;gap:18px}} .card{{background:var(--paper);border:1px solid var(--line);padding:22px}}
h2{{font-size:18px;margin:0 0 14px}} h3{{font-size:14px;margin:18px 0 8px}} dl{{display:grid;grid-template-columns:150px 1fr;gap:7px 14px;margin:0}} dt{{color:var(--muted)}} dd{{margin:0;overflow-wrap:anywhere}}
.flow{{display:flex;align-items:center;gap:8px;flex-wrap:wrap}} .node{{border:1px solid var(--line);padding:7px 10px;background:var(--soft)}} .arrow{{color:var(--muted)}}
table{{border-collapse:collapse;width:100%;font-size:13px}} th,td{{text-align:left;border-bottom:1px solid var(--line);padding:9px 8px;vertical-align:top}} th{{color:var(--muted);font-weight:600}} .table-wrap{{overflow:auto}}
code,pre{{font-family:"Cascadia Mono",Consolas,monospace}} code{{overflow-wrap:anywhere}} pre{{white-space:pre-wrap;overflow-wrap:anywhere;background:#f7f7f3;border:1px solid var(--line);padding:14px;max-height:520px;overflow:auto}}
details summary{{cursor:pointer;font-weight:600}} .muted{{color:var(--muted)}} a{{color:var(--accent)}}
@media(max-width:780px){{.shell{{padding:24px 14px 50px}} main{{grid-template-columns:1fr}} h1{{font-size:28px}} dl{{grid-template-columns:1fr}} dt{{margin-top:6px}}}}
@media print{{body{{background:#fff}} .shell{{max-width:none;padding:0}} main{{display:block}} .card{{break-inside:avoid;margin-bottom:14px}} details{{display:block}} details>summary{{display:none}} details>*{{display:block}} pre{{max-height:none}}}}
</style>
</head>
<body><div class="shell">
<header><div class="eyebrow">Servitor Workflows · Run Summary</div><h1>Workflow 运行摘要</h1><p class="lead">{verdict}</p>
<div class="chips"><span class="chip">任务 {name}</span><span class="chip">状态 {status}</span><span class="chip">阶段 {phase}</span><span class="chip">恢复 {resume_count} 次</span><span class="chip">成功调用 {succeeded}</span><span class="chip">失败调用 {failed}</span></div></header>
<main><div class="stack">
<section class="card"><h2>运行状态</h2><div class="flow"><span class="node">workflow</span><span class="arrow">→</span><span class="node">calls</span><span class="arrow">→</span><span class="node">terminal state</span></div>
<p>当前 run 已进入 <strong>{status}</strong>，最后阶段为 <strong>{phase}</strong>。此页只汇总编排器事实；项目结论由 workflow 返回的交付汇报承载。</p></section>
<section class="card"><h2>调用与检查证据</h2><div class="table-wrap"><table><thead><tr><th>步骤</th><th>标签</th><th>类型</th><th>状态</th></tr></thead><tbody>{rows}</tbody></table></div>
<details><summary>技术标识</summary><div class="table-wrap"><table><thead><tr><th>标签</th><th>稳定键</th><th>Transport run</th></tr></thead><tbody>{technical_rows}</tbody></table></div></details></section>
<section class="card"><h2>最终输出</h2><details><summary>展开结构化结果</summary><pre>{result}</pre></details></section>
<section class="card"><h2>边界</h2><p>这个页面根据持久化 state 与 journal 生成。它只证明编排器记录的执行结果与调用状态，不替代项目测试、线上 smoke、人工验收、外部事实核查或面向读者的交付判断。</p><p>错误信息：<code>{error}</code></p></section>
</div><aside class="stack"><section class="card"><h2>运行身份</h2><dl><dt>任务</dt><dd>{name}</dd><dt>Run ID</dt><dd><code>{run_id}</code></dd><dt>创建时间</dt><dd>{created}</dd><dt>更新时间</dt><dd>{updated}</dd></dl></section>
<section class="card"><h2>证据位置</h2><dl><dt>State</dt><dd><code>state.json</code></dd><dt>Journal</dt><dd><code>journal.jsonl</code></dd><dt>Workflow</dt><dd><code>workflow.js</code></dd><dt>Run summary</dt><dd><code>run-summary.html</code></dd></dl></section></aside></main>
</div></body></html>"#,
        name = escape(&state.name),
        verdict = escape(verdict),
        status = escape(status),
        phase = escape(phase),
        resume_count = state.resume_count,
        succeeded = succeeded,
        failed = failed,
        rows = rows,
        technical_rows = technical_rows,
        result = escape(&result_json),
        error = escape(error),
        run_id = escape(&state.run_id),
        created = escape(&state.created_at.to_rfc3339()),
        updated = escape(&state.updated_at.to_rfc3339()),
    ))
}

fn status_name(status: &RunStatus) -> &'static str {
    match status {
        RunStatus::Running => "running",
        RunStatus::WaitingHuman => "waiting_human",
        RunStatus::Pausing => "pausing",
        RunStatus::Paused => "paused",
        RunStatus::Cancelling => "cancelling",
        RunStatus::Succeeded => "succeeded",
        RunStatus::Failed => "failed",
        RunStatus::Cancelled => "cancelled",
    }
}

fn call_kind(entry: &JournalEntry) -> &'static str {
    match entry.kind {
        crate::model::CallKind::Agent => "agent",
        crate::model::CallKind::Command => "command",
        crate::model::CallKind::Gate => "gate",
    }
}

fn call_state(entry: &JournalEntry) -> &'static str {
    match entry.state {
        CallState::Submitted => "submitted",
        CallState::Succeeded => "succeeded",
        CallState::Failed => "failed",
        CallState::Cancelled => "cancelled",
    }
}

fn escape(text: &str) -> String {
    text.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&#39;")
}

#[cfg(test)]
mod tests {
    use super::escape;

    #[test]
    fn html_escape_covers_markup_and_quotes() {
        assert_eq!(escape("<a x='1'>&\""), "&lt;a x=&#39;1&#39;&gt;&amp;&quot;");
    }
}
