//! Machine run-summary HTML only; delivery reports are skill-owned (karma #451).
use crate::error::WorkflowError;
use crate::model::{CallState, JournalEntry, RunState, RunStatus};
use crate::store::WorkflowStore;
use serde_json::Value;
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
        RunStatus::WaitingHuman => "执行暂停，等待人工闸门审批",
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
            "<tr><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td></tr>",
            index + 1,
            escape(&entry.label),
            escape(call_kind(entry)),
            escape(call_state(entry)),
            escape(entry.phase.as_deref().unwrap_or("—")),
            format_duration(entry.duration_ms),
            usage_tokens(entry.usage.as_ref()).map_or_else(|| "—".to_owned(), |n| n.to_string()),
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
            "<tr><td colspan=\"7\" class=\"muted\">这个 workflow 没有外部调用。</td></tr>",
        );
        technical_rows
            .push_str("<tr><td colspan=\"3\" class=\"muted\">没有技术调用标识。</td></tr>");
    }
    let total_tokens = entries
        .iter()
        .filter_map(|entry| usage_tokens(entry.usage.as_ref()))
        .fold(None::<u64>, |acc, n| {
            Some(acc.unwrap_or(0).saturating_add(n))
        });
    let tokens_chip = total_tokens.map_or_else(String::new, |total| {
        format!("<span class=\"chip\">Tokens {total}</span>")
    });
    let gate_card = if let Some(gate) = state.waiting_gate.as_ref() {
        let expect_row = gate.expect.as_deref().map_or_else(String::new, |expect| {
            format!("<dt>期望</dt><dd>{}</dd>", escape(expect))
        });
        let current_row = match gate.current.as_ref() {
            Some(current) => format!(
                "<dt>当前值</dt><dd><pre>{}</pre></dd>",
                escape(&serde_json::to_string_pretty(current)?)
            ),
            None => String::new(),
        };
        let hint_row = gate.hint.as_deref().map_or_else(String::new, |hint| {
            format!("<dt>提示</dt><dd>{}</dd>", escape(hint))
        });
        format!(
            "<section class=\"card\"><h2>等待人工审批</h2><dl><dt>闸门</dt><dd>{}</dd><dt>问题</dt><dd>{}</dd>{expect_row}{current_row}{hint_row}</dl><p>处理命令：<code>servitor-workflows approve {} --reason &quot;...&quot;</code> 或 <code>servitor-workflows reject {} --reason &quot;...&quot;</code></p></section>",
            escape(&gate.label),
            escape(&gate.question),
            escape(&state.run_id),
            escape(&state.run_id),
        )
    } else {
        String::new()
    };

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
<div class="chips"><span class="chip">任务 {name}</span><span class="chip">状态 {status}</span><span class="chip">阶段 {phase}</span><span class="chip">恢复 {resume_count} 次</span><span class="chip">成功调用 {succeeded}</span><span class="chip">失败调用 {failed}</span>{tokens_chip}</div></header>
<main><div class="stack">
{gate_card}<section class="card"><h2>运行状态</h2><div class="flow"><span class="node">workflow</span><span class="arrow">→</span><span class="node">calls</span><span class="arrow">→</span><span class="node">terminal state</span></div>
<p>当前 run 已进入 <strong>{status}</strong>，最后阶段为 <strong>{phase}</strong>。此页只汇总编排器事实；项目结论由 workflow 返回的交付汇报承载。</p></section>
<section class="card"><h2>调用与检查证据</h2><div class="table-wrap"><table><thead><tr><th>步骤</th><th>标签</th><th>类型</th><th>状态</th><th>阶段</th><th>耗时</th><th>Tokens</th></tr></thead><tbody>{rows}</tbody></table></div>
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
        tokens_chip = tokens_chip,
        gate_card = gate_card,
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
        RunStatus::Superseded => "superseded",
    }
}

fn call_kind(entry: &JournalEntry) -> &'static str {
    match entry.kind {
        crate::model::CallKind::Agent => "agent",
        crate::model::CallKind::Command => "command",
        crate::model::CallKind::Gate => "gate",
        crate::model::CallKind::Workflow => "workflow",
        crate::model::CallKind::Spawn => "spawn",
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

fn format_duration(ms: Option<u64>) -> String {
    match ms {
        Some(ms) if ms >= 1000 => format!("{:.1}s", ms as f64 / 1000.0),
        Some(ms) => format!("{ms}ms"),
        None => "—".to_owned(),
    }
}

pub fn usage_tokens(usage: Option<&Value>) -> Option<u64> {
    let usage = usage?.as_object()?;
    for key in ["total_tokens", "totalTokens", "total"] {
        if let Some(total) = usage.get(key).and_then(Value::as_u64) {
            return Some(total);
        }
    }
    for (input, output) in [
        ("input", "output"),
        ("input_tokens", "output_tokens"),
        ("prompt_tokens", "completion_tokens"),
        ("inputTokens", "outputTokens"),
    ] {
        if let (Some(input), Some(output)) = (
            usage.get(input).and_then(Value::as_u64),
            usage.get(output).and_then(Value::as_u64),
        ) {
            return Some(input.saturating_add(output));
        }
    }
    None
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
    use super::{escape, format_duration, usage_tokens};
    use serde_json::json;

    #[test]
    fn html_escape_covers_markup_and_quotes() {
        assert_eq!(escape("<a x='1'>&\""), "&lt;a x=&#39;1&#39;&gt;&amp;&quot;");
    }

    #[test]
    fn usage_tokens_supports_provider_shapes() {
        assert_eq!(usage_tokens(Some(&json!({"total_tokens": 42}))), Some(42));
        assert_eq!(usage_tokens(Some(&json!({"totalTokens": 42}))), Some(42));
        assert_eq!(
            usage_tokens(Some(&json!({"input": 100, "output": 20}))),
            Some(120)
        );
        assert_eq!(
            usage_tokens(Some(&json!({"input_tokens": 1, "output_tokens": 2}))),
            Some(3)
        );
        assert_eq!(usage_tokens(Some(&json!({"weird": true}))), None);
        assert_eq!(usage_tokens(None), None);
    }

    #[test]
    fn duration_format_is_compact() {
        assert_eq!(format_duration(Some(250)), "250ms");
        assert_eq!(format_duration(Some(12_340)), "12.3s");
        assert_eq!(format_duration(None), "—");
    }
}
