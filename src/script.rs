use crate::agent::AgentOptions;
use crate::command::CommandOptions;
use crate::error::WorkflowError;
use crate::model::{CallKind, CallState, GateRequest, JournalEntry, RunStatus, WorkflowEvent};
use crate::scheduler::{RuntimeHost, call_key};
use boa_engine::{
    Context, JsArgs, JsNativeError, JsResult, JsValue, NativeFunction, Source,
    builtins::promise::PromiseState, js_string, object::JsData, object::builtins::JsPromise,
};
use boa_gc::{Finalize, Trace};
use chrono::Utc;
use serde::Deserialize;
use serde_json::{Value, json};
use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet};
use std::fs::OpenOptions;
use std::io::Write;
use std::sync::{Arc, Mutex};
use std::thread;

const BOOTSTRAP: &str = r#"
globalThis.args = JSON.parse(__argsJson);
globalThis.agent = async (prompt, options = {}) =>
  JSON.parse(await __agent(String(prompt), JSON.stringify(options)));
globalThis.command = async (program, argv = [], options = {}) =>
  JSON.parse(await __command(String(program), JSON.stringify(argv), JSON.stringify(options)));
globalThis.gate = async (question, options = {}) =>
  JSON.parse(await __gate(String(question), JSON.stringify(options)));
globalThis.workflow = async (path, args = {}, options = {}) =>
  JSON.parse(await __workflow(String(path), JSON.stringify(args), JSON.stringify(options)));
globalThis.supersede = async options =>
  JSON.parse(await __supersede(JSON.stringify(options || {})));
globalThis.phase = name => __phase(String(name));
// parallel(entries) never rejects. Each entry may be a thunk (() => ...) or an
// already-created promise; thunks are invoked at parallel entry. A failed or
// throwing entry resolves to null in place; siblings are unaffected. Result
// order matches input order. (Backward-compatible except reject-on-first-failure
// becomes null-in-place — the accepted behavior break for this slice.)
globalThis.parallel = entries => {
  const arr = Array.from(entries);
  return Promise.all(arr.map(entry => {
    let p;
    try {
      p = typeof entry === 'function' ? entry() : entry;
    } catch (e) {
      return Promise.resolve(null);
    }
    return Promise.resolve(p).then(v => v, () => null);
  }));
};
// pipeline(items, ...stages) runs each item through every stage in order,
// independently per item — no cross-item barrier between stages. Each stage
// callback receives (prevResult, originalItem, index). A stage that throws
// (or rejects) drops only its own item to null and skips that item's remaining
// stages. pipeline never rejects. The degenerate pipeline(items, worker) call
// is the N=1 case and produces the same results as before (minus reject).
globalThis.pipeline = (items, ...stages) => {
  const arr = Array.from(items);
  const fns = stages.filter(s => typeof s === 'function');
  return Promise.all(arr.map((item, idx) => (async () => {
    let cur = item;
    for (let s = 0; s < fns.length; s++) {
      try {
        cur = await fns[s](cur, item, idx);
      } catch (e) {
        return null;
      }
    }
    return cur;
  })()));
};
// log(message) streams narration to workflow.log in the run record dir via the
// __log host function. It never becomes a journal entry. Resume idempotency is
// occurrence-based: on VM start the host counts lines already in workflow.log,
// and the first N log() calls of a re-executed script are skipped (same
// determinism assumption journal replay already relies on), so a resumed run
// does not duplicate narration. log() returns undefined and never throws; a
// write failure is swallowed, never propagated into the script.
globalThis.log = message => { try { __log(String(message == null ? '' : message)); } catch (e) {} };
globalThis.retry = (() => {
  // Captured native clock: exempt from the determinism guard below because it
  // never feeds call keys — it only bounds wall-time inside retry().
  const __nowMs = Date.now.bind(Date);
  return async (fn, options = {}) => {
    const maxAttempts = options.maxAttempts ?? 3;
    const delayMs = options.delayMs ?? 1000;
    const backoff = options.backoff ?? 1;
    const wallMs = options.wallTimeSeconds != null ? options.wallTimeSeconds * 1000 : null;
    const nonRetryable = options.nonRetryable ?? [];
    const started = __nowMs();
    let lastError;
    let delay = delayMs;
    for (let attempt = 1; attempt <= maxAttempts; attempt++) {
      if (wallMs != null && __nowMs() - started >= wallMs) {
        throw new Error(`retry wall-time exceeded after ${attempt - 1} attempts`);
      }
      try {
        return await fn(attempt);
      } catch (error) {
        lastError = error;
        const text = String(error && error.message ? error.message : error);
        if (nonRetryable.some(marker => text.includes(marker))) {
          throw error;
        }
        if (attempt < maxAttempts) {
          await __sleep(delay);
          delay = Math.round(delay * backoff);
        }
      }
    }
    throw lastError;
  };
})();
// Determinism guard — MUST stay the FINAL block of BOOTSTRAP. Prelude helpers
// that legitimately need the native clock (retry above) capture it before the
// swap; anything defined after this point sees GuardedDate. Later prelude
// additions go BEFORE this block. Wall-clock or random values flowing into
// agent/command/gate inputs change call keys on journal-replay resume and
// re-execute paid work, so the common nondeterminism sources throw. This is an
// anti-footgun, not a security sandbox: a determined script can still reach
// nondeterminism; deterministic PRNGs seeded from args stay fine.
(() => {
  const NativeDate = Date;
  const TAIL = " Pass timestamps in via args (e.g. args.startedAt) and stamp wall-clock times after the workflow returns; new Date(explicitValue) stays legal.";
  function GuardedDate(...a) {
    if (new.target === undefined) {
      throw new TypeError("Date() called as a function is nondeterministic and breaks journal-replay resume." + TAIL);
    }
    if (a.length === 0) {
      throw new TypeError("new Date() without arguments is nondeterministic and breaks journal-replay resume." + TAIL);
    }
    return new NativeDate(...a);
  }
  GuardedDate.prototype = NativeDate.prototype;
  NativeDate.prototype.constructor = GuardedDate; // close the `(new Date(0)).constructor` escape
  GuardedDate.parse = NativeDate.parse;           // deterministic statics stay legal
  GuardedDate.UTC = NativeDate.UTC;
  GuardedDate.now = () => {
    throw new TypeError("Date.now() is nondeterministic and breaks journal-replay resume." + TAIL);
  };
  globalThis.Date = GuardedDate;
  Math.random = () => {
    throw new TypeError("Math.random() is nondeterministic and breaks journal-replay resume. Pass any needed randomness in via args or compute it outside the workflow.");
  };
})();
"#;
const VM_STACK_SIZE: usize = 8 * 1024 * 1024;

#[derive(Clone, Finalize, Trace)]
struct HostState {
    #[unsafe_ignore_trace]
    runtime: Arc<RuntimeHost>,
    #[unsafe_ignore_trace]
    occurrences: Arc<Mutex<BTreeMap<String, usize>>>,
    #[unsafe_ignore_trace]
    calls: Arc<Mutex<usize>>,
    /// Snapshot of journal keys at VM start plus keys completed in this process.
    #[unsafe_ignore_trace]
    journal_keys: Arc<Mutex<BTreeSet<String>>>,
    /// log() resume idempotency: (lines already in workflow.log at VM start,
    /// log() calls seen in this execution). Lazily initialized on first __log.
    /// A re-executed script skips its first `existing` calls instead of
    /// re-appending them — occurrence-based, like journal replay.
    #[unsafe_ignore_trace]
    log_replay: Arc<Mutex<Option<(usize, usize)>>>,
    #[unsafe_ignore_trace]
    phase: Arc<Mutex<Option<String>>>,
    #[unsafe_ignore_trace]
    budget: Option<crate::budget::Budget>,
    max_calls: usize,
}

impl JsData for HostState {}

impl HostState {
    fn key(&self, kind: &str, input: &Value) -> Result<String, String> {
        let identity = format!(
            "{kind}:{}",
            serde_json::to_string(input).map_err(|error| error.to_string())?
        );
        let mut occurrences = self
            .occurrences
            .lock()
            .map_err(|_| "occurrence lock poisoned".to_owned())?;
        let occurrence = occurrences.entry(identity).or_default();
        let key = call_key(kind, input, *occurrence);
        *occurrence += 1;

        // Replay of journaled keys is free. New keys consume the shared max_calls
        // budget that already includes prior journal entries at VM start.
        // Budget counts every host key (agent/command/gate), not agent-only.
        {
            let journal_keys = self
                .journal_keys
                .lock()
                .map_err(|_| "journal key lock poisoned".to_owned())?;
            if journal_keys.contains(&key) {
                return Ok(key);
            }
        }

        // V2-B: when a budget handle is present, gate through the durable
        // budget.jsonl ledger; otherwise fall back to the v1 in-memory counter.
        if let Some(ref budget) = self.budget {
            let call_kind = match kind {
                "agent" => CallKind::Agent,
                "command" => CallKind::Command,
                "gate" => CallKind::Gate,
                "workflow" => CallKind::Workflow,
                _ => {
                    return Err("unknown call kind".to_owned());
                }
            };
            let jk = self
                .journal_keys
                .lock()
                .map_err(|_| "journal key lock poisoned".to_owned())?;
            crate::budget::budget_gate_key(budget, &key, call_kind, input, &jk, self.max_calls)
                .map_err(|error| error.to_string())?;
        } else {
            let mut calls = self
                .calls
                .lock()
                .map_err(|_| "call counter lock poisoned".to_owned())?;
            *calls += 1;
            if *calls > self.max_calls {
                return Err(format!("workflow exceeded max_calls={}", self.max_calls));
            }
        }
        Ok(key)
    }

    fn remember_journal_key(&self, key: &str) -> Result<(), String> {
        let mut journal_keys = self
            .journal_keys
            .lock()
            .map_err(|_| "journal key lock poisoned".to_owned())?;
        journal_keys.insert(key.to_owned());
        Ok(())
    }
}

#[derive(Debug, Deserialize, Default)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct GateOptions {
    #[serde(default)]
    label: Option<String>,
    #[serde(default)]
    expect: Option<String>,
    #[serde(default)]
    current: Option<Value>,
    #[serde(default)]
    hint: Option<String>,
}

fn wrap_script(script: &str) -> String {
    let body = match meta_export_start(script) {
        Some(start) => format!(
            "{}const meta{}",
            &script[..start],
            &script[start + "export const meta".len()..]
        ),
        None => script.to_owned(),
    };
    format!(
        "(async () => {{ const __result = await (async () => {{ {body} }})(); return JSON.stringify(__result ?? null); }})()"
    )
}

/// Parse the workflow exactly as `execute` would run it, without executing.
/// Catches plain syntax errors and module-only syntax (e.g. `import.meta`)
/// that would otherwise kill the run before its first phase.
pub fn parse_check(script: &str) -> Result<(), WorkflowError> {
    let mut context = Context::default();
    let wrapped = wrap_script(script);
    boa_engine::Script::parse(Source::from_bytes(&wrapped), None, &mut context)
        .map(|_| ())
        .map_err(|error| WorkflowError::InvalidWorkflow(error.to_string()))
}

/// Extract the `export const meta = { ... }` object literal as JSON-like data.
/// Returns `Ok(None)` when the script declares no `meta`. Metadata uses the
/// JSON5 literal subset (unquoted keys, single-quoted strings, arrays, nested
/// values, and comments), but never evaluates JavaScript expressions. This
/// keeps validation side-effect-free and rejects computed properties, calls,
/// getters, and references to ambient globals.
pub fn parse_meta(script: &str) -> Result<Option<Value>, WorkflowError> {
    let Some(literal) = extract_meta_literal(script) else {
        return Ok(None);
    };
    json5::from_str(&literal).map(Some).map_err(|error| {
        WorkflowError::InvalidWorkflow(format!("meta must be a JSON5 object literal: {error}"))
    })
}

/// Resolve the workflow contract from its `meta` declaration.
///
/// - `Some("workflow.v2")` when `meta.contract` is the string `"workflow.v2"`;
///   the run becomes a v2 run with the versioned event stream.
/// - `None` when `meta` omits `contract` — the run stays on the frozen v1
///   compatibility path (no event stream, journal-only replay).
/// - `Err` when `contract` is present but not the string `"workflow.v2"`
///   (null, number, bool, or any other string). New v2 runs must not accept
///   missing or non-v2 contract metadata.
pub fn contract_of(script: &str) -> Result<Option<String>, WorkflowError> {
    let Some(meta) = parse_meta(script)? else {
        return Ok(None);
    };
    match meta.get("contract") {
        None => Ok(None),
        Some(Value::String(s)) if s == "workflow.v2" => Ok(Some(s.clone())),
        Some(other) => Err(WorkflowError::InvalidWorkflow(format!(
            "unsupported workflow `contract` metadata: {} — expected the string \"workflow.v2\" or omit the field for a v1 run",
            summarize_contract(other)
        ))),
    }
}

fn summarize_contract(value: &Value) -> String {
    match value {
        Value::String(s) => format!("\"{s}\""),
        Value::Null => "null".to_owned(),
        other => other.to_string(),
    }
}

/// Locate a real top-level `export const meta` declaration, ignoring strings,
/// comments, and regular-expression literals. The byte after `meta` must be
/// whitespace or `=` so `export const metadata` is not treated as a workflow
/// declaration.
fn meta_export_start(script: &str) -> Option<usize> {
    const KEY: &[u8] = b"export const meta";
    let bytes = script.as_bytes();
    let mut index = 0;
    let mut depth = 0usize;
    let mut expects_expression = true;

    while index < bytes.len() {
        let byte = bytes[index];
        if byte.is_ascii_whitespace() {
            index += 1;
            continue;
        }
        if byte == b'/' && bytes.get(index + 1) == Some(&b'/') {
            index += 2;
            while bytes
                .get(index)
                .is_some_and(|next| *next != b'\n' && *next != b'\r')
            {
                index += 1;
            }
            continue;
        }
        if byte == b'/' && bytes.get(index + 1) == Some(&b'*') {
            index += 2;
            while !(bytes.get(index) == Some(&b'*') && bytes.get(index + 1) == Some(&b'/')) {
                bytes.get(index)?;
                index += 1;
            }
            index += 2;
            continue;
        }
        if matches!(byte, b'\'' | b'"' | b'`') {
            index = skip_quoted(bytes, index)?;
            expects_expression = false;
            continue;
        }
        if byte == b'/' && expects_expression {
            index = skip_regex(bytes, index)?;
            expects_expression = false;
            continue;
        }
        if depth == 0 && bytes[index..].starts_with(KEY) {
            let following = bytes.get(index + KEY.len()).copied();
            if following.is_none_or(|next| next.is_ascii_whitespace() || next == b'=') {
                return Some(index);
            }
        }
        if byte.is_ascii_alphabetic() || byte == b'_' || byte == b'$' {
            let start = index;
            index += 1;
            while bytes
                .get(index)
                .is_some_and(|next| next.is_ascii_alphanumeric() || *next == b'_' || *next == b'$')
            {
                index += 1;
            }
            let word = &script[start..index];
            expects_expression = matches!(
                word,
                "return"
                    | "case"
                    | "throw"
                    | "else"
                    | "do"
                    | "typeof"
                    | "void"
                    | "delete"
                    | "new"
                    | "in"
                    | "of"
                    | "yield"
                    | "await"
                    | "const"
                    | "let"
                    | "var"
            );
            continue;
        }
        expects_expression = match byte {
            b'{' => {
                depth += 1;
                true
            }
            b'}' => {
                depth = depth.saturating_sub(1);
                false
            }
            b'(' | b'[' => true,
            b')' | b']' => false,
            b'.' => false,
            b'+' | b'-' if bytes.get(index + 1) == Some(&byte) => false,
            b'/' => true,
            _ => !byte.is_ascii_digit(),
        };
        index += 1;
    }
    None
}

fn skip_quoted(bytes: &[u8], mut index: usize) -> Option<usize> {
    let quote = *bytes.get(index)?;
    index += 1;
    while let Some(byte) = bytes.get(index) {
        if *byte == b'\\' {
            index += 2;
        } else if *byte == quote {
            return Some(index + 1);
        } else {
            index += 1;
        }
    }
    None
}

fn skip_regex(bytes: &[u8], mut index: usize) -> Option<usize> {
    debug_assert_eq!(bytes.get(index), Some(&b'/'));
    index += 1;
    let mut in_class = false;
    while let Some(byte) = bytes.get(index) {
        match *byte {
            b'\\' => index += 2,
            b'[' => {
                in_class = true;
                index += 1;
            }
            b']' => {
                in_class = false;
                index += 1;
            }
            b'/' if !in_class => {
                index += 1;
                while bytes
                    .get(index)
                    .is_some_and(|flag| flag.is_ascii_alphabetic())
                {
                    index += 1;
                }
                return Some(index);
            }
            b'\n' | b'\r' => return None,
            _ => index += 1,
        }
    }
    None
}

/// Scan `export const meta = { ... }` and return the balanced object literal
/// substring (including the outer braces). Returns `None` when no meta literal
/// can be located. Tracks string and brace nesting so a `}` inside a string
/// value does not prematurely close the scan.
fn extract_meta_literal(script: &str) -> Option<String> {
    let start = meta_export_start(script)?;
    let after_key = &script[start + "export const meta".len()..];
    let eq = skip_metadata_trivia(after_key)?;
    if after_key.as_bytes().get(eq) != Some(&b'=') {
        return None;
    }
    let rest = &after_key[skip_metadata_trivia(&after_key[eq + 1..])? + eq + 1..];
    if rest.as_bytes().first() != Some(&b'{') {
        return None;
    }
    let bytes = rest.as_bytes();
    let mut depth: i64 = 0;
    let mut quote: Option<u8> = None;
    let mut escaped = false;
    let mut index = 0;

    while index < bytes.len() {
        let byte = bytes[index];
        if let Some(active_quote) = quote {
            if escaped {
                escaped = false;
            } else if byte == b'\\' {
                escaped = true;
            } else if byte == active_quote {
                quote = None;
            }
            index += 1;
            continue;
        }
        if byte == b'/' && bytes.get(index + 1) == Some(&b'/') {
            index += 2;
            while index < bytes.len() && bytes[index] != b'\n' && bytes[index] != b'\r' {
                index += 1;
            }
            continue;
        }
        if byte == b'/' && bytes.get(index + 1) == Some(&b'*') {
            index += 2;
            while index + 1 < bytes.len() && !(bytes[index] == b'*' && bytes[index + 1] == b'/') {
                index += 1;
            }
            if index + 1 >= bytes.len() {
                return None;
            }
            index += 2;
            continue;
        }
        match byte {
            b'{' => depth += 1,
            b'}' => {
                depth -= 1;
                if depth == 0 {
                    return Some(rest[..=index].to_owned());
                }
            }
            b'\'' | b'\"' => quote = Some(byte),
            _ => {}
        }
        index += 1;
    }
    None
}

fn skip_metadata_trivia(text: &str) -> Option<usize> {
    let bytes = text.as_bytes();
    let mut index = 0;
    loop {
        while bytes
            .get(index)
            .is_some_and(|byte| byte.is_ascii_whitespace())
        {
            index += 1;
        }
        if bytes.get(index) == Some(&b'/') && bytes.get(index + 1) == Some(&b'/') {
            index += 2;
            while bytes
                .get(index)
                .is_some_and(|byte| *byte != b'\n' && *byte != b'\r')
            {
                index += 1;
            }
            continue;
        }
        if bytes.get(index) == Some(&b'/') && bytes.get(index + 1) == Some(&b'*') {
            index += 2;
            while !(bytes.get(index) == Some(&b'*') && bytes.get(index + 1) == Some(&b'/')) {
                bytes.get(index)?;
                index += 1;
            }
            index += 2;
            continue;
        }
        return Some(index);
    }
}

pub fn execute(
    runtime: Arc<RuntimeHost>,
    script: &str,
    args: &Value,
    max_calls: usize,
) -> Result<Value, WorkflowError> {
    let script = script.to_owned();
    let args = args.clone();
    thread::Builder::new()
        .name("servitor-workflow-js".to_owned())
        .stack_size(VM_STACK_SIZE)
        .spawn(move || execute_vm(runtime, &script, &args, max_calls))
        .map_err(|error| {
            WorkflowError::Invariant(format!("failed to start JavaScript VM: {error}"))
        })?
        .join()
        .map_err(|_| WorkflowError::Invariant("JavaScript VM panicked".to_owned()))?
}

fn execute_vm(
    runtime: Arc<RuntimeHost>,
    script: &str,
    args: &Value,
    max_calls: usize,
) -> Result<Value, WorkflowError> {
    let mut context = Context::default();
    let budget = runtime.budget.clone();
    let journal_index = runtime
        .store
        .journal_index(&runtime.run_id)
        .unwrap_or_default();
    let journal_used = journal_index.len();
    let journal_keys: BTreeSet<String> = journal_index.into_keys().collect();
    let initial_phase = runtime
        .store
        .load_state(&runtime.run_id)
        .ok()
        .and_then(|state| state.phase);
    context.insert_data(HostState {
        runtime,
        occurrences: Arc::new(Mutex::new(BTreeMap::new())),
        calls: Arc::new(Mutex::new(journal_used)),
        journal_keys: Arc::new(Mutex::new(journal_keys)),
        log_replay: Arc::new(Mutex::new(None)),
        phase: Arc::new(Mutex::new(initial_phase)),
        budget,
        max_calls,
    });
    context
        .register_global_builtin_callable(
            js_string!("__agent"),
            2,
            NativeFunction::from_async_fn(host_agent),
        )
        .map_err(js_error)?;
    context
        .register_global_builtin_callable(
            js_string!("__command"),
            3,
            NativeFunction::from_async_fn(host_command),
        )
        .map_err(js_error)?;
    context
        .register_global_builtin_callable(
            js_string!("__gate"),
            2,
            NativeFunction::from_async_fn(host_gate),
        )
        .map_err(js_error)?;
    context
        .register_global_builtin_callable(
            js_string!("__workflow"),
            3,
            NativeFunction::from_async_fn(host_workflow),
        )
        .map_err(js_error)?;
    context
        .register_global_builtin_callable(
            js_string!("__supersede"),
            1,
            NativeFunction::from_async_fn(host_supersede),
        )
        .map_err(js_error)?;
    context
        .register_global_builtin_callable(
            js_string!("__phase"),
            1,
            NativeFunction::from_fn_ptr(host_phase),
        )
        .map_err(js_error)?;
    context
        .register_global_builtin_callable(
            js_string!("__sleep"),
            1,
            NativeFunction::from_fn_ptr(host_sleep),
        )
        .map_err(js_error)?;
    context
        .register_global_builtin_callable(
            js_string!("__log"),
            1,
            NativeFunction::from_fn_ptr(host_log),
        )
        .map_err(js_error)?;
    context
        .register_global_property(
            js_string!("__argsJson"),
            js_string!(serde_json::to_string(args)?),
            boa_engine::property::Attribute::READONLY
                | boa_engine::property::Attribute::NON_ENUMERABLE,
        )
        .map_err(js_error)?;
    context
        .eval(Source::from_bytes(BOOTSTRAP))
        .map_err(js_error)?;

    let wrapped = wrap_script(script);
    let value = context
        .eval(Source::from_bytes(&wrapped))
        .map_err(js_error)?;
    let object = value
        .as_object()
        .ok_or_else(|| WorkflowError::JavaScript("workflow did not return a Promise".to_owned()))?;
    let promise = JsPromise::from_object(object).map_err(js_error)?;
    context.run_jobs().map_err(js_error)?;
    match promise.state() {
        PromiseState::Fulfilled(value) => {
            let text = value
                .to_string(&mut context)
                .map_err(js_error)?
                .to_std_string_escaped();
            serde_json::from_str(&text).map_err(WorkflowError::Json)
        }
        PromiseState::Rejected(value) => {
            let text = value
                .to_string(&mut context)
                .map_err(js_error)?
                .to_std_string_escaped();
            Err(WorkflowError::JavaScript(text))
        }
        PromiseState::Pending => Err(WorkflowError::Invariant(
            "workflow Promise remained pending".to_owned(),
        )),
    }
}

async fn host_agent(
    _this: &JsValue,
    args: &[JsValue],
    context: &RefCell<&mut Context>,
) -> JsResult<JsValue> {
    let (prompt, options_json, host) = {
        let context = &mut context.borrow_mut();
        let prompt = js_string_arg(args, 0, context)?;
        let options = js_string_arg(args, 1, context)?;
        let host = context
            .get_data::<HostState>()
            .cloned()
            .ok_or_else(|| JsNativeError::error().with_message("workflow host is missing"))?;
        (prompt, options, host)
    };
    let options: AgentOptions = serde_json::from_str(&options_json).map_err(native_error)?;
    let input = json!({"prompt": prompt, "options": options});
    let key = host.key("agent", &input).map_err(native_error)?;
    let phase = host
        .phase
        .lock()
        .map_err(|_| native_error("phase lock poisoned"))?
        .clone();
    let receiver = host.runtime.agent(key.clone(), prompt, options, phase);
    let result = receiver
        .await
        .map_err(|_| native_error("agent worker dropped"))?
        .map_err(native_error)?;
    host.remember_journal_key(&key).map_err(native_error)?;
    Ok(JsValue::from(js_string!(
        serde_json::to_string(&result).map_err(native_error)?
    )))
}

async fn host_command(
    _this: &JsValue,
    args: &[JsValue],
    context: &RefCell<&mut Context>,
) -> JsResult<JsValue> {
    let (program, argv_json, options_json, host) = {
        let context = &mut context.borrow_mut();
        let host = context
            .get_data::<HostState>()
            .cloned()
            .ok_or_else(|| JsNativeError::error().with_message("workflow host is missing"))?;
        (
            js_string_arg(args, 0, context)?,
            js_string_arg(args, 1, context)?,
            js_string_arg(args, 2, context)?,
            host,
        )
    };
    let argv: Vec<String> = serde_json::from_str(&argv_json).map_err(native_error)?;
    let options: CommandOptions = serde_json::from_str(&options_json).map_err(native_error)?;
    let input = json!({"program": program, "args": argv, "options": options});
    let key = host.key("command", &input).map_err(native_error)?;
    let phase = host
        .phase
        .lock()
        .map_err(|_| native_error("phase lock poisoned"))?
        .clone();
    let receiver = host
        .runtime
        .command(key.clone(), program, argv, options, phase);
    let result = receiver
        .await
        .map_err(|_| native_error("command worker dropped"))?
        .map_err(native_error)?;
    host.remember_journal_key(&key).map_err(native_error)?;
    Ok(JsValue::from(js_string!(
        serde_json::to_string(&result).map_err(native_error)?
    )))
}

#[derive(Debug, Deserialize, Default)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct WorkflowOptions {}

async fn host_workflow(
    _this: &JsValue,
    args: &[JsValue],
    context: &RefCell<&mut Context>,
) -> JsResult<JsValue> {
    let (path_text, args_json, options_json, host) = {
        let context = &mut context.borrow_mut();
        let host = context
            .get_data::<HostState>()
            .cloned()
            .ok_or_else(|| JsNativeError::error().with_message("workflow host is missing"))?;
        (
            js_string_arg(args, 0, context)?,
            js_string_arg(args, 1, context)?,
            js_string_arg(args, 2, context)?,
            host,
        )
    };
    let child_args: Value = serde_json::from_str(&args_json).map_err(native_error)?;
    let _: WorkflowOptions = serde_json::from_str(&options_json).map_err(native_error)?;
    let path = std::path::PathBuf::from(&path_text);
    let path = if path.is_absolute() {
        path
    } else {
        host.runtime.cwd.join(path)
    };
    let input = json!({"path": path, "args": child_args});
    let key = host.key("workflow", &input).map_err(native_error)?;
    let existing = host
        .runtime
        .store
        .journal_index(&host.runtime.run_id)
        .map_err(native_error)?
        .remove(&key);
    if let Some(entry) = existing.as_ref() {
        match entry.state {
            CallState::Succeeded => {
                return Ok(JsValue::from(js_string!(
                    serde_json::to_string(&entry.result.clone().unwrap_or(Value::Null))
                        .map_err(native_error)?
                )));
            }
            CallState::Failed | CallState::Cancelled => {
                return Err(native_error(
                    entry
                        .error
                        .clone()
                        .unwrap_or_else(|| "child workflow failed".to_owned()),
                ));
            }
            CallState::Submitted => {}
        }
    }
    let engine = crate::engine::Engine::from_shared(
        Arc::clone(&host.runtime.store),
        Arc::clone(&host.runtime.transport),
    );
    let child = engine
        .prepare_child(&host.runtime.run_id, &key, &path, child_args)
        .map_err(native_error)?;
    if existing.is_none() {
        host.runtime
            .store
            .append(
                &host.runtime.run_id,
                &JournalEntry {
                    at: Utc::now(),
                    key: key.clone(),
                    kind: CallKind::Workflow,
                    state: CallState::Submitted,
                    label: path.display().to_string(),
                    result: None,
                    error: None,
                    transport_run_id: None,
                    child_run_id: Some(child.run_id.clone()),
                    phase: host
                        .phase
                        .lock()
                        .map_err(|_| native_error("phase lock poisoned"))?
                        .clone(),
                    duration_ms: None,
                    usage: None,
                    schema_correction: None,
                },
            )
            .map_err(native_error)?;
    }
    let child_run_id = child.run_id.clone();
    let child_state = host
        .runtime
        .store
        .load_state(&child_run_id)
        .map_err(native_error)?;
    let state = if child_state.status.is_terminal() {
        child_state
    } else {
        let scheduler = Arc::clone(&host.runtime.scheduler);
        let (sender, receiver) = futures_channel::oneshot::channel();
        thread::spawn(move || {
            let result = engine.execute_child(&child_run_id, scheduler);
            let _ = sender.send(result);
        });
        receiver
            .await
            .map_err(|_| native_error("child workflow worker dropped"))?
            .map_err(native_error)?
    };
    let (call_state, result, error) = match state.status {
        RunStatus::Succeeded => (
            CallState::Succeeded,
            state.result.unwrap_or(Value::Null),
            None,
        ),
        RunStatus::WaitingHuman => {
            let gate = state
                .waiting_gate
                .clone()
                .ok_or_else(|| native_error("waiting child has no gate"))?;
            let origin_run_id = gate
                .origin_run_id
                .clone()
                .unwrap_or_else(|| child.run_id.clone());
            let bubbled = GateRequest {
                origin_run_id: Some(origin_run_id.clone()),
                ..gate
            };
            host.runtime
                .transition(
                    WorkflowEvent::GateOpened {
                        key: bubbled.key.clone(),
                        origin_run_id: Some(origin_run_id),
                        label: bubbled.label.clone(),
                        question: bubbled.question.clone(),
                        expect: bubbled.expect.clone(),
                        current: bubbled.current.clone(),
                        hint: bubbled.hint.clone(),
                    },
                    |state| {
                        state.status = RunStatus::WaitingHuman;
                        state.waiting_gate = Some(bubbled);
                    },
                )
                .map_err(native_error)?;
            return Err(native_error("child workflow is waiting for human input"));
        }
        RunStatus::Paused | RunStatus::Pausing => {
            return Err(native_error("child workflow is paused"));
        }
        _ => (
            CallState::Failed,
            Value::Null,
            Some(
                state
                    .error
                    .unwrap_or_else(|| "child workflow failed".to_owned()),
            ),
        ),
    };
    host.runtime
        .store
        .append(
            &host.runtime.run_id,
            &JournalEntry {
                at: Utc::now(),
                key: key.clone(),
                kind: CallKind::Workflow,
                state: call_state.clone(),
                label: path.display().to_string(),
                result: (call_state == CallState::Succeeded).then_some(result.clone()),
                error: error.clone(),
                transport_run_id: None,
                child_run_id: Some(child.run_id),
                phase: host
                    .phase
                    .lock()
                    .map_err(|_| native_error("phase lock poisoned"))?
                    .clone(),
                duration_ms: None,
                usage: None,
                schema_correction: None,
            },
        )
        .map_err(native_error)?;
    if let Some(budget) = host.budget.as_ref() {
        budget.settle(&key, None, 0).map_err(native_error)?;
    }
    host.remember_journal_key(&key).map_err(native_error)?;
    match error {
        Some(error) => Err(native_error(error)),
        None => Ok(JsValue::from(js_string!(
            serde_json::to_string(&result).map_err(native_error)?
        ))),
    }
}

async fn host_gate(
    _this: &JsValue,
    args: &[JsValue],
    context: &RefCell<&mut Context>,
) -> JsResult<JsValue> {
    let (question, options_json, host) = {
        let context = &mut context.borrow_mut();
        let host = context
            .get_data::<HostState>()
            .cloned()
            .ok_or_else(|| JsNativeError::error().with_message("workflow host is missing"))?;
        (
            js_string_arg(args, 0, context)?,
            js_string_arg(args, 1, context)?,
            host,
        )
    };
    let options: GateOptions = serde_json::from_str(&options_json).map_err(native_error)?;
    let label = options.label.clone().unwrap_or_else(|| question.clone());
    let input = json!({"question": question, "label": label});
    let key = host.key("gate", &input).map_err(native_error)?;
    if let Some(budget) = host.budget.as_ref() {
        // Opening a gate is the completed host call; the later human decision
        // must not retain a budget reservation while the run is paused.
        budget.settle(&key, None, 0).map_err(native_error)?;
    }
    let state = host
        .runtime
        .store
        .load_state(&host.runtime.run_id)
        .map_err(native_error)?;
    if let Some(decision) = state.decisions.get(&key) {
        let result = json!({
            "approved": decision.approved,
            "reason": decision.reason,
            "value": decision.value,
        });
        return Ok(JsValue::from(js_string!(
            serde_json::to_string(&result).map_err(native_error)?
        )));
    }
    let request = GateRequest {
        key,
        origin_run_id: None,
        label,
        question,
        expect: options.expect.clone(),
        current: options.current.clone(),
        hint: options.hint.clone(),
    };
    host.runtime
        .transition(
            WorkflowEvent::GateOpened {
                key: request.key.clone(),
                origin_run_id: request.origin_run_id.clone(),
                label: request.label.clone(),
                question: request.question.clone(),
                expect: request.expect.clone(),
                current: request.current.clone(),
                hint: request.hint.clone(),
            },
            |state| {
                state.status = RunStatus::WaitingHuman;
                state.waiting_gate = Some(request);
            },
        )
        .map_err(native_error)?;
    Err(JsNativeError::error()
        .with_message("workflow is waiting for human input")
        .into())
}

#[derive(Debug, Deserialize, Default)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct SupersedeOptions {
    reason: String,
    #[serde(default)]
    evidence: Option<String>,
    #[serde(default)]
    new_contract: Option<String>,
}

async fn host_supersede(
    _this: &JsValue,
    args: &[JsValue],
    context: &RefCell<&mut Context>,
) -> JsResult<JsValue> {
    let (options_json, host) = {
        let context = &mut context.borrow_mut();
        let host = context
            .get_data::<HostState>()
            .cloned()
            .ok_or_else(|| JsNativeError::error().with_message("workflow host is missing"))?;
        (js_string_arg(args, 0, context)?, host)
    };
    let options: SupersedeOptions = serde_json::from_str(&options_json).map_err(native_error)?;
    if options.reason.trim().is_empty() {
        return Err(native_error("supersede reason is required"));
    }
    host.runtime
        .store
        .request_cancel(&host.runtime.run_id)
        .map_err(native_error)?;
    host.runtime
        .transition(
            WorkflowEvent::RunSuperseded {
                reason: options.reason.clone(),
                evidence: options.evidence.clone(),
                new_contract: options.new_contract.clone(),
            },
            |state| {
                state.status = RunStatus::Superseded;
                state.active.clear();
                state.waiting_gate = None;
                state.supersede = Some(crate::model::SupersedeInfo {
                    reason: options.reason.clone(),
                    evidence: options.evidence.clone(),
                    new_contract: options.new_contract.clone(),
                    decided_at: chrono::Utc::now(),
                });
            },
        )
        .map_err(native_error)?;
    Err(JsNativeError::error()
        .with_message(format!("workflow superseded: {}", options.reason))
        .into())
}

fn host_phase(_this: &JsValue, args: &[JsValue], context: &mut Context) -> JsResult<JsValue> {
    let name = js_string_arg(args, 0, context)?;
    let host = context
        .get_data::<HostState>()
        .cloned()
        .ok_or_else(|| JsNativeError::error().with_message("workflow host is missing"))?;
    let persisted_name = name.clone();
    host.runtime
        .transition(
            WorkflowEvent::PhaseChanged {
                phase: name.clone(),
            },
            |state| state.phase = Some(persisted_name),
        )
        .map_err(native_error)?;
    *host
        .phase
        .lock()
        .map_err(|_| native_error("phase lock poisoned"))? = Some(name);
    Ok(JsValue::undefined())
}

fn host_sleep(_this: &JsValue, args: &[JsValue], context: &mut Context) -> JsResult<JsValue> {
    let ms = args
        .get_or_undefined(0)
        .to_number(context)
        .unwrap_or(0.0)
        .max(0.0) as u64;
    // Cap single sleep to 60s to avoid runaway host stalls from bad scripts.
    let ms = ms.min(60_000);
    std::thread::sleep(std::time::Duration::from_millis(ms));
    Ok(JsValue::undefined())
}

// __log appends a timestamped plain-text line to workflow.log in the run record
// dir. Narration must NOT enter the journal (journal entries replay on resume)
// and must not touch RunState.phase. It is intentionally infallible from the
// script's perspective: any IO failure is swallowed and never propagated, so a
// log() write failure can never break the workflow.
fn host_log(_this: &JsValue, args: &[JsValue], context: &mut Context) -> JsResult<JsValue> {
    let host = context.get_data::<HostState>().cloned();
    let message = match js_string_arg(args, 0, context) {
        Ok(text) => text,
        Err(_) => return Ok(JsValue::undefined()),
    };
    if let Some(host) = host {
        let dir = host.runtime.store.run_dir(&host.runtime.run_id);
        let _ = std::fs::create_dir_all(&dir);
        let path = dir.join("workflow.log");
        // Resume idempotency: a resumed run re-executes the script from the
        // top, so log() calls that already appended a line in a previous
        // execution must not append again. Deterministic scripts (the same
        // assumption journal replay relies on) emit log() calls in the same
        // order, so skipping the first `existing` occurrences — where
        // `existing` is the line count of workflow.log at VM start — makes
        // replayed calls no-ops while new calls append as normal.
        {
            let mut replay = host.log_replay.lock().expect("log replay lock");
            let (existing, calls) = replay.get_or_insert_with(|| {
                let existing = std::fs::read_to_string(&path)
                    .map(|text| text.lines().count())
                    .unwrap_or(0);
                (existing, 0)
            });
            *calls += 1;
            if *calls <= *existing {
                return Ok(JsValue::undefined());
            }
        }
        let line = format!("[{}] {}\n", chrono::Utc::now().to_rfc3339(), message);
        if let Ok(mut file) = OpenOptions::new().create(true).append(true).open(&path) {
            let _ = file.write_all(line.as_bytes());
            let _ = file.sync_data();
        }
    }
    Ok(JsValue::undefined())
}

fn js_string_arg(args: &[JsValue], index: usize, context: &mut Context) -> JsResult<String> {
    Ok(args
        .get_or_undefined(index)
        .to_string(context)?
        .to_std_string_escaped())
}
fn native_error(error: impl ToString) -> boa_engine::JsError {
    JsNativeError::error()
        .with_message(error.to_string())
        .into()
}
fn js_error(error: boa_engine::JsError) -> WorkflowError {
    WorkflowError::JavaScript(error.to_string())
}
