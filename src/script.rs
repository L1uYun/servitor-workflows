use crate::agent::AgentOptions;
use crate::command::CommandOptions;
use crate::error::WorkflowError;
use crate::model::{GateRequest, RunStatus};
use crate::scheduler::{RuntimeHost, call_key};
use boa_engine::{
    Context, JsArgs, JsNativeError, JsResult, JsValue, NativeFunction, Source,
    builtins::promise::PromiseState, js_string, object::JsData, object::builtins::JsPromise,
};
use boa_gc::{Finalize, Trace};
use serde::Deserialize;
use serde_json::{Value, json};
use std::cell::RefCell;
use std::collections::BTreeMap;
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
globalThis.phase = name => __phase(String(name));
globalThis.parallel = promises => Promise.all(promises);
globalThis.pipeline = (items, worker) => Promise.all(Array.from(items, worker));
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
        let mut calls = self
            .calls
            .lock()
            .map_err(|_| "call counter lock poisoned".to_owned())?;
        *calls += 1;
        if *calls > self.max_calls {
            return Err(format!("workflow exceeded max_calls={}", self.max_calls));
        }
        Ok(key)
    }
}

#[derive(Debug, Deserialize, Default)]
#[serde(deny_unknown_fields)]
struct GateOptions {
    #[serde(default)]
    label: Option<String>,
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
    context.insert_data(HostState {
        runtime,
        occurrences: Arc::new(Mutex::new(BTreeMap::new())),
        calls: Arc::new(Mutex::new(0)),
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
            js_string!("__phase"),
            1,
            NativeFunction::from_fn_ptr(host_phase),
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

    let body = script.replacen("export const meta", "const meta", 1);
    let wrapped = format!(
        "(async () => {{ const __result = await (async () => {{ {body} }})(); return JSON.stringify(__result ?? null); }})()"
    );
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
    let receiver = host.runtime.agent(key, prompt, options);
    let result = receiver
        .await
        .map_err(|_| native_error("agent worker dropped"))?
        .map_err(native_error)?;
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
    let receiver = host.runtime.command(key, program, argv, options);
    let result = receiver
        .await
        .map_err(|_| native_error("command worker dropped"))?
        .map_err(native_error)?;
    Ok(JsValue::from(js_string!(
        serde_json::to_string(&result).map_err(native_error)?
    )))
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
    let label = options.label.unwrap_or_else(|| question.clone());
    let input = json!({"question": question, "label": label});
    let key = host.key("gate", &input).map_err(native_error)?;
    let state = host
        .runtime
        .store
        .load_state(&host.runtime.run_id)
        .map_err(native_error)?;
    if let Some(decision) = state.decisions.get(&key) {
        let result = json!({"approved": decision.approved, "reason": decision.reason});
        return Ok(JsValue::from(js_string!(
            serde_json::to_string(&result).map_err(native_error)?
        )));
    }
    host.runtime
        .store
        .update_state(&host.runtime.run_id, |state| {
            state.status = RunStatus::WaitingHuman;
            state.waiting_gate = Some(GateRequest {
                key,
                label,
                question,
            });
        })
        .map_err(native_error)?;
    Err(JsNativeError::error()
        .with_message("workflow is waiting for human input")
        .into())
}

fn host_phase(_this: &JsValue, args: &[JsValue], context: &mut Context) -> JsResult<JsValue> {
    let name = js_string_arg(args, 0, context)?;
    let host = context
        .get_data::<HostState>()
        .cloned()
        .ok_or_else(|| JsNativeError::error().with_message("workflow host is missing"))?;
    host.runtime
        .store
        .update_state(&host.runtime.run_id, |state| {
            state.phase = Some(name);
        })
        .map_err(native_error)?;
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
