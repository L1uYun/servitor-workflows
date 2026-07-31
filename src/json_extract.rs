//! Extract a JSON value from free-form model text.
//!
//! Algorithm matches the common LLM-output pattern used by tools such as
//! `json-from-llm` and LangChain's markdown JSON parsers:
//! 1. strip reasoning / thinking wrappers
//! 2. prefer fenced ```json / untagged ``` blocks
//! 3. scan string-aware balanced `{...}` / `[...]` spans
//! 4. parse candidates, with a single trailing-comma repair pass
//! 5. optionally require object or array when the contract says so
//! 6. when a schema is provided, select among candidates by schema validity
//!    (last valid wins — final-answer convention; no provider-specific branches)

use serde_json::Value;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Expect {
    Any,
    Object,
    Array,
}

/// Enumerate parseable JSON values in discovery order (deduped by text).
///
/// Discovery order: whole cleaned text, fenced blocks (+ nested spans), then
/// document-order balanced spans. Shape filtering uses `expect` only.
pub fn extract_json_values(text: &str, expect: Expect) -> Result<Vec<Value>, String> {
    if text.trim().is_empty() {
        return Err("empty agent output".to_owned());
    }

    let cleaned = strip_reasoning(text);
    let mut candidates: Vec<String> = Vec::new();

    // Whole-text first so a pure JSON body still wins when it is the only candidate.
    candidates.push(cleaned.trim().to_owned());

    let fences = fenced_blocks(&cleaned);
    candidates.extend(fences.iter().cloned());
    for fence in &fences {
        candidates.extend(balanced_spans(fence));
    }
    candidates.extend(balanced_spans(&cleaned));

    let mut last_error = "no JSON value could be extracted".to_owned();
    let mut seen = std::collections::BTreeSet::new();
    let mut values = Vec::new();
    for candidate in candidates {
        let key = candidate.trim().to_owned();
        if key.is_empty() || !seen.insert(key.clone()) {
            continue;
        }
        match parse_candidate(&key) {
            Ok(value) if matches_expect(&value, expect) => values.push(value),
            Ok(_) => last_error = format!("JSON found but expected {expect:?}"),
            Err(error) => last_error = error,
        }
    }
    if values.is_empty() {
        Err(last_error)
    } else {
        Ok(values)
    }
}

/// First shape-matching JSON value (no schema). Prefer
/// [`extract_json_value_for_schema`] when a schema contract exists.
/// Test-only helper; production paths use schema-aware selection.
#[cfg(test)]
pub fn extract_json_value(text: &str, expect: Expect) -> Result<Value, String> {
    extract_json_values(text, expect).map(|mut values| values.remove(0))
}

/// Schema-aware extraction: schema is a **selection** criterion among candidates,
/// not a post-filter on the first shape match.
///
/// General rule (model-agnostic): among shape-matching candidates, keep those
/// that validate against `schema`; if several remain, take the **last** (final
/// answer convention for free-form agent text). No provider-specific branches.
pub fn extract_json_value_for_schema(text: &str, schema: &Value) -> Result<Value, String> {
    let expect = expect_from_schema(Some(schema));
    let candidates = extract_json_values(text, expect)?;
    let mut last_error = "no schema-valid JSON candidate".to_owned();
    let mut last_ok: Option<Value> = None;
    for value in candidates {
        match validate_value_against_schema(&value, schema, "$") {
            Ok(()) => last_ok = Some(value),
            Err(error) => last_error = error,
        }
    }
    last_ok.ok_or(last_error)
}

/// Deterministically validate the supported JSON Schema Draft 2020-12 subset.
///
/// This is deliberately local and provider-independent: a provider-reported success
/// is never enough to accept structured output. Unsupported assertion keywords and
/// remote references fail closed rather than becoming accidental no-ops.
pub fn validate_value_against_schema(
    value: &Value,
    schema: &Value,
    path: &str,
) -> Result<(), String> {
    Validator::new(schema)?.validate(value, schema, path, 0)
}

struct Validator<'a> {
    root: &'a Value,
}

impl<'a> Validator<'a> {
    fn new(root: &'a Value) -> Result<Self, String> {
        if !root.is_object() && !root.is_boolean() {
            return Err("schema must be an object or boolean".to_owned());
        }
        let validator = Self { root };
        validator.preflight_schema(root, 0)?;
        Ok(validator)
    }

    fn preflight_schema(&self, schema: &Value, depth: usize) -> Result<(), String> {
        if depth > 128 {
            return Err("schema validation recursion limit exceeded".to_owned());
        }
        let Value::Object(object) = schema else {
            return Ok(());
        };
        self.reject_unsupported_keywords(object, "$")?;
        if let Some(reference) = object.get("$ref") {
            self.resolve_local_ref(reference, "$")?;
        }
        if let Some(definitions) = object.get("$defs") {
            let definitions = definitions
                .as_object()
                .ok_or_else(|| "$: $defs must be an object".to_owned())?;
            for definition in definitions.values() {
                self.preflight_schema(definition, depth + 1)?;
            }
        }
        for keyword in ["allOf", "anyOf", "oneOf"] {
            if let Some(branches) = object.get(keyword) {
                for branch in schema_array(branches, keyword, "$")? {
                    self.preflight_schema(branch, depth + 1)?;
                }
            }
        }
        for keyword in ["not", "items", "additionalProperties"] {
            if let Some(child) = object.get(keyword)
                && (child.is_object() || child.is_boolean())
            {
                self.preflight_schema(child, depth + 1)?;
            }
        }
        if let Some(properties) = object.get("properties") {
            let properties = properties
                .as_object()
                .ok_or_else(|| "$: properties must be an object".to_owned())?;
            for child in properties.values() {
                self.preflight_schema(child, depth + 1)?;
            }
        }
        Ok(())
    }

    fn validate(
        &self,
        value: &Value,
        schema: &Value,
        path: &str,
        depth: usize,
    ) -> Result<(), String> {
        if depth > 128 {
            return Err(format!(
                "{path}: schema validation recursion limit exceeded"
            ));
        }
        match schema {
            Value::Bool(true) => Ok(()),
            Value::Bool(false) => Err(format!("{path} is rejected by false schema")),
            Value::Object(object) => self.validate_object(value, object, path, depth),
            _ => Err(format!("{path}: schema must be an object or boolean")),
        }
    }

    fn validate_object(
        &self,
        value: &Value,
        schema: &serde_json::Map<String, Value>,
        path: &str,
        depth: usize,
    ) -> Result<(), String> {
        self.reject_unsupported_keywords(schema, path)?;
        if let Some(definitions) = schema.get("$defs")
            && !definitions.is_object()
        {
            return Err(format!("{path}: $defs must be an object"));
        }
        if let Some(reference) = schema.get("$ref") {
            let target = self.resolve_local_ref(reference, path)?;
            self.validate(value, target, path, depth + 1)?;
        }
        if let Some(all_of) = schema.get("allOf") {
            for (index, branch) in schema_array(all_of, "allOf", path)?.iter().enumerate() {
                self.validate(value, branch, path, depth + 1)
                    .map_err(|error| format!("{path}: allOf[{index}] failed: {error}"))?;
            }
        }
        if let Some(any_of) = schema.get("anyOf") {
            let errors = schema_array(any_of, "anyOf", path)?
                .iter()
                .filter_map(|branch| self.validate(value, branch, path, depth + 1).err())
                .collect::<Vec<_>>();
            if errors.len() == schema_array(any_of, "anyOf", path)?.len() {
                return Err(format!(
                    "{path} must match at least one anyOf branch: {}",
                    errors.join("; ")
                ));
            }
        }
        if let Some(one_of) = schema.get("oneOf") {
            let branches = schema_array(one_of, "oneOf", path)?;
            let matches = branches
                .iter()
                .filter(|branch| self.validate(value, branch, path, depth + 1).is_ok())
                .count();
            if matches != 1 {
                return Err(format!(
                    "{path} must match exactly one oneOf branch (matched {matches})"
                ));
            }
        }
        if let Some(not) = schema.get("not")
            && self.validate(value, not, path, depth + 1).is_ok()
        {
            return Err(format!("{path} must not match not"));
        }
        if let Some(expected) = schema.get("type")
            && !matches_type(value, expected)?
        {
            return Err(format!("{path} must be {}", type_description(expected)?));
        }
        if let Some(expected) = schema.get("const")
            && value != expected
        {
            return Err(format!("{path} must equal const"));
        }
        if let Some(options) = schema.get("enum") {
            let options = options
                .as_array()
                .ok_or_else(|| format!("{path}: enum must be an array"))?;
            if !options.iter().any(|option| option == value) {
                return Err(format!("{path} must be one of enum values"));
            }
        }
        self.validate_string(value, schema, path)?;
        self.validate_number(value, schema, path)?;
        self.validate_object_instance(value, schema, path, depth)?;
        self.validate_array(value, schema, path, depth)?;
        Ok(())
    }

    fn validate_string(
        &self,
        value: &Value,
        schema: &serde_json::Map<String, Value>,
        path: &str,
    ) -> Result<(), String> {
        let Some(text) = value.as_str() else {
            return Ok(());
        };
        if let Some(minimum) = schema_usize(schema, "minLength", path)?
            && text.chars().count() < minimum
        {
            return Err(format!("{path} must have minLength {minimum}"));
        }
        if let Some(maximum) = schema_usize(schema, "maxLength", path)?
            && text.chars().count() > maximum
        {
            return Err(format!("{path} must have maxLength {maximum}"));
        }
        if let Some(pattern) = schema.get("pattern") {
            let pattern = pattern
                .as_str()
                .ok_or_else(|| format!("{path}: pattern must be a string"))?;
            let regex = regex::Regex::new(pattern)
                .map_err(|error| format!("{path}: invalid pattern {pattern:?}: {error}"))?;
            if !regex.is_match(text) {
                return Err(format!("{path} must match pattern {pattern:?}"));
            }
        }
        Ok(())
    }

    fn validate_number(
        &self,
        value: &Value,
        schema: &serde_json::Map<String, Value>,
        path: &str,
    ) -> Result<(), String> {
        let Some(number) = value.as_f64() else {
            return Ok(());
        };
        for (keyword, inclusive, lower) in [
            ("minimum", true, true),
            ("exclusiveMinimum", false, true),
            ("maximum", true, false),
            ("exclusiveMaximum", false, false),
        ] {
            let Some(limit) = schema.get(keyword) else {
                continue;
            };
            let limit = limit
                .as_f64()
                .ok_or_else(|| format!("{path}: {keyword} must be a number"))?;
            let valid = match (lower, inclusive) {
                (true, true) => number >= limit,
                (true, false) => number > limit,
                (false, true) => number <= limit,
                (false, false) => number < limit,
            };
            if !valid {
                return Err(format!("{path} violates {keyword} {limit}"));
            }
        }
        if let Some(multiple) = schema.get("multipleOf") {
            let multiple = multiple
                .as_f64()
                .ok_or_else(|| format!("{path}: multipleOf must be a number"))?;
            if multiple <= 0.0 || ((number / multiple).round() - number / multiple).abs() > 1e-12 {
                return Err(format!("{path} must be a multiple of {multiple}"));
            }
        }
        Ok(())
    }

    fn validate_object_instance(
        &self,
        value: &Value,
        schema: &serde_json::Map<String, Value>,
        path: &str,
        depth: usize,
    ) -> Result<(), String> {
        let Some(object) = value.as_object() else {
            return Ok(());
        };
        if let Some(required) = schema.get("required") {
            for name in schema_array(required, "required", path)? {
                let name = name
                    .as_str()
                    .ok_or_else(|| format!("{path}: required names must be strings"))?;
                if !object.contains_key(name) {
                    return Err(format!("{path}.{name} is required"));
                }
            }
        }
        if let Some(minimum) = schema_usize(schema, "minProperties", path)?
            && object.len() < minimum
        {
            return Err(format!("{path} must have minProperties {minimum}"));
        }
        if let Some(maximum) = schema_usize(schema, "maxProperties", path)?
            && object.len() > maximum
        {
            return Err(format!("{path} must have maxProperties {maximum}"));
        }
        let properties = schema
            .get("properties")
            .map(|value| {
                value
                    .as_object()
                    .ok_or_else(|| format!("{path}: properties must be an object"))
            })
            .transpose()?;
        if let Some(properties) = properties {
            for (name, child_schema) in properties {
                if let Some(child) = object.get(name) {
                    self.validate(
                        child,
                        child_schema,
                        &format!("{path}.{}", display_key(name)),
                        depth + 1,
                    )?;
                }
            }
        }
        let additional = schema.get("additionalProperties");
        if matches!(additional, Some(Value::Bool(false))) {
            for name in object.keys() {
                if !properties.is_some_and(|properties| properties.contains_key(name)) {
                    return Err(format!("{path}.{} is not allowed", display_key(name)));
                }
            }
        } else if let Some(extra_schema) = additional {
            if !extra_schema.is_boolean() && !extra_schema.is_object() {
                return Err(format!(
                    "{path}: additionalProperties must be a schema or boolean"
                ));
            }
            for (name, child) in object {
                if !properties.is_some_and(|properties| properties.contains_key(name)) {
                    self.validate(
                        child,
                        extra_schema,
                        &format!("{path}.{}", display_key(name)),
                        depth + 1,
                    )?;
                }
            }
        }
        Ok(())
    }

    fn validate_array(
        &self,
        value: &Value,
        schema: &serde_json::Map<String, Value>,
        path: &str,
        depth: usize,
    ) -> Result<(), String> {
        let Some(items) = value.as_array() else {
            return Ok(());
        };
        if let Some(minimum) = schema_usize(schema, "minItems", path)?
            && items.len() < minimum
        {
            return Err(format!("{path} must have minItems {minimum}"));
        }
        if let Some(maximum) = schema_usize(schema, "maxItems", path)?
            && items.len() > maximum
        {
            return Err(format!("{path} must have maxItems {maximum}"));
        }
        if let Some(unique) = schema.get("uniqueItems")
            && !unique.is_boolean()
        {
            return Err(format!("{path}: uniqueItems must be a boolean"));
        }
        if schema
            .get("uniqueItems")
            .is_some_and(Value::is_boolean_and_true)
        {
            for (index, item) in items.iter().enumerate() {
                if items[..index].iter().any(|previous| previous == item) {
                    return Err(format!("{path}[{index}] duplicates an earlier item"));
                }
            }
        }
        if let Some(item_schema) = schema.get("items") {
            for (index, item) in items.iter().enumerate() {
                self.validate(item, item_schema, &format!("{path}[{index}]"), depth + 1)?;
            }
        }
        Ok(())
    }

    fn resolve_local_ref(&self, reference: &Value, path: &str) -> Result<&'a Value, String> {
        let reference = reference
            .as_str()
            .ok_or_else(|| format!("{path}: $ref must be a string"))?;
        if !reference.starts_with('#') {
            return Err(format!("{path}: remote $ref is forbidden"));
        }
        let pointer = reference.strip_prefix('#').expect("starts_with checked");
        if pointer.is_empty() {
            return Ok(self.root);
        }
        if !pointer.starts_with('/') {
            return Err(format!("{path}: local $ref must use a JSON Pointer"));
        }
        self.root
            .pointer(pointer)
            .ok_or_else(|| format!("{path}: unresolved local $ref {reference}"))
    }

    fn reject_unsupported_keywords(
        &self,
        schema: &serde_json::Map<String, Value>,
        path: &str,
    ) -> Result<(), String> {
        const SUPPORTED: &[&str] = &[
            "$schema",
            "$id",
            "$defs",
            "$ref",
            "title",
            "description",
            "default",
            "examples",
            "type",
            "const",
            "enum",
            "allOf",
            "anyOf",
            "oneOf",
            "not",
            "properties",
            "required",
            "additionalProperties",
            "minProperties",
            "maxProperties",
            "items",
            "minItems",
            "maxItems",
            "uniqueItems",
            "minLength",
            "maxLength",
            "pattern",
            "minimum",
            "maximum",
            "exclusiveMinimum",
            "exclusiveMaximum",
            "multipleOf",
        ];
        for keyword in schema.keys() {
            if !SUPPORTED.contains(&keyword.as_str()) {
                return Err(format!("{path}: unsupported schema keyword {keyword}"));
            }
        }
        Ok(())
    }
}

trait BoolValueExt {
    fn is_boolean_and_true(&self) -> bool;
}

impl BoolValueExt for Value {
    fn is_boolean_and_true(&self) -> bool {
        self.as_bool() == Some(true)
    }
}

fn schema_array<'a>(value: &'a Value, keyword: &str, path: &str) -> Result<&'a Vec<Value>, String> {
    value
        .as_array()
        .ok_or_else(|| format!("{path}: {keyword} must be an array"))
}

fn schema_usize(
    schema: &serde_json::Map<String, Value>,
    keyword: &str,
    path: &str,
) -> Result<Option<usize>, String> {
    let Some(value) = schema.get(keyword) else {
        return Ok(None);
    };
    value
        .as_u64()
        .and_then(|value| usize::try_from(value).ok())
        .map(Some)
        .ok_or_else(|| format!("{path}: {keyword} must be a non-negative integer"))
}

fn matches_type(value: &Value, expected: &Value) -> Result<bool, String> {
    match expected {
        Value::String(kind) => matches_type_name(value, kind),
        Value::Array(kinds) => {
            if kinds.is_empty() {
                return Err("schema type array must not be empty".to_owned());
            }
            let mut matched = false;
            for kind in kinds {
                matched |= matches_type_name(
                    value,
                    kind.as_str().ok_or("schema type values must be strings")?,
                )?;
            }
            Ok(matched)
        }
        _ => Err("schema type must be a string or array of strings".to_owned()),
    }
}

fn type_description(expected: &Value) -> Result<String, String> {
    match expected {
        Value::String(kind) => {
            matches_type_name(&Value::Null, kind)?;
            Ok(kind.clone())
        }
        Value::Array(kinds) => Ok(kinds
            .iter()
            .map(|kind| {
                kind.as_str()
                    .ok_or("schema type values must be strings")
                    .map(str::to_owned)
            })
            .collect::<Result<Vec<_>, _>>()?
            .join(" or ")),
        _ => Err("schema type must be a string or array of strings".to_owned()),
    }
}

fn matches_type_name(value: &Value, kind: &str) -> Result<bool, String> {
    Ok(match kind {
        "object" => value.is_object(),
        "array" => value.is_array(),
        "string" => value.is_string(),
        "number" => value.is_number(),
        "integer" => {
            value.as_i64().is_some()
                || value.as_u64().is_some()
                || value
                    .as_f64()
                    .is_some_and(|number| number.is_finite() && number.fract() == 0.0)
        }
        "boolean" => value.is_boolean(),
        "null" => value.is_null(),
        other => return Err(format!("unsupported schema type {other}")),
    })
}

fn display_key(name: &str) -> String {
    if name
        .chars()
        .all(|character| character.is_ascii_alphanumeric() || character == '_')
    {
        name.to_owned()
    } else {
        format!("[{name:?}]")
    }
}

fn parse_candidate(candidate: &str) -> Result<Value, String> {
    match serde_json::from_str::<Value>(candidate) {
        Ok(value) => Ok(value),
        Err(first) => {
            let repaired = remove_trailing_commas(candidate);
            serde_json::from_str::<Value>(&repaired).map_err(|_| first.to_string())
        }
    }
}

fn matches_expect(value: &Value, expect: Expect) -> bool {
    match expect {
        Expect::Any => true,
        Expect::Object => value.is_object(),
        Expect::Array => value.is_array(),
    }
}

/// Strip closed and unclosed model reasoning wrappers.
/// Unclosed reasoning is discarded from the open tag to the end — safer than
/// mistaking brace-heavy chain-of-thought for the payload.
pub fn strip_reasoning(text: &str) -> String {
    let mut out = text.to_owned();
    // Closed blocks first.
    for (open, close) in [
        ("</think>", ""), // already-closed marker sometimes left as a prefix
        ("<think>", "</think>"),
        ("<thinking>", "</thinking>"),
        ("<reasoning>", "</reasoning>"),
        ("<thought>", "</thought>"),
    ] {
        if close.is_empty() {
            // bare trailing marker prefix
            if let Some(idx) = out.find(open) {
                // only strip when it is a leading-ish artifact
                if out[..idx].trim().is_empty() {
                    out = out[idx + open.len()..].to_owned();
                }
            }
            continue;
        }
        while let Some(start) = find_ascii_tag(&out, open) {
            if let Some(rel_end) = out[start + open.len()..].find(close) {
                let end = start + open.len() + rel_end + close.len();
                out.replace_range(start..end, "");
            } else {
                out.truncate(start);
                break;
            }
        }
    }
    out
}

fn find_ascii_tag(haystack: &str, tag: &str) -> Option<usize> {
    haystack
        .to_ascii_lowercase()
        .find(&tag.to_ascii_lowercase())
}

/// Inner contents of ```json / ```jsonc / untagged ``` fences.
pub fn fenced_blocks(text: &str) -> Vec<String> {
    let mut blocks = Vec::new();
    let bytes = text.as_bytes();
    let mut i = 0;
    while i + 3 <= bytes.len() {
        if &bytes[i..i + 3] != b"```" {
            i += 1;
            continue;
        }
        let after_open = i + 3;
        // language tag on the same line
        let mut j = after_open;
        while j < bytes.len() && bytes[j] != b'\n' && bytes[j] != b'\r' {
            j += 1;
        }
        let lang = text[after_open..j].trim().to_ascii_lowercase();
        // skip newline after lang
        if j < bytes.len() && bytes[j] == b'\r' {
            j += 1;
        }
        if j < bytes.len() && bytes[j] == b'\n' {
            j += 1;
        }
        // find closing fence
        if let Some(rel) = text[j..].find("```") {
            let content = text[j..j + rel].trim();
            let lang_ok = lang.is_empty() || lang.contains("json");
            if !content.is_empty() && lang_ok {
                blocks.push(content.to_owned());
            }
            i = j + rel + 3;
        } else {
            break;
        }
    }
    blocks
}

/// Complete balanced object/array spans, string-aware, document order.
pub fn balanced_spans(text: &str) -> Vec<String> {
    let bytes = text.as_bytes();
    let mut spans = Vec::new();
    let mut i = 0;
    while i < bytes.len() {
        let open = bytes[i];
        if open != b'{' && open != b'[' {
            i += 1;
            continue;
        }
        match match_balanced(bytes, i) {
            Some(end) => {
                spans.push(text[i..end].to_owned());
                i = end;
            }
            None => i += 1,
        }
    }
    spans
}

fn match_balanced(bytes: &[u8], start: usize) -> Option<usize> {
    let mut stack: Vec<u8> = Vec::new();
    let mut in_string = false;
    let mut escaped = false;
    for (offset, &ch) in bytes[start..].iter().enumerate() {
        if in_string {
            if escaped {
                escaped = false;
            } else if ch == b'\\' {
                escaped = true;
            } else if ch == b'"' {
                in_string = false;
            }
            continue;
        }
        match ch {
            b'"' => in_string = true,
            b'{' => stack.push(b'}'),
            b'[' => stack.push(b']'),
            b'}' | b']' => match stack.pop() {
                Some(expected) if expected == ch => {
                    if stack.is_empty() {
                        return Some(start + offset + 1);
                    }
                }
                _ => return None,
            },
            _ => {}
        }
    }
    None
}

/// Drop trailing commas before `}` / `]` outside strings.
///
/// Iterates by `char`, not by byte. A byte+`as char` loop re-encodes each
/// UTF-8 continuation byte as a Latin-1 codepoint when pushed back into the
/// `String`, silently corrupting non-ASCII string values while still removing
/// the comma — the repaired text then parsed as valid JSON with a garbled
/// value, so schema validation passed and the correction retry never fired.
pub fn remove_trailing_commas(input: &str) -> String {
    let chars: Vec<char> = input.chars().collect();
    let mut out = String::with_capacity(input.len());
    let mut in_string = false;
    let mut escaped = false;
    let mut i = 0;
    while i < chars.len() {
        let ch = chars[i];
        if in_string {
            out.push(ch);
            if escaped {
                escaped = false;
            } else if ch == '\\' {
                escaped = true;
            } else if ch == '"' {
                in_string = false;
            }
            i += 1;
            continue;
        }
        if ch == '"' {
            in_string = true;
            out.push(ch);
            i += 1;
            continue;
        }
        if ch == ',' {
            let mut j = i + 1;
            while j < chars.len() && matches!(chars[j], ' ' | '\n' | '\r' | '\t') {
                j += 1;
            }
            if j < chars.len() && matches!(chars[j], '}' | ']') {
                i += 1;
                continue;
            }
        }
        out.push(ch);
        i += 1;
    }
    out
}

pub fn expect_from_schema(schema: Option<&Value>) -> Expect {
    match schema.and_then(|s| s.get("type")).and_then(Value::as_str) {
        Some("object") => Expect::Object,
        Some("array") => Expect::Array,
        _ => Expect::Any,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn extracts_fenced_json_from_prose() {
        let text = r#"[T#1] done

```json
{"summary":"ok","report":"D:/a.html"}
```
"#;
        let value = extract_json_value(text, Expect::Object).expect("extract");
        assert_eq!(value, json!({"summary":"ok","report":"D:/a.html"}));
    }

    #[test]
    fn strips_thinking_then_extracts() {
        let text = r#"<think>{"draft":1}</think>
{"ok":true}
"#;
        let value = extract_json_value(text, Expect::Object).expect("extract");
        assert_eq!(value, json!({"ok": true}));
    }

    #[test]
    fn repairs_trailing_commas() {
        let text = r#"{"a":1,"b":[2,3,],}"#;
        let value = extract_json_value(text, Expect::Object).expect("extract");
        assert_eq!(value, json!({"a":1,"b":[2,3]}));
    }

    #[test]
    fn repairs_trailing_commas_without_corrupting_non_ascii() {
        // Regression: the byte+`as char` loop re-encoded UTF-8 continuation
        // bytes as Latin-1, garbling 中文 string values while still dropping
        // the comma — so the repaired text parsed as valid JSON with a corrupt
        // value and schema validation passed, never triggering the correction
        // retry. This shape is the intro-html workflow's actual output.
        let text = r#"{"html": "<p>你好，世界</p>",}"#;
        let value = extract_json_value(text, Expect::Object).expect("extract");
        assert_eq!(value["html"], json!("<p>你好，世界</p>"));
        // Comma inside a string must NOT be treated as a trailing comma.
        let text2 = r#"{"a": "x,y",}"#;
        let value2 = extract_json_value(text2, Expect::Object).expect("extract");
        assert_eq!(value2["a"], json!("x,y"));
    }

    #[test]
    fn prefers_object_when_expected() {
        let text = r#"notes [1,2,3] and then {"summary":"x","report":"y"}"#;
        let value = extract_json_value(text, Expect::Object).expect("extract");
        assert_eq!(value, json!({"summary":"x","report":"y"}));
    }

    #[test]
    fn real_pi_prose_with_fenced_delivery() {
        let text = r#"[T#1] 流云，两份交付物已落盘并核验通过。

```json
{"summary":"SAG ADVANCE","report":"D:/AgentWork/surveys/a.html"}
```
"#;
        let value = extract_json_value(text, Expect::Object).expect("extract");
        assert_eq!(value["summary"], "SAG ADVANCE");
        assert_eq!(value["report"], "D:/AgentWork/surveys/a.html");
    }

    #[test]
    fn rejects_wrong_shape() {
        let text = r#"[1,2,3]"#;
        let err = extract_json_value(text, Expect::Object).unwrap_err();
        assert!(err.contains("expected Object"));
    }

    #[test]
    fn schema_selects_last_valid_among_multiple_objects() {
        // Intermediate protocol/tool JSON is common in free-form agent transcripts.
        // Schema must select among candidates, not fail on the first object.
        let text = r#"progress {"type":"task_update","id":1}
still working {"status":"partial"}
{"summary":"done","evidence":"D:/tmp/evidence.json"}
"#;
        let schema = json!({
            "type": "object",
            "required": ["summary"],
            "properties": {
                "summary": {"type": "string"},
                "evidence": {"type": "string"}
            }
        });
        let value = extract_json_value_for_schema(text, &schema).expect("schema extract");
        assert_eq!(value["summary"], "done");
        assert_eq!(value["evidence"], "D:/tmp/evidence.json");
    }

    #[test]
    fn schema_rejects_when_no_candidate_valid() {
        let text = r#"{"type":"task_update","id":1} {"status":"partial"}"#;
        let schema = json!({
            "type": "object",
            "required": ["summary"],
            "properties": { "summary": {"type": "string"} }
        });
        let err = extract_json_value_for_schema(text, &schema).unwrap_err();
        assert!(
            err.contains("summary") || err.contains("required"),
            "err={err}"
        );
    }

    #[test]
    fn validates_strict_tagged_union_with_local_refs() {
        let schema = json!({
            "$defs": {
                "email": {
                    "type": "object",
                    "required": ["kind", "address"],
                    "properties": {
                        "kind": {"const": "email"},
                        "address": {"type": "string", "pattern": "^[^@]+@[^@]+$"}
                    },
                    "additionalProperties": false
                },
                "sms": {
                    "type": "object",
                    "required": ["kind", "number"],
                    "properties": {
                        "kind": {"const": "sms"},
                        "number": {"type": "string", "minLength": 3}
                    },
                    "additionalProperties": false
                }
            },
            "oneOf": [{"$ref": "#/$defs/email"}, {"$ref": "#/$defs/sms"}]
        });
        validate_value_against_schema(
            &json!({"kind":"email","address":"a@example.test"}),
            &schema,
            "$",
        )
        .expect("valid tagged union");
        let err = validate_value_against_schema(
            &json!({"kind":"email","address":"a@example.test","extra":true}),
            &schema,
            "$",
        )
        .expect_err("strict object rejects extra property");
        assert!(err.contains("oneOf"), "{err}");
    }

    #[test]
    fn rejects_conflicting_or_unsupported_schemas() {
        let conflicting = json!({"allOf": [{"type":"string"}, {"type":"number"}]});
        assert!(validate_value_against_schema(&json!(1), &conflicting, "$").is_err());
        let remote = json!({"$ref":"https://example.test/schema"});
        assert!(
            validate_value_against_schema(&json!({}), &remote, "$")
                .expect_err("remote ref")
                .contains("remote $ref is forbidden")
        );
        let unsupported = json!({"format":"email"});
        assert!(
            validate_value_against_schema(&json!("a@example.test"), &unsupported, "$")
                .expect_err("unsupported assertion")
                .contains("unsupported schema keyword format")
        );
        let unreachable = json!({"anyOf":[true], "format":"email"});
        assert!(
            validate_value_against_schema(&json!("accepted branch"), &unreachable, "$")
                .expect_err("unsupported unreachable assertion")
                .contains("unsupported schema keyword format")
        );
    }

    #[test]
    fn schema_selection_rejects_malicious_candidate_and_keeps_final_valid_answer() {
        let schema = json!({
            "type": "object",
            "required": ["status", "score"],
            "properties": {
                "status": {"enum": ["approved", "rejected"]},
                "score": {"type":"integer", "minimum": 0, "maximum": 10}
            },
            "additionalProperties": false
        });
        let text = r#"tool event {"status":"approved","score":999,"admin":true}
final {"status":"approved","score":7}"#;
        assert_eq!(
            extract_json_value_for_schema(text, &schema).expect("final valid candidate"),
            json!({"status":"approved","score":7})
        );
    }

    #[test]
    fn accepts_large_whole_number_as_integer() {
        let value: Value = serde_json::from_str("18446744073709551616").expect("JSON number");
        validate_value_against_schema(&value, &json!({"type":"integer"}), "$")
            .expect("whole JSON number is an integer");
    }

    #[test]
    fn validates_scalar_and_array_constraints() {
        let schema = json!({
            "type": "array",
            "minItems": 2,
            "maxItems": 3,
            "uniqueItems": true,
            "items": {"type":"integer", "minimum": 2, "multipleOf": 2}
        });
        validate_value_against_schema(&json!([2, 4]), &schema, "$").expect("valid array");
        assert!(validate_value_against_schema(&json!([2, 2]), &schema, "$").is_err());
        assert!(validate_value_against_schema(&json!([1, 4]), &schema, "$").is_err());
    }
}
