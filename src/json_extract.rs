//! Extract a JSON value from free-form model text.
//!
//! Algorithm matches the common LLM-output pattern used by tools such as
//! `json-from-llm` and LangChain's markdown JSON parsers:
//! 1. strip reasoning / thinking wrappers
//! 2. prefer fenced ```json / untagged ``` blocks
//! 3. scan string-aware balanced `{...}` / `[...]` spans
//! 4. parse candidates, with a single trailing-comma repair pass
//! 5. optionally require object or array when the contract says so

use serde_json::Value;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Expect {
    Any,
    Object,
    Array,
}

pub fn extract_json_value(text: &str, expect: Expect) -> Result<Value, String> {
    if text.trim().is_empty() {
        return Err("empty agent output".to_owned());
    }

    let cleaned = strip_reasoning(text);
    let mut candidates: Vec<String> = Vec::new();

    let fences = fenced_blocks(&cleaned);
    candidates.extend(fences.iter().cloned());
    for fence in &fences {
        candidates.extend(balanced_spans(fence));
    }
    candidates.extend(balanced_spans(&cleaned));

    // Direct whole-text attempt first among equals if it already looks clean.
    candidates.insert(0, cleaned.trim().to_owned());

    let mut last_error = "no JSON value could be extracted".to_owned();
    let mut seen = std::collections::BTreeSet::new();
    for candidate in candidates {
        let key = candidate.trim().to_owned();
        if key.is_empty() || !seen.insert(key.clone()) {
            continue;
        }
        match parse_candidate(&key) {
            Ok(value) if matches_expect(&value, expect) => return Ok(value),
            Ok(_) => last_error = format!("JSON found but expected {expect:?}"),
            Err(error) => last_error = error,
        }
    }
    Err(last_error)
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
pub fn remove_trailing_commas(input: &str) -> String {
    let bytes = input.as_bytes();
    let mut out = String::with_capacity(input.len());
    let mut in_string = false;
    let mut escaped = false;
    let mut i = 0;
    while i < bytes.len() {
        let ch = bytes[i] as char;
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
            while j < bytes.len() && matches!(bytes[j], b' ' | b'\n' | b'\r' | b'\t') {
                j += 1;
            }
            if j < bytes.len() && matches!(bytes[j], b'}' | b']') {
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
}
