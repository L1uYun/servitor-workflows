"""Shared run-model assembly: read a run's journal + optional script and produce
the structured model both viewers render.

1:1 Python port of runner/src/runModel.js. Pure beyond reading files, so a --watch
loop can call build_run_model() repeatedly as the journal grows.
"""
from __future__ import annotations

import json
import re
import time
from pathlib import Path
from typing import Any

import ast

_SESS_KEY_RE = re.compile(r"^sess:([^#]+)#(\d+)$")


def list_journals(run_dir: str | Path) -> list[dict]:
    """List a run directory's journals, newest first by mtime."""
    jdir = Path(run_dir) / ".workflow-journal"
    if not jdir.exists():
        return []
    names = [f for f in jdir.iterdir()
             if f.suffix == ".jsonl"
             and not f.name.endswith(".events.jsonl")
             and not f.name.endswith(".answers.jsonl")]
    out = []
    for p in names:
        try:
            st = p.stat()
            out.append({"path": str(p), "name": p.name, "mtime_ms": st.st_mtime * 1000, "size": st.st_size})
        except OSError:
            out.append({"path": str(p), "name": p.name, "mtime_ms": 0, "size": 0})
    out.sort(key=lambda x: (-x["mtime_ms"], x["name"]))
    return out


def events_path_for(journal_path: str) -> str:
    return re.sub(r"\.jsonl$", "", journal_path, flags=re.IGNORECASE) + ".events.jsonl"

def result_path_for(journal_path: str) -> str:
    return re.sub(r"\.jsonl$", "", journal_path, flags=re.IGNORECASE) + ".result.json"

def run_meta_path_for(journal_path: str) -> str:
    return re.sub(r"\.jsonl$", "", journal_path, flags=re.IGNORECASE) + ".meta.json"

def progress_path_for(journal_path: str) -> str:
    return re.sub(r"\.jsonl$", "", journal_path, flags=re.IGNORECASE) + ".progress.json"

def questions_path_for(journal_path: str) -> str:
    return re.sub(r"\.jsonl$", "", journal_path, flags=re.IGNORECASE) + ".questions.json"

def answers_path_for(journal_path: str) -> str:
    return re.sub(r"\.jsonl$", "", journal_path, flags=re.IGNORECASE) + ".answers.jsonl"


def read_result(journal_path: str) -> Any:
    p = result_path_for(journal_path)
    if not Path(p).exists():
        return None
    try:
        return json.loads(Path(p).read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return None

def read_run_meta(journal_path: str) -> dict | None:
    p = run_meta_path_for(journal_path)
    if not Path(p).exists():
        return None
    try:
        return json.loads(Path(p).read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return None

def read_events(journal_path: str) -> list[dict] | None:
    p = events_path_for(journal_path)
    if not Path(p).exists():
        return None
    evs = []
    try:
        for line in Path(p).read_text(encoding="utf-8").strip().split("\n"):
            if line.strip():
                try:
                    evs.append(json.loads(line))
                except json.JSONDecodeError:
                    pass
    except OSError:
        return None
    return evs

def read_progress(journal_path: str) -> dict:
    p = progress_path_for(journal_path)
    if not Path(p).exists():
        return {}
    try:
        return json.loads(Path(p).read_text(encoding="utf-8")) or {}
    except (json.JSONDecodeError, OSError):
        return {}

def read_questions(journal_path: str) -> list[dict]:
    p = questions_path_for(journal_path)
    if not Path(p).exists():
        return []
    try:
        return json.loads(Path(p).read_text(encoding="utf-8")) or []
    except (json.JSONDecodeError, OSError):
        return []


def live_state(events: list[dict] | None) -> dict | None:
    """Derive live state from the event stream."""
    if not events:
        return None
    by_id: dict[str, dict] = {}
    first_t = float("inf")
    last_t = 0
    ended = 0
    for e in events:
        t = e.get("t")
        if isinstance(t, (int, float)):
            if t < first_t:
                first_t = t
            if t > last_t:
                last_t = t
        eid = e.get("id") or e.get("label")
        c = by_id.get(eid, {"label": e.get("label"), "starts": 0, "ends": 0, "last_start_t": 0})
        if e.get("label"):
            c["label"] = e["label"]
        if e.get("type") == "start":
            c["starts"] += 1
            c["last_start_t"] = t or c["last_start_t"]
            c["phase"] = e.get("phase")
            c["model"] = e.get("model")
            c["effort"] = e.get("effort")
            if e.get("kind") == "session":
                c["kind"] = "session"
                c["sessionId"] = e.get("sessionId")
                c["turn"] = e.get("turn")
        elif e.get("type") in ("end", "cached"):
            c["ends"] += 1
            ended += 1
        by_id[eid] = c
    running = []
    for eid, c in by_id.items():
        if c["starts"] > c["ends"]:
            r = {"id": eid, "label": c["label"], "phase": c.get("phase"),
                 "model": c.get("model"), "effort": c.get("effort"),
                 "startedAt": c["last_start_t"], "status": "running"}
            if c.get("kind") == "session":
                r["kind"] = "session"
                r["sessionId"] = c.get("sessionId")
                r["turn"] = c.get("turn")
            running.append(r)
    return {
        "running": running,
        "doneCount": ended,
        "runStartedAt": first_t if first_t != float("inf") else None,
        "lastEventAt": last_t or None,
    }


def _attach_sessions(run: dict) -> dict:
    """Group session-turn agents into per-worker rollups."""
    by_id: dict[str, dict] = {}
    for a in run["agents"]:
        if a.get("kind") != "session" or not a.get("sessionId"):
            continue
        sid = a["sessionId"]
        s = by_id.get(sid, {
            "id": sid, "label": a["label"], "phase": a.get("phase"),
            "model": None, "effort": None, "order": a["order"],
            "turns": [], "tokens": 0, "ms": 0, "running": False, "threadId": None,
        })
        if a.get("label"):
            s["label"] = a["label"]
        if a.get("model"):
            s["model"] = a["model"]
        if a.get("effort") is not None:
            s["effort"] = a["effort"]
        if a.get("threadId"):
            s["threadId"] = a["threadId"]
        s["order"] = min(s["order"], a["order"])
        running = a.get("status") == "running"
        s["turns"].append({
            "id": a["id"], "turn": a.get("turn", len(s["turns"])),
            "status": a.get("turnStatus", "completed") if not running else "running",
            "tokens": a.get("tokens"), "ms": a.get("ms"),
        })
        s["tokens"] += a.get("tokens") or 0
        s["ms"] += a.get("ms") or 0
        if running:
            s["running"] = True
        by_id[sid] = s
    sessions = list(by_id.values())
    for s in sessions:
        s["turns"].sort(key=lambda x: x.get("turn", 0))
        s["status"] = "running" if s["running"] else (s["turns"][-1]["status"] if s["turns"] else "completed")
        del s["running"]
    sessions.sort(key=lambda x: x["order"])
    run["sessions"] = sessions
    run["counts"]["sessions"] = len(sessions)
    return run


def build_run_model(*, journal_path: str, script_path: str | None = None,
                    run_dir: str | None = None, title: str | None = None,
                    generated_at: str | None = None) -> dict:
    """Build the structured run model from a journal + optional script."""
    by_key: dict[str, dict] = {}
    journal_text = ""
    try:
        journal_text = Path(journal_path).read_text(encoding="utf-8")
    except OSError:
        pass

    for line in journal_text.strip().split("\n"):
        if not line.strip():
            continue
        try:
            e = json.loads(line)
            if e and e.get("label"):
                by_key[e.get("key", e["label"])] = e
        except json.JSONDecodeError:
            pass

    entries_all = list(by_key.values())
    agents_raw = [e for e in entries_all if not e.get("human")]
    checkpoints = [
        {"id": e["key"], "qid": e["label"], "question": e.get("question", ""),
         "answer": e.get("result"), "source": e.get("source")}
        for e in entries_all if e.get("human")
    ]

    meta = None
    if script_path and Path(script_path).exists():
        try:
            src = Path(script_path).read_text(encoding="utf-8")
            tree = ast.parse(src)
            for node in ast.iter_child_nodes(tree):
                if isinstance(node, ast.Assign):
                    for target in node.targets:
                        if isinstance(target, ast.Name) and target.id == "meta":
                            meta = ast.literal_eval(node.value)
        except Exception:
            pass

    meta_phases = []
    if meta and isinstance(meta.get("phases"), list):
        for p in meta["phases"]:
            if isinstance(p, str):
                meta_phases.append({"title": p, "detail": ""})
            else:
                meta_phases.append({"title": p.get("title", ""), "detail": p.get("detail", "")})

    agents = []
    for i, e in enumerate(agents_raw):
        a = {
            "id": e.get("key", e["label"]),
            "label": e["label"],
            "order": i,
            "phase": e.get("phase", "Agents"),
            "model": e.get("model"),
            "effort": e.get("effort"),
            "tokens": e.get("tokens") if isinstance(e.get("tokens"), (int, float)) else None,
            "ms": e.get("ms") if isinstance(e.get("ms"), (int, float)) else None,
            "result": e.get("result"),
        }
        sess_key = _SESS_KEY_RE.match(str(a["id"])) if a["id"] else None
        if e.get("session") or sess_key:
            a["kind"] = "session"
            a["sessionId"] = e.get("sessionId") or (sess_key.group(1) if sess_key else None)
            a["turn"] = e.get("turn") if isinstance(e.get("turn"), int) else (int(sess_key.group(2)) if sess_key else None)
            a["turnStatus"] = e.get("status", "completed")
            if e.get("threadId"):
                a["threadId"] = e["threadId"]
        agents.append(a)

    phase_order = [p["title"] for p in meta_phases]
    for a in agents:
        if a["phase"] not in phase_order:
            phase_order.append(a["phase"])

    models: dict[str, int] = {}
    for a in agents:
        if a.get("model"):
            models[a["model"]] = models.get(a["model"], 0) + 1

    total_tokens = sum(a.get("tokens") or 0 for a in agents)
    total_ms = sum(a.get("ms") or 0 for a in agents)
    has_metrics = any(a.get("tokens") is not None or a.get("ms") is not None for a in agents)

    return _attach_sessions({
        "name": title or (meta and meta.get("name")) or Path(journal_path).stem.replace(".workflow", ""),
        "description": (meta and meta.get("description")) or "",
        "phases": [{"title": t, "detail": next((p["detail"] for p in meta_phases if p["title"] == t), "")}
                   for t in phase_order],
        "agents": agents,
        "models": models,
        "totals": {"tokens": total_tokens, "ms": total_ms, "has_metrics": has_metrics},
        "counts": {"phases": len(phase_order), "agents": len(agents)},
        "checkpoints": checkpoints,
        "result": read_result(journal_path),
        "sources": {"journal": journal_path, "script": script_path if script_path and Path(script_path).exists() else None,
                     "runDir": run_dir},
        "generatedAt": generated_at or time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime()),
    })


def build_live_run_model(opts: dict) -> dict:
    """Build run model + merge live event stream for running agents."""
    run = build_run_model(**opts)
    ls = live_state(read_events(opts["journal_path"]))
    run["live"] = ls or {"running": [], "doneCount": len(run["agents"]),
                          "runStartedAt": None, "lastEventAt": None}
    if ls and ls["running"]:
        done_ids = {a["id"] for a in run["agents"]}
        order = len(run["agents"])
        for r in ls["running"]:
            rid = r.get("id") or r.get("label")
            if rid in done_ids:
                continue
            phase = r.get("phase") or "Agents"
            a = {"id": rid, "label": r["label"], "order": order, "phase": phase,
                 "model": r.get("model"), "effort": r.get("effort"),
                 "tokens": None, "ms": None, "result": None, "status": "running",
                 "startedAt": r.get("startedAt")}
            if r.get("kind") == "session":
                a["kind"] = "session"
                a["sessionId"] = r.get("sessionId")
                a["turn"] = r.get("turn")
            run["agents"].append(a)
            if not any(p["title"] == phase for p in run["phases"]):
                run["phases"].append({"title": phase, "detail": ""})
            order += 1
        run["counts"] = {"phases": len(run["phases"]), "agents": len(run["agents"])}
        _attach_sessions(run)

    prog = read_progress(opts["journal_path"])
    for a in run["agents"]:
        if a.get("status") == "running":
            p = prog.get(a["id"]) or prog.get(a["label"])
            if p:
                a["progress"] = p
    run["questions"] = read_questions(opts["journal_path"])
    return run
