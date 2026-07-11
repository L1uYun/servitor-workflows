"""servitor_workflows.cli: command-line interface.

1:1 Python port of runner/bin/run-workflow.js core flow.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import pathlib
import sys
import time
from pathlib import Path

from . import __version__
from .journal import Journal
from .run_workflow import extract_meta, resolve_default_agent, run_workflow_file


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="servitor-workflows",
        description="Run dynamic workflow scripts on local servitor subagents.",
    )
    parser.add_argument("--version", action="version", version=__version__)
    sub = parser.add_subparsers(dest="command")

    run_p = sub.add_parser("run", help="Run a workflow file")
    run_p.add_argument("script", help="Path to .workflow.py file")
    run_p.add_argument("--args", default=None, help="JSON args string")
    run_p.add_argument("--args-file", default=None, help="Path to JSON args file")
    run_p.add_argument("--budget", type=int, default=None, help="Token budget")
    run_p.add_argument("--agent", default=None, help="Default agent/provider (e.g. claude, codebuddy)")
    run_p.add_argument("--model", default=None, help="Default model")
    run_p.add_argument("--pin-model", default=None, help="Pin all agents to one model")
    run_p.add_argument("--effort", default=None, help="Default effort level")
    run_p.add_argument("--auto-effort", action="store_true", help="Scale effort by layer width")
    run_p.add_argument("--pin-effort", default=None, help="Pin all agents to one effort")
    run_p.add_argument("--plan", action="store_true", help="Dry run: no model calls (agent() returns schema skeletons). Workflow Python still executes — avoid cache/file side effects or gate them with the plan flag.")
    run_p.add_argument("--resume", action="store_true", help="Reuse prior results from journal")
    run_p.add_argument("--journal", default=None, help="Journal path override")
    run_p.add_argument("--run-id", default=None, help="Suffix for journal/sidecar paths")
    run_p.add_argument("--fresh", action="store_true", help="Discard prior journal before run")
    run_p.add_argument("--no-journal", action="store_true", help="Disable journal")
    summary_mode = run_p.add_mutually_exclusive_group()
    summary_mode.add_argument("--summary", action="store_true", help="Print a richer bounded human result summary")
    summary_mode.add_argument("--no-summary", action="store_true", help="Suppress the human result summary")
    run_p.add_argument("--json", action="store_true", help="Output JSON result to stdout")
    run_p.add_argument("--transport", default="servitor", help="Transport backend (for testing)")
    run_p.set_defaults(func=_cmd_run)

    # summarize subcommand
    sum_p = sub.add_parser("summarize", help="Print a run summary")
    sum_p.add_argument("journal", help="Journal path")
    sum_p.add_argument("--script", default=None, help="Workflow script path")
    sum_p.add_argument("--include-result", action="store_true")
    sum_p.add_argument("--json", action="store_true")
    sum_p.set_defaults(func=_cmd_summarize)

    # map subcommand
    map_p = sub.add_parser("map", help="Render an ASCII execution DAG")
    map_p.add_argument("journal", help="Journal path")
    map_p.add_argument("--script", default=None, help="Workflow script path")
    map_p.add_argument("--width", type=int, default=80)
    map_p.set_defaults(func=_cmd_map)

    # status subcommand
    status_p = sub.add_parser("status", help="Fleet status of one or more runs")
    status_p.add_argument("targets", nargs="*", help="Run directories or journal paths")
    status_p.add_argument("--json", action="store_true")
    status_p.set_defaults(func=_cmd_status)

    # compare subcommand
    cmp_p = sub.add_parser("compare", help="Compare runs over time")
    cmp_p.add_argument("targets", nargs="*", help="Run directories or journal paths")
    cmp_p.add_argument("--json", action="store_true")
    cmp_p.set_defaults(func=_cmd_compare)

    # supervise subcommand
    sup_p = sub.add_parser("supervise", help="Poll fleet status in a loop")
    sup_p.add_argument("targets", nargs="*", help="Run directories or journal paths")
    sup_p.add_argument("--interval", type=float, default=5.0, help="Poll interval in seconds")
    sup_p.add_argument("--rounds", type=int, default=None, help="Max polling rounds")
    sup_p.set_defaults(func=_cmd_supervise)

    return parser


def _cmd_compare(args):
    from .compare_runs import collect_comparison, render_comparison_text
    import json as _json
    data = collect_comparison(args.targets or ["."])
    if args.json:
        print(_json.dumps(data, ensure_ascii=False, indent=2, default=str))
    else:
        print(render_comparison_text(data))
    return 0


def _cmd_supervise(args):
    from .supervise import supervise
    import asyncio as _asyncio
    _asyncio.run(supervise(
        args.targets or ["."],
        interval_s=args.interval,
        max_rounds=args.rounds,
    ))
    return 0


def _cmd_summarize(args):
    from .run_summary import summarize_run
    s = summarize_run(journal_path=args.journal, script_path=args.script,
                      include_result=args.include_result)
    import json as _json
    if args.json:
        print(_json.dumps(s, ensure_ascii=False, indent=2, default=str))
    else:
        from .run_summary import render_end_of_run
        print(render_end_of_run(s))
    return 0


def _cmd_map(args):
    from .run_model import build_run_model
    from .ascii_map import render_map
    run = build_run_model(journal_path=args.journal, script_path=args.script)
    print(render_map(run, width=args.width))
    return 0


def _cmd_status(args):
    from .fleet_status import inspect_run, render_fleet_text
    import json as _json
    targets = args.targets or ["."]
    journals = []
    for t in targets:
        tpath = pathlib.Path(t)
        if tpath.suffix == ".jsonl":
            journals.append(str(tpath))
        else:
            from .run_model import list_journals
            for j in list_journals(tpath):
                journals.append(j["path"])
    infos = [inspect_run(j) for j in journals]
    if args.json:
        print(_json.dumps(infos, ensure_ascii=False, indent=2, default=str))
    else:
        print(render_fleet_text(infos))
    return 0


def _compact_text(value, limit=180):
    text = " ".join(str(value if value is not None else "").split())
    if len(text) <= limit:
        return text
    return text[: max(0, limit - 3)] + "..."


def _summary_value(value, limit=180):
    if isinstance(value, dict):
        return f"{{{len(value)} fields}}"
    if isinstance(value, (list, tuple)):
        return f"[{len(value)} items]"
    return _compact_text(value, limit)


def _render_result_summary(result, detailed=False):
    if isinstance(result, dict):
        if detailed:
            lines = ["result:"]
            for key, value in list(result.items())[:20]:
                lines.append(f"  {key}: {_summary_value(value, 220)}")
            if len(result) > 20:
                lines.append(f"  ... {len(result) - 20} more fields; use --json")
            return "\n".join(lines)
        preferred = ("status", "ok", "answer", "summary", "message", "result")
        keys = [key for key in preferred if key in result]
        keys.extend(key for key in result if key not in keys)
        parts = [f"{key}={_summary_value(result[key], 80)}" for key in keys[:6]]
        if len(keys) > 6:
            parts.append(f"+{len(keys) - 6} fields")
        return "result: " + (" | ".join(parts) if parts else "(empty)")
    if isinstance(result, (list, tuple)):
        if detailed:
            lines = [f"result: {len(result)} items"]
            lines.extend(f"  - {_summary_value(value, 220)}" for value in result[:20])
            if len(result) > 20:
                lines.append(f"  ... {len(result) - 20} more items; use --json")
            return "\n".join(lines)
        return f"result: [{len(result)} items]"
    return f"result: {_summary_value(result, 240)}"


def _cmd_run(args: argparse.Namespace) -> int:
    script = str(Path(args.script).resolve())
    workflow_meta = extract_meta(Path(script).read_text(encoding="utf-8")) or {}
    effective_agent, effective_agent_source = resolve_default_agent(args.agent, workflow_meta)

    # Parse args
    workflow_args = None
    if args.args:
        workflow_args = json.loads(args.args)
    elif args.args_file:
        workflow_args = json.loads(Path(args.args_file).read_text(encoding="utf-8"))

    # Journal setup
    journal = None
    journal_path = None
    if not args.no_journal:
        run_suffix = f"--{args.run_id}" if args.run_id else ""
        run_suffix = run_suffix.replace(" ", "_").replace("/", "_")
        default_name = Path(script).stem + run_suffix + ".jsonl"
        journal_path = args.journal or str(Path(".workflow-journal") / default_name)
        journal_path = str(Path(journal_path).resolve())
        Path(journal_path).parent.mkdir(parents=True, exist_ok=True)
        if args.fresh:
            try:
                Path(journal_path).unlink()
            except FileNotFoundError:
                pass
        if not Path(journal_path).exists():
            Path(journal_path).touch()
        journal = Journal(journal_path, reuse=args.resume)
        journal.load()
        print(f"{'↻ resuming from' if args.resume else '✎ journal:'} {journal_path}", file=sys.stderr)

    # Events sidecar
    events_path = None
    if journal_path:
        events_path = Path(journal_path).with_suffix(".events.jsonl")
        events_path.write_text("", encoding="utf-8")

    def _on_phase(title: str):
        print(f"\n━━ {title} ━━", file=sys.stderr)

    def _on_log(message: str):
        print(message, file=sys.stderr)

    def _on_event(evt: dict):
        if events_path:
            with open(events_path, "a", encoding="utf-8") as f:
                f.write(json.dumps(evt, ensure_ascii=False) + "\n")

    defaults = {}
    if args.effort:
        defaults["effort"] = args.effort

    if args.plan:
        print(
            "⚠ --plan: agent() will not call models, but workflow Python still runs. "
            "Gate disk/cache side effects with the runtime `plan` flag.",
            file=sys.stderr,
        )
    run_opts = {
        "args": workflow_args or {},
        "budget_total": args.budget,
        "defaults": defaults,
        "default_agent": effective_agent,
        "default_agent_source": effective_agent_source,
        "default_model": args.model,
        "pinned_model": args.pin_model,
        "auto_effort": args.auto_effort,
        "pinned_effort": args.pin_effort,
        "plan": args.plan,
        "on_phase": _on_phase,
        "on_log": _on_log,
        "on_event": _on_event,
        "journal": journal,
    }

    # Write run-meta sidecar
    if journal_path:
        meta_path = Path(journal_path).with_suffix(".meta.json")
        meta_path.write_text(json.dumps({
            "budget": args.budget, "agent": args.agent,
            "effectiveAgent": effective_agent, "effectiveAgentSource": effective_agent_source,
            "model": args.pin_model or args.model,
            "autoEffort": args.auto_effort, "pinEffort": args.pin_effort,
            "pid": os.getpid(), "startedAt": int(time.time() * 1000),
            "script": script, "runId": args.run_id,
        }), encoding="utf-8")

    end_status = "completed"
    try:
        result = asyncio.run(run_workflow_file(script, run_opts))
        if args.json:
            print(json.dumps(result, ensure_ascii=False, indent=2, default=str))
        elif not args.no_summary:
            print(_render_result_summary(result, detailed=args.summary))
        # Persist result
        if journal_path and result is not None:
            result_path = Path(journal_path).with_suffix(".result.json")
            result_path.write_text(json.dumps(result, ensure_ascii=False, default=str), encoding="utf-8")
    except Exception as e:
        end_status = "budget_exceeded" if getattr(e, "code", None) == "BUDGET_EXCEEDED" else "failed"
        if getattr(e, "code", None) == "BUDGET_EXCEEDED":
            print(f"\n💸 {e}", file=sys.stderr)
        else:
            print(f"\nworkflow failed: {e}", file=sys.stderr)
        return 1

    return 0


def main(argv=None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if not hasattr(args, "func"):
        parser.print_help()
        return 0
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
