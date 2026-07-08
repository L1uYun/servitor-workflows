"""Controller-side advisor: ask a stronger model via servitor.

Usage:
  python -m servitor_workflows.advisor "I'm stuck on X, here's the context..."
  python -m servitor_workflows.advisor --model newapi/claude-opus-4-8 --prompt-file problem.md
  echo "problem text" | python -m servitor_workflows.advisor --stdin

Defaults: agent=pi, model=newapi/claude-opus-4-8 (heavy tier, strong reasoning).
The advisor sees ONLY what you give it — this is NOT server-side API advisor.
You must include relevant code, errors, and context in the prompt yourself.
"""
from __future__ import annotations

import argparse
import json
import pathlib
import sys
import textwrap
import time

DEFAULT_AGENT = "pi"
DEFAULT_MODEL = "newapi/gpt-5.5"
DEFAULT_WAIT = 180

ADVISOR_PREAMBLE = """\
You are an advisor to another AI coding agent. The agent below will describe
a problem it is stuck on or a decision it needs to make. Give focused,
actionable guidance. Do not repeat the problem back. Do not write the full
solution unless asked — point to the approach, the pitfall, the key constraint.

Keep your guidance under 300 words unless the problem clearly needs more.
"""


def _read_prompt(args: argparse.Namespace) -> str:
    """Assemble advisor prompt from stdin, --prompt-file, or --prompt."""
    parts = []

    if args.stdin:
        data = sys.stdin.read().strip()
        if data:
            parts.append(data)

    if args.prompt_file:
        p = pathlib.Path(args.prompt_file)
        if not p.exists():
            print(f"advisor: prompt-file not found: {p}", file=sys.stderr)
            sys.exit(2)
        parts.append(p.read_text(encoding="utf-8").strip())

    if args.prompt:
        parts.append(args.prompt.strip())

    if not parts:
        print(
            "advisor: no prompt given. Use --prompt, --prompt-file, or pipe to --stdin.",
            file=sys.stderr,
        )
        sys.exit(2)

    return "\n\n---\n\n".join(parts)


def main() -> int:
    parser = argparse.ArgumentParser(
        prog="advisor",
        description="Ask a stronger model for strategic guidance via servitor.",
    )
    parser.add_argument(
        "prompt", nargs="?", default=None,
        help="Short problem description (use --prompt-file for long context).",
    )
    parser.add_argument("--prompt-file", default=None, help="Path to a prompt file.")
    parser.add_argument("--stdin", action="store_true", help="Read prompt from stdin.")
    parser.add_argument("--agent", default=DEFAULT_AGENT, help=f"Servitor agent (default: {DEFAULT_AGENT}).")
    parser.add_argument("--model", default=DEFAULT_MODEL, help=f"Model id (default: {DEFAULT_MODEL}).")
    parser.add_argument("--cwd", default=str(pathlib.Path.cwd()), help="Working directory for the run.")
    parser.add_argument("--wait", type=int, default=DEFAULT_WAIT, help=f"Seconds to wait (default: {DEFAULT_WAIT}).")
    parser.add_argument("--json", action="store_true", help="Output full JSON result instead of plain text.")
    args = parser.parse_args()

    user_text = _read_prompt(args)
    full_prompt = ADVISOR_PREAMBLE + "\n\n" + user_text + "\n"

    import servitor

    run_info = servitor.run_agent(
        provider_name=args.agent,
        prompt=full_prompt,
        model=args.model,
        cwd=args.cwd,
        timeout_seconds=args.wait,
    )
    run_dir = run_info.get("run_dir") if isinstance(run_info, dict) else getattr(run_info, "run_dir", None)

    servitor.wait_for_completion(run_dir, args.wait)
    meta = servitor.read_result(run_dir)

    ok = meta.get("ok", False) if isinstance(meta, dict) else False
    result = meta.get("result") if isinstance(meta, dict) else None
    failure = meta.get("failure_reason") if isinstance(meta, dict) else None

    if args.json:
        meta["_advisor_run_dir"] = run_dir
        print(json.dumps(meta, ensure_ascii=False, indent=2))
    else:
        if ok and result:
            print(result)
        elif failure:
            print(f"[advisor] failure_reason={failure}", file=sys.stderr)
            print(f"[advisor] run_dir={run_dir}", file=sys.stderr)
            if result:
                print(result, file=sys.stderr)
            return 1
        else:
            print(f"[advisor] no result. run_dir={run_dir}", file=sys.stderr)
            return 1

    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
