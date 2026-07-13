"""Version-controlled fixture for the skill-review L2/L3 incomplete-evidence gate."""
from __future__ import annotations

import json
from pathlib import Path

meta = {
    "name": "skill-portfolio-review-3layer-fixture",
    "description": "Minimal tracked fixture for the L2/L3 incomplete-evidence gate",
    "default_agent": "pi",
}

BASE = Path(__file__).resolve().parent
L1_PACKETS = BASE / "L1_packets_compact"
GROUPS_PATH = BASE / "L2_groups.json"
INDEX_PATH = BASE / "skill_index.json"


async def main(agent, parallel, pipeline, phase, log, budget, args, human, workflow, plan=False):
    packets = sorted(L1_PACKETS.glob("*.md"), key=lambda p: p.stat().st_size)
    skills = [p.stem for p in packets]
    phase("L1-independent-skill-reviews")
    l1_rows = []
    for skill in skills:
        if plan:
            l1_rows.append({"skill": skill, "ok": False, "result": None, "cached": False, "plan_skip": True})
        else:
            l1_rows.append({"skill": skill, "ok": True, "result": {"skill": skill}, "cached": False})
    l1_map = {row["skill"]: row["result"] for row in l1_rows if row and row.get("ok") and row.get("result")}

    groups = json.loads(GROUPS_PATH.read_text(encoding="utf-8"))
    index = json.loads(INDEX_PATH.read_text(encoding="utf-8"))
    log(f"index={len(index)} groups={len(groups)}")
    phase("L2-workflow-reviews")
    l2_rows = []
    for name, group in groups.items():
        if plan:
            l2_rows.append({"workflow": name, "ok": False, "result": None, "cached": False, "plan_skip": True, "skills": group["skills"]})
        else:
            l2_rows.append({"workflow": name, "ok": True, "result": {"workflow": name, "summary": "ok", "ordered_edits": []}, "cached": False})
    l2_map = {row["workflow"]: row["result"] for row in l2_rows if row and row.get("ok") and row.get("result")}

    log(f"L2 complete ok={len(l2_map)}/{len(groups)}")
    incomplete = len(l1_map) != len(skills) or len(l2_map) != len(groups)
    if incomplete:
        missing_l1 = [s for s in skills if s not in l1_map]
        missing_l2 = [n for n in groups if n not in l2_map]
        if not plan:
            raise RuntimeError(
                f"refusing L3 with incomplete evidence: missing_l1={missing_l1}, missing_l2={missing_l2}"
            )
        log(
            f"Skipping L3 due to incomplete evidence in plan mode: missing_l1={missing_l1}, missing_l2={missing_l2}"
        )
        l3_result = None
    else:
        phase("L3-portfolio-synthesis")
        if plan:
            l3_result = {"plan": True}
        else:
            l3_result = await agent("l3")
    log("L3 complete")
    return {
        "l1_ok": len(l1_map),
        "l1_total": len(skills),
        "l1_missing": [s for s in skills if s not in l1_map],
        "l2_ok": len(l2_map),
        "l2_total": len(groups),
        "l2_missing": [n for n in groups if n not in l2_map],
        "l3_ok": l3_result is not None,
        "plan": plan,
    }
