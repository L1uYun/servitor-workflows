# Multi-agent negotiation (canary)

> Owner: `servitor-workflows` only. Servitor stays transport.
> Protocol: `negotiate-2body.v1` · 2026-07-26

## Purpose

Prove two-body negotiation above existing primitives
(`agent` / `parallel` / `pipeline` / `gate` / journal replay).
No new Rust command, role registry, or servitor batch surface.

## Roles (script-level only)

| Role | Identity | Duty |
|---|---|---|
| ROLE_A | proposer | emit concrete proposal + assumptions + confidence |
| ROLE_B | reviewer | `accept` / `reject` / `revise` + critique + must_fix |
| synthesizer | consolidator | final `accepted` + decision + open_issues |

Roles are `systemPrompt` + prompt markers, not runtime types.

## Round loop

1. ROLE_A proposes (or revises from PRIOR history).
2. ROLE_B reviews that proposal.
3. Stop if `verdict == accept` **or** `round == maxRounds` (default 2, hard cap 4).
4. synthesizer reads full history + stop reason → decision object.
5. Optional human `gate` when `args.requireHumanGate=true`.

## Schemas (minimal)

```text
proposal:  { proposal:string, assumptions:string[], confidence:number }
review:    { verdict:string, critique:string, must_fix:string[] }
decision:  { accepted:bool, decision:string, rationale:string, open_issues:string[] }
```

`verdict` values: `accept` | `reject` | `revise` (lowercase compare in script).

## Evidence

- workflow `journal.jsonl`: every agent/gate key
- workflow return value: `{ protocol, topic, stopReason, rounds, history, decision, human }`
- servitor run ids remain transport evidence only

## Harness map (local)

| Level | Meaning here | Status |
|---|---|---|
| C1 | single agent call through servitor | already via `agent()` |
| C3 | multi-step phase + journal resume | already via `phase` + store |
| C4 | fan-out / concurrent agents | already via `parallel`/`pipeline` |
| C5 | human gate in the loop | already via `gate()` |
| C6 | multi-body negotiation protocol | **this canary** (script composition) |

Gap after canary: only richer stop policies (vote quorum, N-body, cost caps) if a real task demands them — do not pre-build.

## Entry

```powershell
servitor-workflows run D:\AgentWork\tools\servitor-workflows\examples\negotiate-2body.workflow.js `
  --args '{"topic":"pick a 30s local canary","maxRounds":2}'
servitor-workflows get RUN_ID
```

Offline proof: `cargo test negotiate_two_body_reaches_accept_after_revise` in this crate.

## Non-goals

- Do not add roles/batch/negotiate into `servitor`.
- Do not add Rust negotiation engine, role registry, or dashboard.
- Do not require human gate for the default canary.

## Goalchain embedding (一条龙)

Negotiation is an **internal** step of loop-engineering 	emplates/goalchain-dispatcher.js, not a parallel product.

Outer sequential shell:

`	ext
readiness → scout? → negotiate? → dispatch → review-gate → verification → semantic-gate → release? → writeback
→ (controller) neat-freak Closure after human final acceptance
`

- CONFIG.negotiation.topic empty/null → negotiate skipped (behavior-preserving).
- topic set → multi-body propose/review/synth; decision written to decisionPath (default <scratch>/negotiation-decision.json).
- 
equireAccept default true: ccepted=false fails the chain (
egotiate-gate / supersede → Grill), does not silently implement.
- Worker prompt receives the decision path; implement follows it, does not re-litigate without new evidence.
- CONFIG.release.commands optional post dual-gate install/blue-green/skills-sync; empty skips.
- neat-freak is **not** a workflow phase; controller runs Closure after acceptance.

