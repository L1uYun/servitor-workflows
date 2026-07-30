export const meta = {
  name: "goalchain",
  description: "Demand-to-delivery work chain with dual gates and independent review",
  contract: "workflow",
  // Boundary: the chain names its observable write surface up front; the host
  // audits every command against it and records the evidence. This is an audit
  // boundary, not an OS sandbox. network is "allow" because every agent() call
  // is a transport submission, which the host audits as network use; the write
  // surface stays ./out only.
  boundary: {
    readPaths: ["."],
    writePaths: ["./out"],
    network: "allow",
    environment: { allow: ["EVIDENCE"] },
  },
  // Capability registry: explicit candidates only. Routing never probes a
  // provider or manufactures a silent fallback; an inadmissible choice fails
  // before transport submission. `reviewer` is declared here (and inherited by
  // the review child) so the independent-review contract is part of the policy;
  // the reviewer agent itself runs in the child workflow, never in this run.
  //
  // Reviewer independence here is STRUCTURAL, not model-level: the reviewer runs
  // in a separate run with its own journal and a read-only boundary, so the
  // maker's output is never self-graded in the same run (G13). With a single
  // declared provider both roles resolve to the same model — that is fine for
  // G13, which forbids self-review, not same-model review. We therefore do NOT
  // declare `independentFrom`: with one provider that constraint could never
  // fire, and declaring dead policy would overstate the guarantee.
  capabilities: {
    providers: [
      // pi model format: "provider/model-id" (see ~/.pi/agent/models.json).
      // Bare model ids (e.g. "glm-5.2") make pi guess the upstream provider
      // and fail with "No API key found for <guessed-provider>".
      { agent: "pi", model: "newapi/glm-5.2", capabilities: ["reasoning"], maxEffort: "high", contextTokens: 400000 },
    ],
    roles: {
      maker: { requires: ["reasoning"], effort: "high", contextTokens: 100000 },
      reviewer: { requires: ["reasoning"], effort: "high", contextTokens: 100000 },
      semantic: { requires: ["reasoning"], effort: "high", contextTokens: 100000 },
    },
  },
};

// Demand-to-delivery goalchain dispatcher. Envelope recovery, schema handling,
// boundary fingerprinting, resume, and cost accounting are owned by the
// workflow runtime; this chain keeps only the controller discipline the runtime
// does not supersede:
//
//   - G1  chain entry is instantiating this script per task
//   - G3  every agent ends schema-valid JSON (the `schema` option enforces it)
//   - G13 independent review as a child workflow, never self-review
//   - G15 mechanical tokens bound to contract text and evidence keys
//   - G16 `servitor-workflows check` before run
//
// args:
//   contractPath: string       confirmed contract the delivery must satisfy
//   evidencePath: string       machine evidence file the chain produces
//   mechanicalTokens: string[] tokens that must appear in contract + evidence
//   reviewScript: string       independent reviewer child (default goalchain-review.workflow.js)
//   requireHumanGate: boolean  park for human acceptance before the semantic gate (default true)

const contractPath = String((args && args.contractPath) || "contract.md");
const evidencePath = String((args && args.evidencePath) || "./out/evidence.json");
const mechanicalTokens = Array.isArray(args && args.mechanicalTokens) ? args.mechanicalTokens : [];
const reviewScript = String((args && args.reviewScript) || "goalchain-review.workflow.js");
const requireHumanGate = !(args && args.requireHumanGate === false);

// ---------------------------------------------------------------------------
phase("readiness");
// G15: the mechanical tokens must be non-empty and present in the frozen
// contract before any worker runs. This is the acceptance-identifier binding;
// an empty table means the chain has no mechanical gate and must not start.
if (mechanicalTokens.length === 0) {
  throw new Error("readiness gate failed: mechanicalTokens empty — bind contract acceptance identifiers first");
}
const readiness = await command("pwsh", [
  "-NoProfile", "-Command",
  "$ErrorActionPreference='Stop';" +
  "$p='" + contractPath.replace(/'/g, "''") + "';" +
  "if(!(Test-Path -LiteralPath $p)){ Write-Error 'contract missing'; exit 11 };" +
  "$c=Get-Content -LiteralPath $p -Raw -Encoding utf8;" +
  "$tokens=@(" + mechanicalTokens.map(t => "'" + String(t).replace(/'/g, "''") + "'").join(",") + ");" +
  "foreach($t in $tokens){ if($c -notmatch [regex]::Escape($t)){ Write-Error \"contract missing token: $t\"; exit 12 } };" +
  "'ready'",
], { label: "readiness", timeoutSeconds: 60 });
if (readiness.exitCode !== 0 || readiness.timedOut) {
  throw new Error("readiness gate failed: " + (readiness.stderr || readiness.stdout));
}

// ---------------------------------------------------------------------------
phase("dispatch");
// Bounded worker. One objective, one evidence class, explicit write boundary.
// The schema option is the G3 contract: the final channel is schema-valid JSON
// only. On an invalid first output the runtime runs schema correction
// automatically.
const workerSchema = {
  type: "object",
  required: ["summary", "evidence"],
  properties: { summary: { type: "string" }, evidence: { type: "string" } },
};
const worker = await agent(
  [
    "GOALCHAIN_WORKER",
    "CONTRACT=" + contractPath,
    "Deliver the bounded objective. Write machine-readable evidence as a JSON",
    "string under the `evidence` key with a top-level boolean true for every",
    "mechanical token: " + mechanicalTokens.join(", ") + ".",
  ].join("\n"),
  { label: "worker", role: "maker", timeoutSeconds: 900, schema: workerSchema },
);

// Land the evidence inside the declared write boundary. The evidence body is
// passed via the EVIDENCE environment variable (declared in the boundary
// allowlist), never interpolated into the script text, so quoting in the
// evidence cannot break the command; the value is redacted in the journal. The
// command is audited against meta.boundary: a write outside ./out would be
// recorded as a violation and block success.
const writeEvidence = await command("pwsh", [
  "-NoProfile", "-Command",
  "$ErrorActionPreference='Stop';" +
  "$p='" + evidencePath.replace(/'/g, "''") + "';" +
  "$dir=Split-Path -Parent $p; if($dir -and !(Test-Path -LiteralPath $dir)){ New-Item -ItemType Directory -Force -Path $dir | Out-Null };" +
  "Set-Content -LiteralPath $p -Value $env:EVIDENCE -Encoding utf8 -NoNewline;" +
  "'evidence-written'",
], { label: "write-evidence", timeoutSeconds: 60, env: { EVIDENCE: String(worker.evidence) } });
if (writeEvidence.exitCode !== 0 || writeEvidence.timedOut) {
  throw new Error("dispatch failed: cannot write evidence: " + (writeEvidence.stderr || writeEvidence.stdout));
}

// ---------------------------------------------------------------------------
phase("review-gate");
// G13: independent per-slice review runs as a CHILD WORKFLOW, not an external
// placeholder path and never self-review. The child has its own run id, its own
// journal, its own narrowed boundary, and an independent capability role; its
// outcome is persisted in the tree and attributed in the shared budget. A
// rejecting reviewer fails the chain before the mechanical gate.
const review = await workflow(reviewScript, {
  contractPath,
  evidencePath,
});
if (String(review.verdict).toLowerCase() !== "approve") {
  throw new Error("review-gate rejected: " + JSON.stringify(review));
}

// ---------------------------------------------------------------------------
phase("verification");
// Mechanical gate: read the landed evidence back from disk and require a
// top-level boolean true for every token. This is the half of the dual gate
// that checks identifiers, not meaning. It proves the evidence *landed inside
// the write boundary and parses with every token true*; it does NOT prove the
// worker's authored content is independently true — that is the semantic
// gate's job below. Reading from disk (not the worker's in-memory self-report)
// is what gives this gate teeth: a worker that claims success but fails to land
// a true token on disk is failed here.
const readEvidence = await command("pwsh", [
  "-NoProfile", "-Command",
  "$ErrorActionPreference='Stop';" +
  "$p='" + evidencePath.replace(/'/g, "''") + "';" +
  "if(!(Test-Path -LiteralPath $p)){ Write-Error 'evidence missing'; exit 21 };" +
  "Get-Content -LiteralPath $p -Raw -Encoding utf8",
], { label: "read-evidence", timeoutSeconds: 30 });
if (readEvidence.exitCode !== 0 || readEvidence.timedOut) {
  throw new Error("verification failed: evidence unreadable: " + (readEvidence.stderr || readEvidence.stdout));
}
let evidenceJson;
try {
  evidenceJson = JSON.parse(String(readEvidence.stdout).trim());
} catch (error) {
  throw new Error("verification failed: evidence is not JSON: " + String(error));
}
for (const token of mechanicalTokens) {
  if (evidenceJson[token] !== true) {
    throw new Error("mechanical gate failed: evidence token not true: " + token);
  }
}
const mechanical = { passed: true, tokens: mechanicalTokens, evidence: evidenceJson };

// ---------------------------------------------------------------------------
// Human acceptance. The gate parks the run WaitingHuman; a killed-and-restarted
// controller reconstructs the same tree via `watch` and resumes with `approve`.
// Journal replay makes every completed call above free on resume.
let human = null;
if (requireHumanGate) {
  phase("human-gate");
  human = await gate("Accept the delivery before the semantic gate?", {
    label: "accept-delivery",
    current: { summary: worker.summary, review, evidencePath },
    hint: "approve to run the semantic gate; reject to block the chain",
  });
  if (!human.approved) {
    throw new Error("human gate rejected the delivery");
  }
}

// ---------------------------------------------------------------------------
phase("semantic-gate");
// The second half of the dual gate: an independent reviewer judges meaning, not
// identifiers. It reads through host tools (no body inlining) and returns a
// schema-valid verdict.
const semanticSchema = {
  type: "object",
  required: ["approved", "rationale"],
  properties: { approved: { type: "boolean" }, rationale: { type: "string" } },
};
const semantic = await agent(
  [
    "GOALCHAIN_SEMANTIC",
    "CONTRACT=" + contractPath,
    "EVIDENCE=" + evidencePath,
    "REVIEW=" + JSON.stringify(review),
    "Judge whether the delivery satisfies the contract in meaning, not just",
    "tokens. Return approved=true only if scope, numbers, and prohibited",
    "actions all hold.",
  ].join("\n"),
  // noContinuation: the semantic gate is an independent semantic review, not a
  // continuation of the worker's session. Default-ON continuation threading
  // would otherwise let the maker and semantic roles share a session when they
  // resolve to the same agent/model (see B1). Forcing a cold review preserves
  // the semantic gate's independence in the same run, so the worker's prior
  // reasoning cannot prime the verdict on its own work.
  { label: "semantic-gate", role: "semantic", timeoutSeconds: 900, schema: semanticSchema, noContinuation: true },
);
if (semantic.approved !== true) {
  throw new Error("semantic gate rejected: " + JSON.stringify(semantic));
}

// ---------------------------------------------------------------------------
phase("writeback");
// Liber Null writeback and the reader-facing report stay controller-owned; this
// run returns the evidence the controller needs to close the item.
return {
  protocol: "goalchain",
  contractPath,
  evidencePath,
  mechanicalTokens,
  worker,
  review,
  mechanical,
  semantic,
  human,
};
