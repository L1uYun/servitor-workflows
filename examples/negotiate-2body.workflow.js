export const meta = {
  name: "negotiate-2body",
  description: "Two-body proposal/review loop with synthesizer decision (C6 canary)",
  contract: "workflow",
};

// args:
//   topic: string (required for useful runs; default provided)
//   maxRounds?: number (default 2, hard cap 4)
//   requireHumanGate?: boolean
//   agent?: string (default pi)
//   timeoutSeconds?: number (default 180)

const topic = (args && args.topic) || "Choose one minimal local canary for multi-agent negotiation";
const maxRounds = Math.min(Math.max(Number((args && args.maxRounds) || 2), 1), 4);
const requireHumanGate = !!(args && args.requireHumanGate);
const agentName = (args && args.agent) || "pi";
const timeoutSeconds = Number((args && args.timeoutSeconds) || 180);

const proposalSchema = {
  type: "object",
  required: ["proposal", "assumptions", "confidence"],
  properties: {
    proposal: { type: "string" },
    assumptions: { type: "array", items: { type: "string" } },
    confidence: { type: "number" },
  },
};

const reviewSchema = {
  type: "object",
  required: ["verdict", "critique", "must_fix"],
  properties: {
    verdict: { type: "string" },
    critique: { type: "string" },
    must_fix: { type: "array", items: { type: "string" } },
  },
};

const decisionSchema = {
  type: "object",
  required: ["accepted", "decision", "rationale", "open_issues"],
  properties: {
    accepted: { type: "boolean" },
    decision: { type: "string" },
    rationale: { type: "string" },
    open_issues: { type: "array", items: { type: "string" } },
  },
};

const history = [];
let latestProposal = null;
let latestReview = null;
let stopReason = "max_rounds";

phase("negotiate");
for (let round = 1; round <= maxRounds; round++) {
  const prior = history.length
    ? `PRIOR=${JSON.stringify(history)}`
    : "PRIOR=[]";

  latestProposal = await agent(
    [
      "NEGOTIATE_PROPOSE",
      "ROLE=ROLE_A",
      `ROUND=${round}`,
      `TOPIC=${topic}`,
      prior,
      "Return bare JSON only with keys proposal, assumptions, confidence.",
      "If PRIOR includes critique/must_fix, revise the proposal to address them.",
    ].join("\n"),
    {
      label: `propose-r${round}`,
      agent: agentName,
      timeoutSeconds,
      schema: proposalSchema,
      systemPrompt:
        "You are ROLE_A proposer. Be concrete and minimal. Output bare JSON only.",
    },
  );

  latestReview = await agent(
    [
      "NEGOTIATE_REVIEW",
      "ROLE=ROLE_B",
      `ROUND=${round}`,
      `TOPIC=${topic}`,
      `PROPOSAL_JSON=${JSON.stringify(latestProposal)}`,
      "Return bare JSON only with keys verdict, critique, must_fix.",
      "verdict must be one of: accept, reject, revise.",
      "Accept only if the proposal is actionable and assumptions are explicit.",
    ].join("\n"),
    {
      label: `review-r${round}`,
      agent: agentName,
      timeoutSeconds,
      schema: reviewSchema,
      systemPrompt:
        "You are ROLE_B reviewer. Skeptical but constructive. Output bare JSON only.",
    },
  );

  history.push({
    round,
    proposal: latestProposal,
    review: latestReview,
  });

  const verdict = String(latestReview.verdict || "").toLowerCase();
  if (verdict === "accept") {
    stopReason = "reviewer_accept";
    break;
  }
}

phase("synthesize");
const decision = await agent(
  [
    "NEGOTIATE_SYNTH",
    `TOPIC=${topic}`,
    `HISTORY_JSON=${JSON.stringify(history)}`,
    `STOP_REASON=${stopReason}`,
    "Return bare JSON only with keys accepted, decision, rationale, open_issues.",
    "If the final review accepted, set accepted=true and consolidate the proposal.",
    "If not accepted, set accepted=false and list residual blockers in open_issues.",
  ].join("\n"),
  {
    label: "synthesize",
    agent: agentName,
    timeoutSeconds,
    schema: decisionSchema,
    systemPrompt:
      "You are synthesizer. Prefer the last proposal that addressed must_fix. Output bare JSON only.",
  },
);

let human = null;
if (requireHumanGate) {
  phase("human-gate");
  human = await gate("Approve multi-agent negotiation decision?", {
    label: "negotiate-decision",
    current: { decision, history, stopReason },
    hint: "approve to accept synthesizer decision; reject to block",
  });
}

return {
  protocol: "negotiate-2body",
  topic,
  maxRounds,
  stopReason,
  rounds: history.length,
  history,
  decision,
  human,
};
