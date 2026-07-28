export const meta = {
  name: "goalchain-review",
  description: "Independent per-slice reviewer (G13): its own run, read-only boundary",
  contract: "workflow",
  // The reviewer is read-only. writePaths is empty, which is a strict narrowing
  // of the parent chain's ./out write surface — the reviewer cannot land files,
  // only judge. network stays "allow" because the reviewer agent is itself a
  // transport submission (audited as network use). This is an audit boundary,
  // not an OS sandbox.
  boundary: {
    readPaths: ["."],
    writePaths: [],
    network: "allow",
    environment: { allow: [] },
  },
  // Capabilities are inherited from the parent chain (no meta.capabilities
  // here): the child may only narrow, never widen, and inheriting keeps the
  // reviewer on the same declared candidate set. Independence from the maker is
  // enforced structurally — the reviewer runs in this separate run, so the
  // maker's model choice is never even a candidate in this run's capability
  // events.
};

// args (passed by the parent goalchain.workflow.js):
//   contractPath: string
//   evidencePath: string

const contractPath = String((args && args.contractPath) || "contract.md");
const evidencePath = String((args && args.evidencePath) || "./out/evidence.json");

phase("review");
// G13 + G3: an independent reviewer judges the slice and ends schema-valid.
// It reads the contract and evidence through host tools (no body inlining);
// path-only references are the contract, the model does the reading.
const reviewSchema = {
  type: "object",
  required: ["verdict", "critique", "must_fix"],
  properties: {
    verdict: { type: "string" },
    critique: { type: "string" },
    must_fix: { type: "array", items: { type: "string" } },
  },
};
const review = await agent(
  [
    "GOALCHAIN_REVIEW",
    "CONTRACT=" + contractPath,
    "EVIDENCE=" + evidencePath,
    "Independently review the delivery against the contract. Read both through",
    "host tools. verdict must be one of: approve, reject, revise. Reject only",
    "with concrete must_fix items.",
  ].join("\n"),
  { label: "independent-review", role: "reviewer", timeoutSeconds: 900, schema: reviewSchema },
);

return {
  protocol: "goalchain-review",
  verdict: review.verdict,
  critique: review.critique,
  must_fix: review.must_fix,
};
