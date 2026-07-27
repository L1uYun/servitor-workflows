export const meta = {
  name: "dynamic-real-chain",
  description: "Discover work, fan out, and summarize",
  contract: "workflow.v2",
};

phase("discover");
const found = await agent("Return JSON with items containing exactly [\"alpha\", \"beta\"].", {
  label: "discover",
  schema: {
    type: "object",
    required: ["items"],
    properties: {
      items: { type: "array", items: { type: "string" } },
    },
  },
  timeoutSeconds: 120,
});

phase("fan-out");
const results = await pipeline(found.items, item =>
  agent(`INPUT_JSON=${JSON.stringify(item)}. Return the JSON boolean true because this literal input is a non-empty string. Do not inspect anything.`, {
    label: item,
    timeoutSeconds: 120,
    schema: { type: "boolean" },
  }),
);

return {
  found: found.items,
  results: found.items.map((item, index) =>
    results[index] ? `${item}-ok` : `${item}-failed`,
  ),
};
