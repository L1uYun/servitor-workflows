export const meta = {
  name: "spawn-fanout",
  description:
    "Runtime fan-out: spawn N inline child workflows from a single spawn() call.",
  contract: "workflow",
};

// args.count (default 3) determines the fan-out width at runtime — the child
// specs are built from args, not hardcoded at authoring time. Each child is an
// independent workflow run (own journal, boundary, budget attribution) and
// returns its index. spawn() returns an array of {runId, result} objects.
const count = Math.max(1, Number(args.count ?? 3));

const specs = [];
for (let i = 0; i < count; i++) {
  specs.push({
    inline: `export const meta = { name: "spawn-child", contract: "workflow" }; return { i: ${i}, echo: args.label ?? "child" };`,
    args: { label: "child" },
  });
}

const results = await spawn(specs);

return {
  count,
  runs: results.map((entry) => ({ runId: entry.runId, i: entry.result.i })),
};
