export const meta = {
  name: 'batch-research',
  description: 'Fan-out parallel web research on multiple dimensions, verify key claims, synthesize',
  whenToUse: 'Grill research interlude: multiple open decision branches need external evidence before the user can choose. Pass dimensions as args.',
  contract: 'workflow',
  // Web research requires an agent that can actually search. pi runs
  // `pi --offline` and has no web tools; route to codex (web_search = live) or
  // claude instead. Override per call via args.researchAgent / args.verifyAgent.
  boundary: {
    readPaths: ["."],
    network: "allow",
  },
  phases: [
    { title: 'Research', detail: 'One agent per dimension, parallel web search' },
    { title: 'Verify', detail: 'Adversarially check top claims from each dimension' },
    { title: 'Synthesize', detail: 'Merge into a decision-ready brief' },
  ],
}

// args: {
//   question: string,
//   dimensions: [{key, prompt}],
//   context?: string,
//   researchAgent?: string,  // default "codex" — agent able to web search
//   verifyAgent?: string,    // default "codex"
//   synthAgent?: string,     // default "codex"
// }
const { question, dimensions, context } = args || {}
const RESEARCH_AGENT = (args && args.researchAgent) || 'codex'
const VERIFY_AGENT = (args && args.verifyAgent) || 'codex'
const SYNTH_AGENT = (args && args.synthAgent) || 'codex'

if (!dimensions || !dimensions.length) {
  throw new Error('args.dimensions required: [{key: "rendering", prompt: "..."}, ...]')
}

const CONTEXT = context ? `\n\nProject context:\n${context}` : ''

// --- Phase 1: parallel research ---
phase('Research')
log(`Researching ${dimensions.length} dimensions for: ${question}`)

const findings = await parallel(dimensions.map(d => () =>
  agent(
    `You are a technical researcher. Use WebSearch and WebFetch to gather current evidence (2025-2026).

Research dimension: ${d.key}
Focus: ${d.prompt}
${CONTEXT}

Requirements:
- Search multiple queries, fetch at least 2-3 primary sources (GitHub READMEs, docs, benchmarks)
- Prefer quantitative data (latency ms, VRAM GB, RTF, fps, pricing) over vibes
- Note source URLs for key claims
- Output a structured markdown brief: findings table + key tradeoffs + your recommendation

Return ONLY the research brief as your final text.`,
    { label: `research:${d.key}`, phase: 'Research', agent: RESEARCH_AGENT }
  )
))

const validFindings = findings.filter(Boolean)
if (!validFindings.length) throw new Error('All research agents failed')

// --- Phase 2: verify top claims ---
phase('Verify')

const verifyTargets = dimensions.map((d, i) => ({
  key: d.key,
  brief: validFindings[i] || '(agent failed)',
})).filter(x => x.brief !== '(agent failed)')

const verified = await parallel(verifyTargets.map(v => () =>
  agent(
    `You are a skeptical fact-checker. Below is a research brief on "${v.key}".

${v.brief}

Your job:
1. Pick the 2-3 most decision-critical quantitative claims (latency, VRAM, pricing, compatibility)
2. Use WebSearch to independently verify or refute each
3. Flag any claim that is outdated, exaggerated, or missing caveats

Return a short verdict per claim: CONFIRMED / REFUTED / UNCERTAIN + correction if needed.`,
    { label: `verify:${v.key}`, phase: 'Verify', agent: VERIFY_AGENT }
  )
))

// --- Phase 3: synthesize ---
phase('Synthesize')

const synthesis = await agent(
  `You are a technical advisor preparing a decision brief for a developer.

Original question: ${question}
${CONTEXT}

Research findings by dimension:
${verifyTargets.map((v, i) => `### ${v.key}\n${v.brief}\n\nVerification:\n${verified[i] || '(skipped)'}`).join('\n\n---\n\n')}

Produce a final decision brief:
1. Per dimension: 2-3 viable options ranked, with hard numbers, one-line recommendation
2. Cross-dimension interactions (e.g. shell choice constrains rendering choice)
3. A "decision list" template: each open question phrased as a single-choice question with recommended answer marked
4. Remaining unknowns that need the user's preference (not facts)

Keep it under 2000 words. Be direct. No filler.`,
  { label: 'synthesize', phase: 'Synthesize', agent: SYNTH_AGENT }
)

return { question, dimensions: dimensions.map(d => d.key), synthesis, rawFindings: validFindings, verifications: verified.filter(Boolean) }
