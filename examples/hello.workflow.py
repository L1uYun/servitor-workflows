"""Hello workflow: two agents in parallel, one plain, one schema-constrained."""
meta = {
    "name": "hello",
    "description": "Two agents in parallel: one plain string, one schema-constrained",
}


async def main(agent, parallel, pipeline, phase, log, budget, args, human, workflow):
    phase("Answer")

    pong, capital = await parallel([
        lambda: agent("Reply with exactly one word: pong. No punctuation, nothing else.", {"effort": "low"}),
        lambda: agent(
            "What is the capital of France? Respond using the provided schema.",
            {
                "effort": "low",
                "schema": {
                    "type": "object",
                    "additionalProperties": False,
                    "required": ["capital"],
                    "properties": {"capital": {"type": "string"}},
                },
            },
        ),
    ])

    log(f"plain   -> {pong}")
    log(f"schema  -> {capital}")

    return {"pong": pong, "capital": capital}
