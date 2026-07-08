"""Hello smoke: one agent, one word reply. For real provider testing."""
meta = {
    "name": "hello-smoke",
    "description": "Single agent smoke test using the default provider",
}


async def main(agent, parallel, pipeline, phase, log, budget, args, human, workflow):
    phase("Smoke")
    result = await agent(
        "Reply with exactly one word: pong. No punctuation, nothing else.",
        {"label": "pong-test"},
    )
    log(f"result: {result}")
    return {"pong": result}
