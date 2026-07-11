from pathlib import Path
import tomllib


def test_runtime_dependency_declares_local_servitor_contract():
    pyproject = Path(__file__).parents[1] / "pyproject.toml"
    metadata = tomllib.loads(pyproject.read_text(encoding="utf-8"))

    dependencies = metadata["project"]["dependencies"]
    assert "servitor>=0.1.0" in dependencies
    assert metadata["tool"]["pytest"]["ini_options"]["asyncio_default_fixture_loop_scope"] == "function"
