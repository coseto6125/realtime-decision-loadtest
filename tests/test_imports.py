import importlib

import pytest


@pytest.mark.parametrize("mod", ["collector", "context", "decision", "model", "payments", "scheduler"])
def test_service_module_imports(mod):
    assert importlib.import_module(f"lt.{mod}").app is not None
