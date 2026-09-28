import ast
from pathlib import Path


def test_worker_role_ordering_imports_url_normalizer():
    """The live scheduler must not resolve this helper only when dispatch runs."""
    tree = ast.parse((Path(__file__).parents[1] / "main.py").read_text(encoding="utf-8"))
    imported_from_workers = {
        alias.name
        for node in ast.walk(tree)
        if isinstance(node, ast.ImportFrom) and node.module == "workers"
        for alias in node.names
    }

    assert "normalize_worker_url_key" in imported_from_workers
