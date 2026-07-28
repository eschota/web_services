from pathlib import Path


STATIC_ROOT = Path(__file__).resolve().parents[2] / "static"


def test_model_sale_download_blocking_does_not_observe_its_own_class_changes():
    source = (STATIC_ROOT / "js" / "model-sale-offers.js").read_text(encoding="utf-8")

    assert "model-sale-downloads-blocked" in source
    assert "MutationObserver" not in source
    assert "document.documentElement.classList.add('model-sale-downloads-blocked')" in source


def test_task_page_busts_the_cached_recursive_observer_script():
    task_html = (STATIC_ROOT / "task.html").read_text(encoding="utf-8")

    assert "/static/js/model-sale-offers.js?v=20260728-1" in task_html
