"""The links every Civitai post's description ends with come from a live file (no file = none)."""
import json
import os
import tempfile

os.environ.setdefault("AUTORIG_SCRATCH_DIR", tempfile.mkdtemp(prefix="civfooter-"))
import ai_civitai_post as civ  # noqa: E402


def _with_file(tmp_path, data):
    path = os.path.join(str(tmp_path), "civitai-post.json")
    with open(path, "w", encoding="utf-8") as fh:
        fh.write(data if isinstance(data, str) else json.dumps(data))
    civ.POST_FOOTER_FILE = civ.Path(path)
    civ._FOOTER_CACHE.update(mtime=None, links=[])
    return path


def test_no_file_no_links(tmp_path=None):
    tmp_path = tmp_path or tempfile.mkdtemp()
    civ.POST_FOOTER_FILE = civ.Path(os.path.join(str(tmp_path), "absent.json"))
    civ._FOOTER_CACHE.update(mtime=None, links=[])
    out = civ.description_html("A line", [], False)
    assert out == ('<p>A line</p><p>Made with AutoRig nodes: <a href="https://autorig.online/nodes">'
                   'autorig.online/nodes</a></p>')


def test_link_goes_before_the_autorig_line(tmp_path=None):
    tmp_path = tmp_path or tempfile.mkdtemp()
    _with_file(tmp_path, {"footer_links": [{"text": "Workflow <one>", "url": "https://example.org/models/1/name?v=2"}]})
    out = civ.description_html("", ["note"], True)
    assert ('<p>Workflow &lt;one&gt;: <a href="https://example.org/models/1/name?v=2">example.org/models/1/name</a></p>'
            '<p>Made with AutoRig nodes') in out
    assert out.index("Upscaled 2x") < out.index("Workflow &lt;one&gt;")


def test_bad_entries_are_dropped(tmp_path=None):
    tmp_path = tmp_path or tempfile.mkdtemp()
    _with_file(tmp_path, {"footer_links": [{"text": "x", "url": "http://plain.example/"}, {"text": "", "url": "https://a.example"},
                                          "junk", {"text": "ok", "url": "https://ok.example/p"}]})
    assert civ.footer_links() == [{"text": "ok", "url": "https://ok.example/p"}]
    _with_file(tmp_path, "{not json")
    assert civ.footer_links() == []


if __name__ == "__main__":
    for name, fn in list(globals().items()):
        if name.startswith("test_"):
            fn()
            print("ok", name)
    print("PASS")
