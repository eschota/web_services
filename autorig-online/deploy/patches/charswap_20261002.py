"""Stage the «Subgraph · Character swap» node into a hardlinked release (owner 2026-10-02).

python3 patch_charswap.py <release dir> <path to ai_character_swap.py>
Every touched file is removed before it is written (the release shares inodes with the live one).
"""
import hashlib
import os
import shutil
import sys

REL = os.path.join(sys.argv[1], "autorig-online")
MODULE = sys.argv[2]


def write(rel, text):
    path = os.path.join(REL, rel)
    if os.path.exists(path):
        os.remove(path)
    with open(path, "w", encoding="utf-8", newline="") as fh:
        fh.write(text)
    shutil.chown(path, "autorig", "autorig")
    os.chmod(path, 0o666)


def read(rel):
    with open(os.path.join(REL, rel), encoding="utf-8", newline="") as fh:
        return fh.read()


def sub(text, old, new, name):
    if text.count(old) != 1:
        sys.exit(f"{name}: expected one {old[:80]!r}, found {text.count(old)}")
    return text.replace(old, new)


# 1. the module
with open(MODULE, encoding="utf-8") as fh:
    write("backend/ai_character_swap.py", fh.read())

# 2. main.py: the router, gated by the admin check main.py already has
m = read("backend/main.py")
nl = "\r\n" if "\r\n" in m else "\n"
m = sub(m, "app.include_router(build_lora_admin_router(require_admin))" + nl,
        "app.include_router(build_lora_admin_router(require_admin))" + nl + nl
        + "from ai_character_swap import build_character_swap_router" + nl + nl
        + "# Subgraph · Character swap (2026-10-02): admins and scripts on this server start jobs." + nl
        + "app.include_router(build_character_swap_router(get_current_user, is_admin_email))" + nl, "main.py")
write("backend/main.py", m)

# 3. ai_services.py: the node in the catalogue
s = read("backend/ai_services.py")
nl = "\r\n" if "\r\n" in s else "\n"
block = '''

# ------------------------------------------------------------ Subgraph · Character swap
#
# (2026-10-02, owner: «сделай уже Subgraf … инпут-аутпут, а под капотом AI Vision, который добивается
# конкретного результата … сходство стиля, сюжета, но с заменой персонажа консистентно»). One node: the
# reference's look and story in, the character in, the reference re-made with the character out. Under the hood
# (ai_character_swap) Vision reads the reference, picks Z-Image + LoRA + depth or the Qwen canvas, scores every
# attempt side by side (look, story, character, anatomy) and corrects until the target is met. Admins only.
SERVICES.append({
    "id": "character_swap", "title": "Subgraph · Character swap", "path": "/nodes",
    "api": "/api/ai/character-swap", "status": "live", "slow": True,
    "summary": ("A subgraph: the reference's look and story with your character. Vision reads the reference, "
                "picks Z-Image + LoRA + depth or the Qwen canvas, scores every attempt side by side (look, story, "
                "character, anatomy) and corrects until the target is met. The report lists every attempt. "
                "Admins only."),
    "inputs": [
        {"type": IMAGE, "field": "image", "required": True, "title": "Reference (look + story)"},
        {"type": IMAGE, "field": "reference_2", "required": True, "title": "Character", "ref_index": 2},
        {"type": IMAGE, "field": "reference_3", "required": False, "title": "Character, second view",
         "ref_index": 3},
        {"type": TEXT, "field": "prompt", "required": False, "title": "Character look (words)"},
    ],
    "outputs": [
        {"type": IMAGE, "field": "image_url_string", "title": "Result"},
        {"type": TEXT, "field": "answer_string", "title": "Report"},
    ],
})
PARAMS["character_swap"] = [
    {"name": "lora", "title": "Character LoRA (photo engine)", "type": "text", "default": "",
     "help": "<lora:FILE:W>, e.g. <lora:sura_zit_A.safetensors:0.7>"},
    {"name": "clothing", "title": "Clothing", "type": "select", "default": "reference",
     "options": [{"value": "reference", "title": "As the reference"}, {"value": "nude", "title": "Nude"}]},
    {"name": "engine", "title": "Engine", "type": "select", "default": "auto",
     "options": [{"value": "auto", "title": "Auto (Vision decides)"},
                 {"value": "photo", "title": "Photo: Z-Image + LoRA + depth"},
                 {"value": "edit", "title": "Canvas: Qwen edit of the reference"}]},
    {"name": "target", "title": "Target score", "type": "range", "min": 5, "max": 10, "step": 0.5, "default": 8,
     "help": "look, story and character must all reach it"},
    {"name": "attempts", "title": "Attempts", "type": "number", "min": 1, "max": 8, "step": 1, "default": 5},
    {"name": "width", "title": "Width", "type": "number", "min": 0, "max": 2048, "step": 16, "default": 0,
     "help": "0 = the reference's aspect at ~1 MP"},
    {"name": "height", "title": "Height", "type": "number", "min": 0, "max": 2048, "step": 16, "default": 0},
    {"name": "seed", "title": "Seed", "type": "number", "min": 0, "max": 2147483647, "step": 1, "default": 0},
]
'''
write("backend/ai_services.py", s.rstrip("\r\n") + nl + block.replace("\n", nl))

# 4. ai-nodes.js: the runner (waits for the announced file, then reads the report's summary)
j = read("static/js/ai-nodes.js")
nl = "\r\n" if "\r\n" in j else "\n"
anchor = "    text_join: { api: '/api/ai/text-join', finish: pollAiStatus, field: 'answer_string', type: 'text' }," + nl
runner = (anchor
          + "    // Subgraph · Character swap (2026-10-02): the server runs the Vision loop and publishes the best" + nl
          + "    // attempt at the announced address; the report's summary is the text output." + nl
          + "    character_swap: { api: '/api/ai/character-swap', finish: async (accepted, runner, report) => {" + nl
          + "      const value = await pollForFile(accepted, runner, report);" + nl
          + "      const outputs = {image_url_string: value};" + nl
          + "      const rep = accepted.report_url_string" + nl
          + "        ? await fetch(accepted.report_url_string).then(r => (r.ok ? r.json() : null)).catch(() => null)" + nl
          + "        : null;" + nl
          + "      if (rep && rep.summary_string) outputs.answer_string = String(rep.summary_string);" + nl
          + "      return {value, outputs};" + nl
          + "    }, field: 'image_url_string', type: 'image' }," + nl)
j = sub(j, anchor, runner, "ai-nodes.js")
write("static/js/ai-nodes.js", j)

# 5. nodes.html: the script's cache key is the first 10 hex of its sha1
version = hashlib.sha1(j.encode("utf-8")).hexdigest()[:10]
h = read("static/nodes.html")
import re
old = re.search(r'/static/js/ai-nodes\.js\?v=([0-9a-f]+)"', h)
if not old:
    sys.exit("nodes.html: no versioned ai-nodes.js")
h = h.replace(old.group(0), f'/static/js/ai-nodes.js?v={version}"')
write("static/nodes.html", h)
print("staged", version)
