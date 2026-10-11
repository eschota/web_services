"""Texturing · V3 at intake: OBJ + MTL textures embedded, missing files named, broken normals rebuilt, safe ZIP."""
import io
import json
import struct
import sys
import tempfile
import unittest
import zipfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import v3_textures as T  # noqa: E402

PNG_1PX = bytes.fromhex("89504e470d0a1a0a0000000d4948445200000001000000010806000000"
                        "1f15c4890000000d49444154789c6360f8cfc0f01f0005000201a5d4b4c40000000049454e44ae426082")

OBJ = """mtllib box.mtl
v 0 0 0
v 1 0 0
v 1 1 0
v 0 1 0
v 0 0 1
v 1 0 1
v 1 1 1
v 0 1 1
vt 0 0
vt 1 0
vt 1 1
vt 0 1
vn 0 1 0
usemtl Skin
f 1/1/1 2/2/1 3/3/1
f 1/1/1 3/3/1 4/4/1
f 5/1/1 7/3/1 6/2/1
f 5/1/1 8/4/1 7/3/1
f 1/1/1 5/2/1 6/3/1
f 1/1/1 6/3/1 2/4/1
f 4/1/1 3/2/1 7/3/1
f 4/1/1 7/3/1 8/4/1
f 1/1/1 4/2/1 8/3/1
f 1/1/1 8/3/1 5/4/1
f 2/1/1 6/2/1 7/3/1
f 2/1/1 7/3/1 3/4/1
"""


def glb_doc(data):
    n = struct.unpack_from("<I", data, 12)[0]
    return json.loads(data[20:20 + n])


class TextureIntake(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.d = Path(self.tmp.name)
        (self.d / "box.obj").write_text(OBJ)

    def tearDown(self):
        self.tmp.cleanup()

    def test_missing_library_named(self):
        glb, audit = T.normalize_textured(self.d / "box.obj")
        self.assertEqual(audit["status"], "missing_files")
        self.assertIn("box.mtl", audit["missing"])
        self.assertEqual(audit["normals"]["state"], "rebuilt")      # every vn = 0 1 0 on a cube

    def test_mtl_and_texture_embedded(self):
        (self.d / "box.mtl").write_text("newmtl Skin\nKd 0.5 0.4 0.3\nmap_Kd textures\\skin.png\nmap_Bump -bm 1 n.png\n")
        (self.d / "skin.png").write_bytes(PNG_1PX)
        glb, audit = T.normalize_textured(self.d / "box.obj")
        self.assertEqual(audit["missing"], ["n.png"])
        self.assertEqual(audit["embedded"], ["skin.png"])
        doc = glb_doc(glb)
        mat = doc["materials"][0]
        self.assertIn("baseColorTexture", mat["pbrMetallicRoughness"])
        self.assertEqual(doc["images"][0]["mimeType"], "image/png")
        (self.d / "n.png").write_bytes(PNG_1PX)
        glb, audit = T.normalize_textured(self.d / "box.obj")
        self.assertEqual(audit["status"], "source_ok")
        self.assertIn("normalTexture", glb_doc(glb)["materials"][0])

    def test_zip_is_unpacked_safely(self):
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w") as z:
            z.writestr("model/box.obj", OBJ)
            z.writestr("../evil.txt", "x")
            z.writestr("/abs.txt", "x")
        zp = self.d / "up.zip"
        zp.write_bytes(buf.getvalue())
        files = T.safe_extract(zp, self.d / "up_zip")
        self.assertEqual([p.name for p in files], ["box.obj"])
        self.assertFalse((self.d / "evil.txt").exists())
        self.assertEqual(T.pick_model(files).name, "box.obj")

    def test_find_file_by_basename(self):
        (self.d / "sub").mkdir()
        (self.d / "sub" / "Skin.PNG").write_bytes(PNG_1PX)
        self.assertEqual(T.find_file("C:\\work\\skin.png", self.d).name, "Skin.PNG")
        self.assertEqual(T.find_file("skin.tga", self.d).name, "Skin.PNG")       # shipped as PNG instead


if __name__ == "__main__":
    unittest.main()
