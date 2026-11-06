"""V3 intake keeps OBJ groups (Voxel parts · V3, owner 2026-10-14: held props «должны определяться на основе анализа
MTL OBJ ID»): `o body` + `o sword` -> two meshes / nodes named by the group, primitives per usemtl, names in
extras.autorig; an OBJ without groups converts exactly as before."""
import hashlib
import json
import pathlib
import struct
import sys
import unittest

BACKEND = pathlib.Path(__file__).resolve().parents[1]
if str(BACKEND) not in sys.path:
    sys.path.insert(0, str(BACKEND))

import v3_intake as intake


def read(glb: bytes):
    n = struct.unpack("<I", glb[12:16])[0]
    return json.loads(glb[20:20 + n])


GROUPED = b"""mtllib x.mtl
o body
v 0 0 0
v 1 0 0
v 1 1 0
v 0 1 0
vt 0 0
vt 1 0
vt 1 1
vt 0 1
usemtl Skin
f 1/1 2/2 3/3 4/4
o sword
v 1 0.5 0
v 2 0.5 0
v 2 0.6 0
usemtl Blade
f 5/1 6/2 7/3
usemtl Grip
f 2/1 5/2 7/3
"""

PLAIN = b"""v 0 0 0
v 1 0 0
v 1 1 0
v 0 1 0
usemtl A
f 1 2 3
usemtl B
f 1 3 4
"""


class ObjGroupsTest(unittest.TestCase):
    def test_groups_become_meshes(self):
        doc = read(intake.obj_to_glb(GROUPED))
        self.assertEqual([m["name"] for m in doc["meshes"]], ["body", "sword"])
        self.assertEqual([n["name"] for n in doc["nodes"]], ["body", "sword"])
        self.assertEqual(doc["scenes"][0]["nodes"], [0, 1])
        self.assertEqual(len(doc["meshes"][0]["primitives"]), 1)
        self.assertEqual(len(doc["meshes"][1]["primitives"]), 2)
        self.assertEqual([m["name"] for m in doc["materials"]], ["Skin", "Blade", "Grip"])
        sword = doc["meshes"][1]
        self.assertEqual(sword["extras"]["autorig"]["source_group"], "sword")
        self.assertEqual([p["extras"]["autorig"]["source_material"] for p in sword["primitives"]], ["Blade", "Grip"])
        self.assertEqual(doc["nodes"][1]["extras"]["autorig"]["source_group"], "sword")
        # the sword's primitives share its own compact vertices: its 3 corners, the body's vertex 2 and corner 5 with
        # another UV (Grip face) = 5
        pos = {p["attributes"]["POSITION"] for p in sword["primitives"]}
        self.assertEqual(len(pos), 1)
        self.assertEqual(doc["accessors"][pos.pop()]["count"], 5)
        self.assertEqual(doc["accessors"][doc["meshes"][0]["primitives"][0]["attributes"]["POSITION"]]["count"], 4)
        self.assertIn("TEXCOORD_0", sword["primitives"][0]["attributes"])

    def test_plain_obj_unchanged(self):
        out = intake.obj_to_glb(PLAIN)
        doc = read(out)
        self.assertEqual(len(doc["meshes"]), 1)
        self.assertEqual(doc["nodes"], [{"mesh": 0, "name": "model"}])
        self.assertEqual([m["name"] for m in doc["materials"]], ["A", "B"])
        self.assertEqual(doc["asset"]["generator"], "AutoRig V3 intake obj_to_glb")
        # byte-identical to the converter before groups were kept (sha256 of this exact input, 2026-10-14)
        self.assertEqual(hashlib.sha256(out).hexdigest(), PLAIN_SHA)

    def test_single_group_is_the_old_path(self):
        doc = read(intake.obj_to_glb(b"o only\n" + PLAIN))
        self.assertEqual(doc["nodes"], [{"mesh": 0, "name": "model"}])


PLAIN_SHA = "f1ff5d84845a7125e619251a5e2d95649f09054a24600d01819ef0720a4f9bb7"

if __name__ == "__main__":
    unittest.main()
