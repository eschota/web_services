"""ASCII FBX intake: the exporter repair and the assimp GLB conversion (assimp test runs where it is installed)."""
import pathlib
import shutil
import struct
import sys
import tempfile
import unittest

BACKEND = pathlib.Path(__file__).resolve().parents[1]
if str(BACKEND) not in sys.path:
    sys.path.insert(0, str(BACKEND))

import fbx_ascii

# One triangle; the Vertices array lacks its `a:` key like AssetStudio exports.
TRIANGLE = """; FBX 7.3.0 project file
; ----------------------------------------------------

FBXHeaderExtension:  {
\tFBXHeaderVersion: 1003
\tFBXVersion: 7300
\tCreator: "AutoRig intake test"
}
GlobalSettings:  {
\tVersion: 1000
\tProperties70:  {
\t\tP: "UpAxis", "int", "Integer", "",1
\t\tP: "UpAxisSign", "int", "Integer", "",1
\t\tP: "FrontAxis", "int", "Integer", "",2
\t\tP: "FrontAxisSign", "int", "Integer", "",1
\t\tP: "CoordAxis", "int", "Integer", "",0
\t\tP: "CoordAxisSign", "int", "Integer", "",1
\t\tP: "UnitScaleFactor", "double", "Number", "",1
\t}
}
Definitions:  {
\tVersion: 100
\tCount: 2
\tObjectType: "Model" {
\t\tCount: 1
\t}
\tObjectType: "Geometry" {
\t\tCount: 1
\t}
}
Objects:  {
\tGeometry: 1000, "Geometry::tri", "Mesh" {
\t\tVertices: *9 {
\t\t\t0,0,0,1,0,0,0,1,0
\t\t}
\t\tPolygonVertexIndex: *3 {
\t\t\ta: 0,1,-3
\t\t}
\t\tGeometryVersion: 124
\t}
\tModel: 2000, "Model::tri", "Mesh" {
\t\tVersion: 232
\t\tProperties70:  {
\t\t}
\t}
}
Connections:  {
\tC: "OO",1000,2000
\tC: "OO",2000,0
}
"""


class FbxAsciiTests(unittest.TestCase):
    def tmp(self):
        return tempfile.TemporaryDirectory()

    def test_repair_inserts_only_missing_array_keys(self):
        fixed, inserted = fbx_ascii.repair_ascii_fbx(TRIANGLE)
        self.assertEqual(inserted, 1)
        self.assertIn("\t\t\ta: 0,0,0,1,0,0,0,1,0", fixed)
        self.assertEqual(fixed.count("a: 0,1,-3"), 1)                     # an existing key stays single
        again, second = fbx_ascii.repair_ascii_fbx(fixed)
        self.assertEqual((again, second), (fixed, 0))                      # idempotent

    def test_detection_is_by_content_not_name(self):
        with self.tmp() as tmp:
            root = pathlib.Path(tmp)
            (root / "a.fbx").write_text(TRIANGLE)
            (root / "bom.fbx").write_bytes(b"\xef\xbb\xbf" + TRIANGLE.encode())
            (root / "binary.fbx").write_bytes(b"Kaydara FBX Binary  \x00\x1a\x00" + b"\x00" * 32)
            (root / "model.glb").write_bytes(b"glTF" + struct.pack("<II", 2, 12))
            self.assertEqual([fbx_ascii.is_ascii_fbx(root / n) for n in ("a.fbx", "bom.fbx", "binary.fbx", "model.glb")],
                             [True, True, False, False])
            with self.assertRaises(fbx_ascii.FbxAsciiError):
                fbx_ascii.ascii_fbx_to_glb(root / "binary.fbx", root / "out.glb")

    @unittest.skipUnless(shutil.which(fbx_ascii.ASSIMP) or pathlib.Path(fbx_ascii.ASSIMP).is_file(),
                         "assimp is not installed here")
    def test_repaired_ascii_fbx_becomes_a_glb_and_the_original_stays(self):
        with self.tmp() as tmp:
            root = pathlib.Path(tmp)
            src = root / "upload.fbx"
            src.write_text(TRIANGLE)
            before = src.read_bytes()
            receipt = fbx_ascii.ascii_fbx_to_glb(src, root / "upload.glb")
            data = (root / "upload.glb").read_bytes()
            self.assertEqual(data[:4], b"glTF")
            self.assertEqual((receipt["inserted_a_keys"], receipt["meshes"]), (1, 1))
            self.assertEqual(src.read_bytes(), before)


if __name__ == "__main__":
    unittest.main()
