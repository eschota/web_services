#!/usr/bin/env python3
import copy
import json
import struct
import unittest
from pathlib import Path

import numpy as np

import direct_glb_head_reader as reader


WORK=Path(__file__).resolve().parents[4]/".work"/"hair-lab-reader-tests"


def pad4(data,byte=b"\0"):
    return data+byte*((-len(data))%4)


def fixture():
    chunks=[]; views=[]; accessors=[]
    def add(array,component,type_name,normalized=False):
        raw=pad4(array.tobytes()); offset=sum(len(x) for x in chunks); chunks.append(raw); views.append({"buffer":0,"byteOffset":offset,"byteLength":array.nbytes}); spec={"bufferView":len(views)-1,"componentType":component,"count":len(array),"type":type_name}
        if normalized: spec["normalized"]=True
        accessors.append(spec); return len(accessors)-1
    position=add(np.array([[0,0,0],[1,0,0],[0,1,0]],np.float32),5126,"VEC3")
    normal=add(np.array([[0,0,1]]*3,np.float32),5126,"VEC3")
    joints=add(np.array([[0,0,0,0]]*3,np.uint8),5121,"VEC4")
    weights=add(np.array([[1,0,0,0]]*3,np.float32),5126,"VEC4")
    indices=add(np.array([0,1,2],np.uint16),5123,"SCALAR")
    binary=b"".join(chunks)
    doc={"asset":{"version":"2.0"},"buffers":[{"byteLength":len(binary)}],"bufferViews":views,"accessors":accessors,"meshes":[{"primitives":[{"attributes":{"POSITION":position,"NORMAL":normal,"JOINTS_0":joints,"WEIGHTS_0":weights},"indices":indices}]}],"nodes":[{"name":"Head"},{"mesh":0,"skin":0}],"skins":[{"joints":[0]}],"scenes":[{"nodes":[0,1]}],"scene":0}
    return doc,binary


def write_glb(path,doc,binary,duplicate_json=False):
    js=pad4(json.dumps(doc,separators=(",",":")).encode(),b" "); chunks=[struct.pack("<II",len(js),0x4E4F534A)+js]
    if duplicate_json: chunks.append(struct.pack("<II",len(js),0x4E4F534A)+js)
    binary=pad4(binary); chunks.append(struct.pack("<II",len(binary),0x004E4942)+binary); body=b"".join(chunks); path.write_bytes(struct.pack("<4sII",b"glTF",2,12+len(body))+body)


class DirectReaderTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls): WORK.mkdir(parents=True,exist_ok=True)

    def build(self,name,mutate=None):
        doc,binary=fixture()
        if mutate: mutate(doc,binary)
        path=WORK/name; write_glb(path,doc,binary); return path

    def test_valid_and_provenance(self):
        arrays,receipt=reader.read_head(self.build("valid.glb"))
        self.assertEqual(arrays["faces"].shape,(1,3)); self.assertEqual(int(arrays["node_ids"][0]),1); self.assertEqual(int(arrays["triangle_ids"][0]),0); self.assertIn("bind-mesh",receipt["geometry_state"])

    def assertRejected(self,name,mutate,pattern):
        with self.assertRaisesRegex(ValueError,pattern): reader.read_head(self.build(name,mutate))

    def test_secondary_weights_rejected(self):
        self.assertRejected("joints1.glb",lambda d,b:d["meshes"][0]["primitives"][0]["attributes"].update({"JOINTS_1":2}),"secondary")

    def test_joint_range_rejected(self):
        def mutate(d,b): b[d["bufferViews"][2]["byteOffset"]]=5
        doc,binary=fixture(); binary=bytearray(binary); mutate(doc,binary); path=WORK/"joint-range.glb"; write_glb(path,doc,bytes(binary))
        with self.assertRaisesRegex(ValueError,"joint index"): reader.read_head(path)

    def test_negative_weight_rejected(self):
        doc,binary=fixture(); binary=bytearray(binary); struct.pack_into("<f",binary,doc["bufferViews"][3]["byteOffset"],-1.0); path=WORK/"negative-weight.glb"; write_glb(path,doc,bytes(binary))
        with self.assertRaisesRegex(ValueError,"negative"): reader.read_head(path)

    def test_index_type_rejected(self):
        self.assertRejected("index-type.glb",lambda d,b:d["accessors"][4].update({"componentType":5126}),"indices")

    def test_non_affine_rejected(self):
        self.assertRejected("matrix.glb",lambda d,b:d["nodes"][1].update({"matrix":[1,0,0,1,0,1,0,0,0,0,1,0,0,0,0,1]}),"non-affine")

    def test_duplicate_json_rejected(self):
        doc,binary=fixture(); path=WORK/"duplicate.glb"; write_glb(path,doc,binary,True)
        with self.assertRaisesRegex(ValueError,"duplicate JSON"): reader.read_head(path)

    def test_unaligned_glb_rejected(self):
        path=self.build("unaligned.glb"); data=bytearray(path.read_bytes()); data.extend(b"x"); struct.pack_into("<I",data,8,len(data)); path.write_bytes(data)
        with self.assertRaisesRegex(ValueError,"unaligned"): reader.read_head(path)

    def test_negative_accessor_offset_rejected(self):
        doc,binary=fixture(); doc["accessors"][0]["byteOffset"]=-1
        with self.assertRaisesRegex(ValueError,"offset"): reader.accessor(doc,binary,0)

    def test_view_buffer_rejected(self):
        self.assertRejected("buffer.glb",lambda d,b:d["bufferViews"][0].update({"buffer":1}),"buffer 0")

    def test_threshold_rejected(self):
        with self.assertRaisesRegex(ValueError,"threshold"): reader.read_head(self.build("threshold.glb"),float("nan"))


if __name__=="__main__": unittest.main()
