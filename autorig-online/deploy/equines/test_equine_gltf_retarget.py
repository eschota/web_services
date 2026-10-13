import copy
import json
import pathlib
import struct
import tempfile
import unittest

import equine_gltf_retarget as GR


def glb(doc, binary=b"BASE-TARGET-BYTES"):
    doc=copy.deepcopy(doc); doc["buffers"]=[{"byteLength":len(binary)}]
    js=json.dumps(doc,separators=(",",":"),sort_keys=True).encode(); js+=b" "*(-len(js)%4)
    binary+=b"\0"*(-len(binary)%4); total=12+8+len(js)+8+len(binary)
    return b"glTF"+struct.pack("<II",2,total)+struct.pack("<II",len(js),GR.JSON_CHUNK)+js+struct.pack("<II",len(binary),GR.BIN_CHUNK)+binary


def target_doc():
    return {"asset":{"version":"2.0"},"nodes":[
        {"name":"root","translation":[0,1,0],"rotation":[0,0,0,1],"scale":[1,1,1],"children":[1]},
        {"name":"spine","translation":[0,1,0],"rotation":[0,0,0,1],"scale":[1,1,1],"mesh":0}],
        "meshes":[{"name":"customer-mesh","primitives":[{"attributes":{}}]}],
        "materials":[{"name":"customer-material"}],"textures":[{"source":0}],"images":[{"uri":"texture.png"}],
        "skins":[{"joints":[0,1]}],"bufferViews":[],"accessors":[]}


def rig_doc():
    return {"model_height_units":2.0,"forward_axis":"+z","bones":[
        {"name":"root","parent":None,"head":[0,0,0],"tail":[0,1,0]},
        {"name":"spine","parent":"root","head":[0,1,0],"tail":[0,2,0]}]}


def source_candidate():
    sk=[
      {"index":0,"name":"Armature","parent":None,"is_skin_joint":False,"is_animated":False,
       "translation":[0,0,0],"rotation_xyzw":[0,0,0,1],"scale":[1,1,1]},
      {"index":1,"name":"src_root","parent":0,"is_skin_joint":True,"is_animated":True,
       "translation":[0,1,0],"rotation_xyzw":[0,0,0,1],"scale":[1,1,1]},
      {"index":2,"name":"src_spine","parent":1,"is_skin_joint":True,"is_animated":True,
       "translation":[0,1,0],"rotation_xyzw":[0,0,0,1],"scale":[1,1,1]}]
    times=[0.0,1/30,2/30]
    return {"schema":GR.SOURCE_SCHEMA,"state":"candidate_pending_real_qa","action_id":"walk_forward",
            "taxonomy":"full34","source_glb_sha256":"a"*64,"derived_glb":{"sha256":"b"*64},
            "reference_skeleton":sk,"channels":[
              {"node":1,"node_name":"src_root","path":"translation","interpolation":"LINEAR","times":times,
               "values":[[0,1,0],[.5,1,0],[1,1,0]]},
              {"node":1,"node_name":"src_root","path":"rotation","interpolation":"LINEAR","times":times,
               "values":[[0,0,0,1]]*3},
              {"node":2,"node_name":"src_spine","path":"rotation","interpolation":"LINEAR","times":times,
               "values":[[0,0,0,1],[0,0,.382683,.92388],[0,0,0,1]]}]}


def calibration():
    return {"schema":"autorig.hoof-source-calibration.v1","height":2.0,"ground_baseline":0.0,
            "forward_axis":[0,0,1],"loop":True,"bones":{
              "1":{"target":"root","head":[0,0,0],"tail":[0,1,0]},
              "2":{"target":"spine","head":[0,1,0],"tail":[0,2,0]}}}


class GltfRetargetTests(unittest.TestCase):
    def test_actual_source_motion_adapter(self):
        entry,mapping=GR.adapt_source_motion(source_candidate(),calibration())
        self.assertEqual(entry["clip"],"walk_forward")
        self.assertEqual(set(mapping),{"src_root","src_spine"})
        self.assertEqual(entry["tracks"]["2.rotation"]["bone"],"src_spine")
        self.assertTrue(entry["source_motion"]["candidate_only"])

    def test_rotated_armature_and_multiple_static_ancestors_are_composed_locally(self):
        candidate=source_candidate(); nodes=candidate["reference_skeleton"]
        # +90 degrees around Z, then two translated non-retained ancestors.
        nodes[0]["translation"]=[10,0,0]
        nodes[0]["rotation_xyzw"]=[0,0,2**-0.5,2**-0.5]
        nodes.extend([
            {"index":3,"name":"StaticA","parent":0,"is_skin_joint":False,"is_animated":False,
             "translation":[0,2,0],"rotation_xyzw":[0,0,0,1],"scale":[1,1,1]},
            {"index":4,"name":"StaticB","parent":3,"is_skin_joint":False,"is_animated":False,
             "translation":[0,0,3],"rotation_xyzw":[0,0,0,1],"scale":[1,1,1]}])
        nodes[1]["parent"]=4
        entry,_mapping=GR.adapt_source_motion(candidate,calibration())
        root=next(b for b in entry["reference_skeleton"]["bones"] if b["name"]=="src_root")
        for actual,expected in zip(root["rest_translation"],[7,0,3]): self.assertAlmostEqual(actual,expected,places=6)
        track=entry["tracks"]["1.translation"]["values"]
        for actual,expected in zip(track[0],[7,0,3]): self.assertAlmostEqual(actual,expected,places=6)
        for actual,expected in zip(track[-1],[7,1,3]): self.assertAlmostEqual(actual,expected,places=6)
        # Values are local to the nearest retained parent (none here), and the
        # child remains local to retained src_root rather than being world-baked.
        spine=next(b for b in entry["reference_skeleton"]["bones"] if b["name"]=="src_spine")
        self.assertEqual(spine["parent"],"src_root")
        self.assertEqual(spine["rest_translation"],[0.0,1.0,0.0])

    def test_source_adapter_rejects_unmapped_scale_and_nonuniform(self):
        bad=source_candidate(); bad["channels"][0]["path"]="scale"
        with self.assertRaisesRegex(GR.GltfRetargetError,"scale/morph"):
            GR.adapt_source_motion(bad,calibration())
        bad=source_candidate(); bad["channels"][1]["times"]=[0,.02,.07]
        with self.assertRaisesRegex(GR.GltfRetargetError,"exact sample times"):
            GR.adapt_source_motion(bad,calibration())

    def test_full_source_aggregate_is_exact_and_single_walk_stays_candidate(self):
        ids=("walk_forward","trot_jog")
        rows=[]; calibrations={}
        for action in ids:
            row=source_candidate(); row["action_id"]=action; rows.append(row); calibrations[action]=calibration()
        adapted,mapping=GR.adapt_source_library(rows,calibrations,ids)
        self.assertTrue(all(row["source_motion"]["release_complete"] for row in adapted))
        self.assertEqual(set(mapping),{"src_root","src_spine"})
        with self.assertRaisesRegex(GR.GltfRetargetError,"exactly match"):
            GR.adapt_source_library(rows[:-1],calibrations,ids)

    def test_target_glb_materialization_preserves_customer_payload(self):
        target=glb(target_doc()); original_doc,original_bin=GR._read_glb(target)
        entry,mapping=GR.adapt_source_motion(source_candidate(),calibration())
        target_meta,_=GR.target_from_glb(target,rig_doc())
        clip=GR.ER.retarget_entry(entry,target_meta,mapping)
        derived=GR.append_clip_bytes(target,clip)
        out_doc,out_bin=GR._read_glb(derived)
        for key in ("nodes","meshes","materials","textures","images","skins"):
            self.assertEqual(out_doc[key],original_doc[key])
        self.assertEqual(out_bin[:len(original_bin)],original_bin)
        self.assertEqual(len(out_doc["animations"]),1)
        self.assertEqual(out_doc["animations"][0]["name"],"walk_forward")

    def test_existing_animation_matrix_and_hierarchy_fail_closed(self):
        doc=target_doc(); doc["animations"]=[{"name":"old","channels":[],"samplers":[]}]
        entry,mapping=GR.adapt_source_motion(source_candidate(),calibration())
        meta,_=GR.target_from_glb(glb(target_doc()),rig_doc()); clip=GR.ER.retarget_entry(entry,meta,mapping)
        with self.assertRaisesRegex(GR.GltfRetargetError,"already has animations"):
            GR.append_clip_bytes(glb(doc),clip)
        doc=target_doc(); doc["nodes"][1]["matrix"]=[1,0,0,0,0,1,0,0,0,0,1,0,0,0,0,1]
        with self.assertRaisesRegex(GR.GltfRetargetError,"matrix"):
            GR.target_from_glb(glb(doc),rig_doc())
        doc=target_doc(); doc["nodes"][0]["children"]=[]
        with self.assertRaisesRegex(GR.GltfRetargetError,"hierarchy"):
            GR.target_from_glb(glb(doc),rig_doc())

    def test_writer_creates_fresh_derived_path_and_receipt(self):
        with tempfile.TemporaryDirectory(dir=str(pathlib.Path(__file__).parent)) as tmp:
            root=pathlib.Path(tmp); source=root/"target.glb"; output=root/"derived.glb"
            source.write_bytes(glb(target_doc())); before=source.read_bytes()
            entry,mapping=GR.adapt_source_motion(source_candidate(),calibration())
            receipt=GR.write_retargeted_glb(source,output,entry,rig_doc(),mapping)
            self.assertTrue(output.is_file()); self.assertEqual(source.read_bytes(),before)
            self.assertTrue(pathlib.Path(receipt["receipt_path"]).is_file())
            self.assertEqual(receipt["target_before_sha256"],GR._sha(before))
            self.assertEqual(receipt["state"],"candidate_not_release")
            self.assertFalse(receipt["activation_allowed"])
            with self.assertRaisesRegex(GR.GltfRetargetError,"already exists"):
                GR.write_retargeted_glb(source,output,entry,rig_doc(),mapping)


if __name__=="__main__": unittest.main()
