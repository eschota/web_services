import unittest

import equine_gltf_retarget as GR
import prepare_actual_walk_retarget as driver
from test_equine_gltf_retarget import calibration, glb, rig_doc, source_candidate, target_doc


class ActualWalkDriverTests(unittest.TestCase):
    def test_bundle_bound_calibration_and_numeric_sampler_validation(self):
        source=source_candidate(); source["source_glb_sha256"]="a"*64
        source["reference_skeleton"][1]["name"]="root"
        source["reference_skeleton"][2]["name"]="spine"
        rig=rig_doc(); bundle={"schema":"autorig-actionless-fitting-bundle.v1","source":{"sha256":"a"*64}}
        derived_calibration=driver.derive_calibration(source,rig,bundle)
        self.assertEqual(derived_calibration["source"],"exact_target_rig_json_same_source_glb")
        self.assertFalse("camera_z" in derived_calibration)
        self.assertIs(derived_calibration["loop"],False)
        entry,mapping=GR.adapt_source_motion(source,derived_calibration)
        target=glb(target_doc()); target_meta,_=GR.target_from_glb(target,rig)
        clip=GR.ER.retarget_entry(entry,target_meta,mapping)
        derived=GR.append_clip_bytes(target,clip,existing_animation_policy="replace")
        # inspect_actual_output consumes a file; its accessor reader is tested
        # separately here against the exact produced JSON/BIN.
        doc,binary=GR._read_glb(derived); animation=doc["animations"][0]
        channels=animation["channels"]; self.assertGreater(len(channels),1)
        for channel in channels:
            sampler=animation["samplers"][channel["sampler"]]
            times=driver._accessor(doc,binary,sampler["input"],1)
            width=4 if channel["target"]["path"]=="rotation" else 3
            values=driver._accessor(doc,binary,sampler["output"],width)
            self.assertEqual(len(times),len(values))

    def test_wrong_source_identity_blocks(self):
        source=source_candidate(); source["reference_skeleton"][1]["name"]="root"; source["reference_skeleton"][2]["name"]="spine"
        with self.assertRaisesRegex(ValueError,"identity mismatch"):
            driver.derive_calibration(source,rig_doc(),{"source":{"sha256":"f"*64}})

    def test_missing_loop_rejected_and_nonloop_propagates(self):
        source=source_candidate(); source["source_glb_sha256"]="a"*64
        source["reference_skeleton"][1]["name"]="root"; source["reference_skeleton"][2]["name"]="spine"
        source.pop("loop")
        with self.assertRaisesRegex(ValueError,"explicit boolean"):
            driver.derive_calibration(source,rig_doc(),{"source":{"sha256":"a"*64}})
        source["loop"]=False
        result=driver.derive_calibration(source,rig_doc(),{"source":{"sha256":"a"*64}})
        self.assertIs(result["loop"],False)


if __name__=="__main__": unittest.main()
