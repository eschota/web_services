from __future__ import annotations
import argparse, hashlib, json, pathlib, shutil, sys, tempfile, unittest

HERE=pathlib.Path(__file__).resolve().parent
sys.path.insert(0,str(HERE))
import farm_fit_video as FF
import joint_limit_candidate as JL

def write(path,value): path.parent.mkdir(parents=True,exist_ok=True); path.write_text(json.dumps(value),encoding='utf-8'); return path
def pin(path): return {'path':str(path),'sha256':hashlib.sha256(path.read_bytes()).hexdigest()}

class Contracts(unittest.TestCase):
    def setUp(self):
        root=HERE/'.work-tests'; root.mkdir(exist_ok=True); self.t=pathlib.Path(tempfile.mkdtemp(dir=root))
    def tearDown(self): shutil.rmtree(self.t)
    def test_profile_receipt_merges_exact_fitting_config(self):
        source=self.t/'source.glb'; source.write_bytes(b'glb')
        rig=write(self.t/'rig.json',{'bones':[{'name':'Hips'}]})
        profile=write(self.t/'profile.json',{'schema':JL.SCHEMA,'state':'candidate','scope':'run','run_id':'r1','approved':False,'category_default':False,'source_sha256':JL.sha(source),'rig_json_sha256':JL.sha(rig),'bones':{'Hips':{'x':[-1,1],'y':[-.5,.5],'z':[-.2,.2]}},'fitting_config_overrides':{'joint_rest_weight':.01,'max_nfev':700,'allow_unbounded_joints':False,'active_bones':['Hips']}})
        receipt=JL.build_receipt(argparse.Namespace(source=source,rig_json=rig,profile=profile,run_id='r1'))
        self.assertNotIn('regularization',receipt); self.assertEqual(receipt['fitting_config_overrides']['max_nfev'],700)
        base={'schema':'autorig-fitting-config.v1',**JL.FIT_DEFAULTS}; config=write(self.t/'config.json',base)
        out=FF.merged_fit_config({'fit_config':config},receipt,self.t)
        merged=json.loads(out.read_text()); self.assertEqual(merged['joint_rest_weight'],.01); self.assertEqual(merged['active_bones'],['Hips']); self.assertFalse(merged['allow_unbounded_joints'])
    def test_legacy_regularization_is_explicit_error(self):
        source=self.t/'source.glb'; source.write_bytes(b'glb'); rig=write(self.t/'rig.json',{'bones':[{'name':'Hips'}]})
        profile=write(self.t/'profile.json',{'schema':JL.SCHEMA,'state':'candidate','scope':'run','run_id':'r1','approved':False,'category_default':False,'source_sha256':JL.sha(source),'rig_json_sha256':JL.sha(rig),'bones':{'Hips':{'x':[-1,1],'y':[-1,1],'z':[-1,1]}},'regularization':{'joint_limit_weight':1}})
        with self.assertRaisesRegex(ValueError,'legacy regularization'): JL.build_receipt(argparse.Namespace(source=source,rig_json=rig,profile=profile,run_id='r1'))
    def test_existing_observations_rejects_nonaccepted_diagnostics(self):
        bundle=self.t/'bundle'; bundle.mkdir(); video=self.t/'video.mp4'; video.write_bytes(b'video')
        meta=write(bundle/'fitting_bundle.json',{'source':{'sha256':'0'*64}}); imm=write(bundle/'immutable_manifest.json',{'files':[]})
        obsroot=self.t/'obs'; obs=write(obsroot/'observations.json',{'provenance':{'source_video_sha256':FF.sha(video),'bundle_sha256':FF.sha(meta),'immutable_manifest_sha256':FF.sha(imm),'relative_depth_contract':'calibrated_to_camera_z_from_immutable_actionless_reference','camera_z_calibration':{}},'depth':[{'mode':'camera_z'}],'tracks':[]})
        diag=write(obsroot/'diagnostics.json',{'decision':'rejected','animation_quality_approved':False})
        manifest=write(obsroot/'observation_bundle_manifest.json',{'schema':'autorig-tracking-observation-bundle.v1','files':[{'path':'observations.json','bytes':obs.stat().st_size,'sha256':FF.sha(obs)},{'path':'diagnostics.json','bytes':diag.stat().st_size,'sha256':FF.sha(diag)}]})
        job={'existing_observations':pin(obs),'existing_observation_manifest':pin(manifest)}
        with self.assertRaisesRegex(ValueError,'not accepted'): FF.existing_observations(job,{'video':video},bundle,{'tracks':[]})

if __name__=='__main__': unittest.main()
