"""Production contract tests; local execution is not project QA."""
import hashlib,json,pathlib,shutil,sys,tempfile,unittest
from unittest import mock
sys.path.insert(0,str(pathlib.Path(__file__).resolve().parent))
import farm_constraint_fit_pipeline as pipe
class Contract(unittest.TestCase):
 def test_pipeline_schema(self): self.assertEqual(pipe.SCHEMA,'autorig.equine-constraint-fit-pipeline-job/1')
 def test_explicit_timing_receipt(self): self.assertIn('autorig.video-timing-evidence.v1',pathlib.Path(pipe.__file__).read_text(encoding='utf-8'))
 def test_timing_receipt_id_is_canonical_payload_digest(self):
  reply=mock.Mock(returncode=0,stdout=json.dumps({'streams':[{'avg_frame_rate':'24/1','nb_read_frames':'49','width':768,'height':448,'duration':'2'}]}),stderr='')
  with mock.patch.object(pipe.subprocess,'run',return_value=reply): receipt=pipe.timing(pathlib.Path('ffprobe'),pathlib.Path('video.mp4'),'1'*64)
  rid=receipt.pop('receipt_id'); canonical=json.dumps(receipt,sort_keys=True,separators=(',',':'),ensure_ascii=False).encode('utf-8')
  self.assertEqual(rid,hashlib.sha256(canonical).hexdigest())
 def _job(self):
  here=pathlib.Path(__file__).resolve().parent; root=pathlib.Path(tempfile.mkdtemp(dir=here)); self.addCleanup(lambda:shutil.rmtree(root))
  def f(name,data=b'x'):
   p=root/name; p.write_bytes(data); return {'path':str(p),'sha256':hashlib.sha256(data).hexdigest()}
  ff=root/'ffprobe.exe'; ff.write_bytes(b'x')
  return {'schema':pipe.SCHEMA,'run_id':'r1','action_id':'walk_forward','loop':False,'ffprobe':{'path':str(ff),'sha256':hashlib.sha256(b'x').hexdigest()},'equines_code':str(here.parent),'raw_observations_module':{'path':str(here.parent/'equine_raw_observations.py'),'sha256':pipe.sha(here.parent/'equine_raw_observations.py')},'limb_constraints_module':{'path':str(here.parent/'equine_limb_constraints.py'),'sha256':pipe.sha(here.parent/'equine_limb_constraints.py')},'contact_evidence_module':{'path':str(here.parent/'equine_contact_evidence.py'),'sha256':pipe.sha(here.parent/'equine_contact_evidence.py')},'source_eligibility_module':{'path':str(here/'equine_source_eligibility.py'),'sha256':pipe.sha(here/'equine_source_eligibility.py')},'constraint_fit_module':{'path':str(here/'equine_constraint_fit.py'),'sha256':pipe.sha(here/'equine_constraint_fit.py')},'animation_fitting_code':'unused','output_root':str(root/'fresh'),'source_scope_sha256':'0'*64,'raw_tracks_meta':f('tracks.json',b'{}'),'raw_tracks_npz':f('tracks.npz'),'source_video':f('video.mp4'),'anchor_map':f('anchors.json',b'{}'),'surface_anchors':f('surface_anchors.json',b'{}'),'fitting_bundle':f('fitting_bundle.json',b'{}'),'bundle_manifest':f('immutable_manifest.json',b'{}'),'skeleton':f('skeleton.json',b'{}')}
 def test_preflight_requires_nonloop_and_fresh_output(self):
  job=self._job(); pipe.preflight(job)
  job['loop']=True
  with self.assertRaisesRegex(ValueError,'non-loop'): pipe.preflight(job)
  job['loop']=False; pathlib.Path(job['output_root']).mkdir()
  with self.assertRaisesRegex(ValueError,'fresh'): pipe.preflight(job)
if __name__=='__main__': unittest.main()
