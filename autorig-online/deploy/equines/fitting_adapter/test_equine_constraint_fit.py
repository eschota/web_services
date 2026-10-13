"""Contract-only tests; project acceptance still requires production farm execution."""
import hashlib,json,math,pathlib,shutil,sys,tempfile,unittest
from types import SimpleNamespace
sys.path.insert(0,str(pathlib.Path(__file__).resolve().parent))
import equine_constraint_fit as fit
class Contract(unittest.TestCase):
 def test_options_are_public_and_bounded(self):
  got=fit.options_get(); self.assertEqual(set(got),set(fit.OPTIONS)); self.assertEqual(got['max_nfev']['value'],250); self.assertEqual(got['max_root_translation_height_ratio']['value'],.5); self.assertEqual(got['max_fit_seconds']['value'],300.0); self.assertEqual(got['limb_torso_vertical_weight']['value'],1.0)
  with self.assertRaises(ValueError): fit.options_get({'max_rotation_delta_rad':2})
  with self.assertRaises(ValueError): fit.options_get({'max_root_translation_height_ratio':3})
 def test_axis_conversion_is_explicit(self): self.assertEqual(fit.gltf_to_blender([1,2,3]),[1,-3,2])
 def test_max_nfev_float_is_rejected(self):
  with self.assertRaisesRegex(ValueError,'integer'): fit.options_get({'max_nfev':250.0})
 def _job(self,constraint_legs=None,constraint_frames=49,constraint_fps=24.0,action_frames=49,action_fps=24.0):
  root=pathlib.Path(tempfile.mkdtemp(dir=pathlib.Path(__file__).resolve().parent)); self.addCleanup(lambda:shutil.rmtree(root))
  def w(name,value):
   p=root/name; p.write_text(json.dumps(value),encoding='utf-8'); return {'path':str(p),'sha256':hashlib.sha256(p.read_bytes()).hexdigest()}
  surface=w('surface_anchors.json',{'bones':[]}); write=lambda name,value:w(name,value)
  write('fitting_bundle.json',{'artifacts':{'surface_anchors':{'sha256':surface['sha256']}}})
  manifest=w('immutable_manifest.json',{'files':[{'filename':'surface_anchors.json','sha256':surface['sha256'],'bytes':pathlib.Path(surface['path']).stat().st_size}]}); source='1'*64; timing={'frame_count':constraint_frames,'fps':constraint_fps,'source_frame_indices':list(range(constraint_frames))}
  rows={leg:{'source_anchor_id':f'{leg}:1'} for leg in (constraint_legs or fit.LEGS)}
  constraints={'schema':fit.CONSTRAINTS,'action':'walk_forward','timing':timing,'constraints':rows,'provenance':{'immutable_manifest_sha256':manifest['sha256'],'source_model_sha256':source}}
  cp=w('constraints.json',constraints); profile=w('profile.json',{'scope':'run','run_id':'r1','accepted':False,'category_default':False,'source_sha256':source,'constraints_sha256':cp['sha256'],'fit_options':{}}); action=w('action.json',{'action_id':'walk_forward','taxonomy':'full34','frame_count':action_frames,'fps':action_fps,'loop':True})
  hooves={leg:{'source_anchor_id':f'{leg}:1','frames':[{'frame':i,'source_frame':i,'state':'PENDING','reason':'fixture'} for i in range(constraint_frames)]} for leg in fit.LEGS}; evidence=w('contact.json',{'schema':'autorig.equine-measured-contact-evidence.v1','state':'measured_source_contact_evidence','accepted_observations':False,'animation_qa_pass':False,'hooves':hooves,'timing':timing,'provenance':{'constraints_sha256':cp['sha256'],'opposite_leg_inference':False,'mirroring':False,'hidden_point_interpolation':False,'gait_phase_prior':False}})
  eligibility=w('eligibility.json',{'schema':'autorig.equine-source-chain-eligibility.v1','state':'source_bound_target_eligibility','constraints_sha256':cp['sha256'],'targets':{leg:{f'{leg}:1':{'frames':[{'frame':i,'source_frame':i,'observed':False,'eligible':False,'reasons':['MISSING_OBSERVATION']} for i in range(constraint_frames)]}} for leg in fit.LEGS},'pending':[]})
  return {'schema':fit.SCHEMA,'action_id':'walk_forward','bundle_manifest':manifest,'surface_anchors':surface,'constraints':cp,'contact_evidence':evidence,'source_eligibility':eligibility,'profile':profile,'action_contract':action,'output':str(root/'out.json')}
 def test_requires_exact_four_legs(self):
  with self.assertRaisesRegex(ValueError,'exactly LF RF LH RH'): fit.preflight(self._job({'LF','RF','LH'}))
 def test_constraint_timing_must_equal_action(self):
  with self.assertRaisesRegex(ValueError,'timing'): fit.preflight(self._job(constraint_frames=48,action_frames=49))
 def test_surface_anchor_pin_must_match_bundle_and_manifest(self):
  job=self._job(); pathlib.Path(job['surface_anchors']['path']).write_text('{}',encoding='utf-8'); job['surface_anchors']['sha256']=fit.sha(pathlib.Path(job['surface_anchors']['path']))
  with self.assertRaisesRegex(ValueError,'RigBundle artifact'): fit.preflight(job)
 def test_pending_contact_never_becomes_observed_stance(self):
  job=self._job(); _,c,_,_,_=fit.preflight(job); evidence=fit.rd(pathlib.Path(job['contact_evidence']['path']))
  self.assertTrue(all(frame['state']=='PENDING' for row in evidence['hooves'].values() for frame in row['frames']))
 def test_contact_frame_mapping_is_fail_closed(self):
  job=self._job(); path=pathlib.Path(job['contact_evidence']['path']); doc=fit.rd(path); doc['hooves']['LF']['frames'][3]['source_frame']=99; path.write_text(json.dumps(doc),encoding='utf-8'); job['contact_evidence']['sha256']=fit.sha(path)
  with self.assertRaisesRegex(ValueError,'frame contract'): fit.preflight(job)
 def test_block_sparsity_uses_one_or_adjacent_full_frame_blocks(self):
  matrix=fit.sparsity_from_dependencies([(0,),(0,1),(2,)],3,3).toarray()
  self.assertEqual(matrix.shape,(3,9)); self.assertEqual(matrix[0].tolist(),[1,1,1,0,0,0,0,0,0]); self.assertEqual(matrix[1].tolist(),[1,1,1,1,1,1,0,0,0]); self.assertEqual(matrix[2].tolist(),[0,0,0,0,0,0,1,1,1])
 def test_cross_leg_skin_influence_adds_actual_other_chain_ancestor_dof(self):
  bones={'LeftFrontFoot':SimpleNamespace(parent='LeftFrontLeg'),'LeftFrontLeg':SimpleNamespace(parent='Spine2'),'RightFrontFoot':SimpleNamespace(parent='RightFrontLeg'),'RightFrontLeg':SimpleNamespace(parent='Spine2'),'Spine2':SimpleNamespace(parent=None)}
  anchor=SimpleNamespace(influences=[SimpleNamespace(bone='LeftFrontFoot',weight=.7),SimpleNamespace(bone='RightFrontFoot',weight=.3)])
  got=fit.influence_variable_indices(anchor,bones,{'LeftFrontLeg':2,'RightFrontLeg':7,'Spine2':9},[10,11]); self.assertEqual(got,[2,7,9,10,11])
 def test_resume_rejects_order_source_and_objective_option_drift(self):
  order=['A.x','root_translation.forward']; pins={'constraints_sha256':'1'*64}; options={k:v['value'] for k,v in fit.options_get().items()}; doc={'schema':'autorig.equine-constraint-fit-progress/1','objective_schema':fit.OBJECTIVE_SCHEMA,'variable_order':order,'pins':pins,'options':options,'completed_through_frame':0,'solved_parameters':{'0':[0,0]}}
  end,_,meta=fit.validate_resume(doc,order,pins,{**options,'max_nfev':500}); self.assertEqual(end,0); self.assertTrue(meta['objective_options_identical'])
  with self.assertRaisesRegex(ValueError,'variable order'): fit.validate_resume(doc,list(reversed(order)),pins,options)
  with self.assertRaisesRegex(ValueError,'source pins'): fit.validate_resume(doc,order,{'constraints_sha256':'2'*64},options)
  with self.assertRaisesRegex(ValueError,'objective options'): fit.validate_resume(doc,order,pins,{**options,'target_scale':2.0})
 def test_solver_control_profile_can_change_solver_only_not_objective_or_base(self):
  job=self._job(); base=pathlib.Path(job['profile']['path']); root=base.parent
  def attach(payload):
   p=root/'solver.json'; p.write_text(json.dumps(payload),encoding='utf-8'); job['solver_control_profile']={'path':str(p),'sha256':fit.sha(p)}
  common={'schema':'autorig.equine-solver-control-profile/1','scope':'run','run_id':'r1','accepted':False,'category_default':False,'base_profile_sha256':fit.sha(base)}
  attach({**common,'solver_options':{'max_nfev':500,'solver_ftol':1e-6}}); *_,options=fit.preflight(job); self.assertEqual(options['max_nfev'],500)
  attach({**common,'solver_options':{'target_scale':2.0}})
  with self.assertRaisesRegex(ValueError,'solver-only'): fit.preflight(job)
  attach({**common,'base_profile_sha256':'0'*64,'solver_options':{'max_nfev':500}})
  with self.assertRaisesRegex(ValueError,'base profile'): fit.preflight(job)
 def test_sole_geometry_counts_distinct_source_ids_and_world_points(self):
  rows=[SimpleNamespace(vertex_id=7,rest_world=[0,0,0]),SimpleNamespace(vertex_id=7,rest_world=[0,0,0]),SimpleNamespace(vertex_id=8,rest_world=[0,0,0]),SimpleNamespace(vertex_id=9,rest_world=[1,0,0])]
  stats=fit.sole_geometry_stats(rows,2.0); self.assertEqual(stats['source_vertex_ids'],[7,8,9]); self.assertEqual(stats['distinct_source_vertex_count'],3); self.assertEqual(stats['distinct_world_point_count'],2)
 def test_joint_constraints_require_exact_source_bone_anchor_and_provenance(self):
  row={'joint_constraints':{'RightFrontLeg':{'source_bone':'RightFrontLeg','source_anchor_id':'RightFrontLeg:7','runtime_track_id':'raw-7','derivation':'derived_from_source_plane_constraint','generated':False,'mirrored':False,'constraints':[{'frame':3,'position':[1,2,3],'confidence':.8}]}}}
  got=fit.joint_constraint_rows(row,['RightFrontToe','RightFrontFoot','RightFrontLeg','RightFrontUpLeg'],{'RightFrontLeg:7':'RightFrontLeg'},10); self.assertEqual(got['RightFrontLeg']['anchor'],'RightFrontLeg:7')
  row['joint_constraints']['RightFrontLeg']['mirrored']=True
  with self.assertRaisesRegex(ValueError,'provenance'): fit.joint_constraint_rows(row,['RightFrontToe','RightFrontFoot','RightFrontLeg'],{'RightFrontLeg:7':'RightFrontLeg'},10)
 def test_farm_guard_requires_managed_identity_and_matching_optional_env(self):
  root=pathlib.Path(tempfile.mkdtemp(dir=pathlib.Path(__file__).resolve().parent)); self.addCleanup(lambda:shutil.rmtree(root)); (root/'box.txt').write_text('f15\n',encoding='utf-8')
  got=fit.farm_execute_guard(platform_name='nt',environ={'FARM_RUNNER_HOME':str(root)}); self.assertEqual(got['box'],'f15')
  with self.assertRaisesRegex(ValueError,'disagrees'): fit.farm_execute_guard(platform_name='nt',environ={'FARM_RUNNER_HOME':str(root),'FARM_BOX':'f13'})
  (root/'box.txt').write_text('worker-4090\n',encoding='utf-8')
  with self.assertRaisesRegex(ValueError,'not allowed'): fit.farm_execute_guard(platform_name='nt',environ={'FARM_RUNNER_HOME':str(root)})
  with self.assertRaisesRegex(ValueError,'Windows'): fit.farm_execute_guard(platform_name='posix',environ={'FARM_RUNNER_HOME':str(root)})
 def test_stable_pseudo_huber_matches_tiny_linear_and_compresses_large_residual(self):
  import numpy as np
  tiny=np.asarray([1e-14,-1e-14]); got=fit.pseudo_huber_residual(tiny,.05); np.testing.assert_allclose(got,tiny,rtol=1e-12,atol=0)
  large=fit.pseudo_huber_residual(np.asarray([10.0]),.05)[0]; self.assertGreater(large,0); self.assertLess(large,10.0); self.assertAlmostEqual(large,math.sqrt(2*.05*10),delta=.01)
 def test_source_relative_vertical_is_root_translation_invariant_and_requires_admission_torso(self):
  import numpy as np
  source=np.asarray([1.,2.,3.]); torso=np.asarray([0.,1.,0.]); actual=np.asarray([2.,4.,5.]); actual_torso=np.asarray([1.,2.,2.]); up=np.asarray([0.,1.,0.]); shift=np.asarray([9.,-4.,3.])
  first=fit.source_relative_vertical_residual(source,torso,actual,actual_torso,up,admitted=True); shifted=fit.source_relative_vertical_residual(source,torso,actual+shift,actual_torso+shift,up,admitted=True); self.assertAlmostEqual(first,shifted)
  self.assertIsNone(fit.source_relative_vertical_residual(source,None,actual,actual_torso,up,admitted=True)); self.assertIsNone(fit.source_relative_vertical_residual(source,torso,actual,actual_torso,up,admitted=False))
if __name__=='__main__': unittest.main()
