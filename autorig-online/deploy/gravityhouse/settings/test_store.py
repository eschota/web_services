import importlib.util,json,unittest,tempfile
from pathlib import Path
base=Path(__file__).resolve().parent
spec=importlib.util.spec_from_file_location('settings_server',base/'server.py');module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
class StoreTests(unittest.TestCase):
    def setUp(self):
        tmp=base/'.test-tmp';tmp.mkdir(exist_ok=True);self.temp=tempfile.TemporaryDirectory(dir=tmp)
        self.schema=json.loads((base/'schema.json').read_text(encoding='utf-8'));self.store=module.Store(self.temp.name,self.schema)
    def tearDown(self):self.temp.cleanup()
    def test_save_survives_restart_and_reset(self):
        values=dict(self.store.defaults);values['maxDegreesPerSecond']=125
        saved=self.store.save(values,1);self.assertEqual(saved['revision'],2)
        restarted=module.Store(self.temp.name,self.schema);self.assertEqual(restarted.read()['settings']['maxDegreesPerSecond'],125)
        reset=restarted.save(restarted.defaults,2);self.assertEqual(reset['settings'],self.store.defaults)
        self.assertEqual(json.loads((Path(self.temp.name)/'previous.json').read_text())['settings']['maxDegreesPerSecond'],125)
    def test_stale_revision_does_not_overwrite(self):
        self.store.save(self.store.defaults,1)
        with self.assertRaises(module.Conflict):self.store.save(self.store.defaults,1)
        self.assertEqual(self.store.read()['revision'],2)
    def test_scene_workshop_roundtrip_preserves_controls(self):
        schema=json.loads((base/'graphics-schema.json').read_text(encoding='utf-8'))
        graphics=module.Store(self.temp.name,schema,'graphics-settings.json')
        values=dict(graphics.defaults)
        values['Environment.exposure']=-.5
        values['Environment.zenith.r']=.23
        values['Wind.speed']=7.5
        values['Ball.mass']=2.3
        values['Cloth.damping']=3.2
        ior=next(k for k in values if k.startswith('Material.') and k.endswith('.ior'))
        values[ior]=1.52
        controls=self.store.read()
        graphics.save(values,graphics.read()['revision'])
        restarted=module.Store(self.temp.name,schema,'graphics-settings.json')
        self.assertEqual(restarted.read()['settings'],values)
        self.assertEqual(self.store.read(),controls)
    def test_workshop_migration_preserves_every_existing_field(self):
        schema=json.loads((base/'graphics-schema.json').read_text(encoding='utf-8'))
        old_schema=dict(schema,fields=[f for f in schema['fields'] if not f['key'].startswith(('Environment.','Wind.','Material.'))])
        old=module.Store(self.temp.name,old_schema,'graphics-settings.json')
        old_values=dict(old.defaults);old_values['Bloom.intensity']=1.81;old_values['Sun.rotation.x']=72.5
        saved=old.save(old_values,old.read()['revision'])
        migrated=module.Store(self.temp.name,schema,'graphics-settings.json').read()
        for key,value in old_values.items():self.assertEqual(migrated['settings'][key],value,key)
        self.assertEqual(migrated['revision'],saved['revision']+1)
        self.assertIn('Wind.speed',migrated['settings'])
    def test_invalid_values_do_not_write(self):
        for value in [float('nan'),float('inf'),False,-1,9999,'80']:
            values=dict(self.store.defaults);values['maxDegreesPerSecond']=value
            with self.assertRaises(ValueError):self.store.save(values,1)
        self.assertEqual(self.store.read()['revision'],1)
        values=dict(self.store.defaults);values['path']='/tmp/no'
        with self.assertRaises(ValueError):self.store.save(values,1)
    def test_graphics_namespace_restart_and_reset_preserve_controls(self):
        schema=json.loads((base/'graphics-schema.json').read_text(encoding='utf-8'))
        control_before=self.store.path.read_bytes()
        graphics=module.Store(self.temp.name,schema,'graphics-settings.json')
        values=dict(graphics.defaults);values['postEnabled']=False;values['showFps']=False;values['Bloom.intensity']=1.25
        graphics.save(values,1)
        restarted=module.Store(self.temp.name,schema,'graphics-settings.json')
        self.assertEqual(restarted.read()['settings'],values)
        with self.assertRaises(module.Conflict):restarted.save(values,1)
        reset=restarted.save(restarted.defaults,2);self.assertEqual(reset['settings'],graphics.defaults)
        self.assertEqual(self.store.path.read_bytes(),control_before)
        self.assertTrue((Path(self.temp.name)/'graphics-previous.json').exists())
        bad=dict(values);bad['antialiasing']=1.5
        with self.assertRaises(ValueError):restarted.save(bad,3)
    def test_graphics_new_field_migration_preserves_existing_values(self):
        schema=json.loads((base/'graphics-schema.json').read_text(encoding='utf-8'))
        graphics=module.Store(self.temp.name,schema,'graphics-settings.json');values=dict(graphics.defaults);values['showFps']=False;graphics.save(values,1)
        schema['fields'].append({'key':'futureEffect','type':'boolean','default':False})
        migrated=module.Store(self.temp.name,schema,'graphics-settings.json').read()
        self.assertFalse(migrated['settings']['showFps']);self.assertFalse(migrated['settings']['futureEffect']);self.assertEqual(migrated['revision'],3)
class LevelStoreTests(unittest.TestCase):
    TOKEN='qa-publish-token-0123456789'
    def setUp(self):
        tmp=base/'.test-tmp';tmp.mkdir(exist_ok=True);self.temp=tempfile.TemporaryDirectory(dir=tmp);self.levels=module.LevelStore(self.temp.name,self.TOKEN)
    def tearDown(self):self.temp.cleanup()
    @staticmethod
    def document(level_id='LittleTilt_Level01',plants=1,**extra):
        doc={'format':'little-tilt.level','version':1,'levelId':level_id,'title':'Дом 1','ivy':{'format':'DynamicIvy.Scene','version':1,'plants':[{'id':'p%d'%i,'recipe':{'Skin':0,'MaxLen':3.5}} for i in range(plants)]},'layers':[]}
        doc.update(extra);return doc
    def publish(self,level_id,doc,revision,token=None):return self.levels.publish(level_id,doc,revision,self.TOKEN if token is None else token)
    def test_first_publish_creates_revision_one_and_index_lists_it(self):
        self.assertIsNone(self.levels.read('LittleTilt_Level01'))
        with self.assertRaises(module.NotFound):self.levels.require('LittleTilt_Level01')
        saved=self.publish('LittleTilt_Level01',self.document(plants=3),0)
        self.assertEqual(saved['revision'],1);self.assertEqual(saved['levelId'],'LittleTilt_Level01');self.assertEqual(len(saved['document']['ivy']['plants']),3)
        index=self.levels.index();self.assertEqual([l['levelId'] for l in index['levels']],['LittleTilt_Level01']);self.assertEqual(index['levels'][0]['plants'],3);self.assertEqual(index['levels'][0]['title'],'Дом 1');self.assertTrue(index['publishing'])
        self.assertEqual(module.LevelStore(self.temp.name,self.TOKEN).require('LittleTilt_Level01')['revision'],1)
    def test_publishing_requires_the_developer_token(self):
        for token in [None,'','wrong',self.TOKEN+'x',7]:
            with self.assertRaises(module.Forbidden):self.publish('LittleTilt_Level01',self.document(),0,token=token)
        self.assertIsNone(self.levels.read('LittleTilt_Level01'))
        unconfigured=module.LevelStore(self.temp.name,None);self.assertFalse(unconfigured.index()['publishing'])
        with self.assertRaises(module.Forbidden):unconfigured.publish('LittleTilt_Level01',self.document(),0,self.TOKEN)
        self.assertEqual(self.publish('LittleTilt_Level01',self.document(),0)['revision'],1)
    def test_stale_revision_is_rejected_and_history_keeps_replaced_records(self):
        self.publish('L1',self.document('L1',plants=1),0)
        with self.assertRaises(module.Conflict):self.publish('L1',self.document('L1',plants=2),0)
        with self.assertRaises(module.Conflict):self.publish('L1',self.document('L1',plants=2),5)
        second=self.publish('L1',self.document('L1',plants=2),1);self.assertEqual(second['revision'],2)
        history=self.levels.history_list('L1')['history'];self.assertEqual([h['revision'] for h in history],[1])
        self.assertEqual(len(self.levels.history_read('L1',1)['document']['ivy']['plants']),1)
        with self.assertRaises(module.NotFound):self.levels.history_read('L1',7)
        self.assertEqual(len(self.levels.require('L1')['document']['ivy']['plants']),2)
    def test_history_is_pruned_to_the_retention_limit(self):
        revision=0
        for i in range(module.LEVEL_HISTORY+5):self.publish('L1',self.document('L1',plants=i+1),revision);revision+=1
        history=self.levels.history_list('L1')['history']
        self.assertEqual(len(history),module.LEVEL_HISTORY);self.assertEqual(history[0]['revision'],revision-1);self.assertEqual(history[-1]['revision'],revision-module.LEVEL_HISTORY)
        self.assertEqual(self.levels.require('L1')['revision'],revision)
    def test_invalid_documents_and_ids_do_not_write(self):
        for level_id in ['../x','L 1','','a'*65,None,3]:
            with self.assertRaises(ValueError):self.publish(level_id,self.document(level_id if isinstance(level_id,str) else 'x'),0)
        bad=[self.document(format='other'),self.document(version=2),self.document(levelId='Other'),self.document(title='x'*121),self.document(ivy={'plants':'no'}),self.document(ivy={'plants':[{}]*1001}),self.document(layers=[{'typeId':''}]),self.document(layers=[{}]),self.document(layers='no'),'not a dict',None]
        for doc in bad:
            with self.assertRaises(ValueError):self.publish('LittleTilt_Level01',doc,0)
            with self.assertRaises(ValueError):self.levels.draft_save('LittleTilt_Level01','draft-0001',doc,0)
        huge=self.document();huge['blob']='x'*(module.LEVEL_BYTES+1)
        with self.assertRaises(ValueError):self.publish('LittleTilt_Level01',huge,0)
        self.assertIsNone(self.levels.read('LittleTilt_Level01'));self.assertEqual(self.levels.index()['levels'],[])
        self.assertFalse(list(Path(self.temp.name).rglob('*.json')))
    def test_player_drafts_are_public_separate_and_never_become_official(self):
        saved=self.levels.draft_save('LittleTilt_Level01','abcdefgh-1',self.document(plants=2),0,'Player One')
        self.assertEqual((saved['revision'],saved['draftId'],saved['author']),(1,'abcdefgh-1','Player One'))
        with self.assertRaises(module.Conflict):self.levels.draft_save('LittleTilt_Level01','abcdefgh-1',self.document(plants=3),0)
        again=self.levels.draft_save('LittleTilt_Level01','abcdefgh-1',self.document(plants=3),1);self.assertEqual(again['revision'],2);self.assertEqual(again['createdAt'],saved['createdAt'])
        self.levels.draft_save('LittleTilt_Level01','abcdefgh-2',self.document(plants=1),0)
        index=self.levels.draft_index('LittleTilt_Level01');self.assertEqual({d['draftId'] for d in index['drafts']},{'abcdefgh-1','abcdefgh-2'});self.assertEqual(index['drafts'][0]['draftId'],'abcdefgh-2')
        self.assertEqual(len(self.levels.draft_require('LittleTilt_Level01','abcdefgh-1')['document']['ivy']['plants']),3)
        self.assertIsNone(self.levels.read('LittleTilt_Level01'));self.assertEqual(self.levels.index()['levels'],[])
        for draft_id in ['short','a b','../x','',None,'x'*65]:
            with self.assertRaises(ValueError):self.levels.draft_save('LittleTilt_Level01',draft_id,self.document(),0)
        with self.assertRaises(ValueError):self.levels.draft_save('LittleTilt_Level01','abcdefgh-3',self.document(),0,'a'*61)
        with self.assertRaises(module.NotFound):self.levels.draft_require('LittleTilt_Level01','abcdefgh-9')
        self.assertEqual(self.levels.draft_index('Other')['drafts'],[])
    def test_oldest_drafts_are_evicted_beyond_the_per_level_cap(self):
        import os,time
        for i in range(module.DRAFTS_PER_LEVEL+3):
            saved=self.levels.draft_save('L1','draft-%08d'%i,self.document('L1'),0)
            os.utime(Path(self.temp.name)/'levels'/'drafts'/'L1'/('draft-%08d.json'%i),(i+1,i+1))
        drafts={d['draftId'] for d in self.levels.draft_index('L1')['drafts']}
        self.assertEqual(len(drafts),module.DRAFTS_PER_LEVEL);self.assertNotIn('draft-00000000',drafts);self.assertIn('draft-%08d'%(module.DRAFTS_PER_LEVEL+2),drafts)
    def test_level_writes_never_touch_control_settings(self):
        schema=json.loads((base/'schema.json').read_text(encoding='utf-8'));controls=module.Store(self.temp.name,schema);before=controls.path.read_bytes()
        self.publish('LittleTilt_Level01',self.document(),0);self.levels.draft_save('LittleTilt_Level01','abcdefgh-1',self.document(),0);self.assertEqual(controls.path.read_bytes(),before);self.assertEqual(controls.read()['revision'],1)
    def test_token_is_read_from_environment_or_data_file(self):
        import os
        os.environ.pop('LEVEL_WRITE_TOKEN',None);self.assertIsNone(module.level_token(self.temp.name))
        (Path(self.temp.name)/'level-write-token').write_text('  file-token \n',encoding='utf-8');self.assertEqual(module.level_token(self.temp.name),'file-token')
        os.environ['LEVEL_WRITE_TOKEN']='env-token'
        try:self.assertEqual(module.level_token(self.temp.name),'env-token')
        finally:os.environ.pop('LEVEL_WRITE_TOKEN',None)
    def test_route_pattern_accepts_only_bounded_paths(self):
        for path in ['/gravityhouse/api/levels','/gravityhouse/api/levels/LittleTilt_Level01','/gravityhouse/api/levels/LittleTilt_Level01/history','/gravityhouse/api/levels/LittleTilt_Level01/history/12','/gravityhouse/api/levels/LittleTilt_Level01/drafts','/gravityhouse/api/levels/LittleTilt_Level01/drafts/abcdefgh-1']:self.assertIsNotNone(module.LEVEL_ROUTE.fullmatch(path),path)
        for path in ['/gravityhouse/api/levels/','/gravityhouse/api/levels/../x','/gravityhouse/api/levels/a b','/gravityhouse/api/levels/'+'a'*65,'/gravityhouse/api/levels/x/history/','/gravityhouse/api/levelsX','/gravityhouse/api/levels/x/other','/gravityhouse/api/levels/x/drafts/a/b']:self.assertIsNone(module.LEVEL_ROUTE.fullmatch(path),path)
if __name__=='__main__':unittest.main()
