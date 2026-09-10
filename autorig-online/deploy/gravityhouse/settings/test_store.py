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
if __name__=='__main__':unittest.main()
