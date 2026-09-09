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
    def test_invalid_values_do_not_write(self):
        for value in [float('nan'),float('inf'),False,-1,9999,'80']:
            values=dict(self.store.defaults);values['maxDegreesPerSecond']=value
            with self.assertRaises(ValueError):self.store.save(values,1)
        self.assertEqual(self.store.read()['revision'],1)
        values=dict(self.store.defaults);values['path']='/tmp/no'
        with self.assertRaises(ValueError):self.store.save(values,1)
if __name__=='__main__':unittest.main()
