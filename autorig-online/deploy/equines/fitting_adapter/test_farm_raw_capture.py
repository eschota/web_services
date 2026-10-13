import json, tempfile, unittest
from pathlib import Path
import numpy as np
import farm_raw_capture as raw

class RawMeasurements(unittest.TestCase):
    def test_reuse_is_source_bound_and_never_approved(self):
        root=Path(__file__).parent/'.work-tests'; root.mkdir(exist_ok=True)
        with tempfile.TemporaryDirectory(dir=root) as td:
            binding={'video_sha256':'a'*64,'frame_count':3}; arrays={'positions':np.ones((3,2),np.float32)}
            raw.store(td,'tracks',binding,arrays,{'backend':'measured'})
            got,prov=raw.load(td,'tracks',binding)
            np.testing.assert_array_equal(got['positions'],arrays['positions'])
            self.assertEqual(prov['backend'],'measured')
            with self.assertRaisesRegex(ValueError,'identity changed'): raw.load(td,'tracks',{'video_sha256':'b'*64,'frame_count':3})
            report=json.loads((Path(td)/'tracks.json').read_text()); self.assertFalse(report['calibration_accepted'])
            report['animation_quality_approved']=True; (Path(td)/'tracks.json').write_text(json.dumps(report))
            with self.assertRaisesRegex(ValueError,'integrity/approval'): raw.load(td,'tracks',binding)

    def test_corruption_or_partial_measurement_blocks_reuse(self):
        root=Path(__file__).parent/'.work-tests'; root.mkdir(exist_ok=True)
        with tempfile.TemporaryDirectory(dir=root) as td:
            binding={'video_sha256':'a'*64}; raw.store(td,'depth',binding,{'values':np.zeros((2,2),np.float32)},{})
            data=Path(td)/'depth.npz'; data.write_bytes(data.read_bytes()+b'corrupt')
            with self.assertRaisesRegex(ValueError,'integrity/approval'): raw.load(td,'depth',binding)
            data.unlink()
            with self.assertRaisesRegex(ValueError,'partial raw capture'): raw.load(td,'depth',binding)

if __name__=='__main__': unittest.main()
