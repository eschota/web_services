"""Windows redirector regression; production contract checks, no GPU inference."""
import pathlib,sys,unittest
from unittest.mock import Mock,patch
sys.path.insert(0,str(pathlib.Path(__file__).resolve().parent))
import farm_observe_reference as observe

class Watchdog(unittest.TestCase):
    def test_real_interpreter_child_of_venv_launcher_is_accepted(self):
        marker={'pid':8208,'parent_pid':7440,'actor_token':'fresh','state':'gpu_admitted'}
        self.assertEqual(observe.verify_actor(marker,7440,'fresh'),8208)
    def test_stale_token_and_unrelated_actor_rejected(self):
        for pid,parent,token in [(8208,500,'fresh'),(8208,7440,'stale'),(True,7440,'fresh')]:
            with self.assertRaises(ValueError):
                observe.verify_actor({'pid':pid,'parent_pid':parent,'actor_token':token,'state':'gpu_admitted'},7440,'fresh')
    def test_direct_interpreter_is_supported(self):
        self.assertEqual(observe.verify_actor({'pid':7440,'actor_token':'fresh','state':'gpu_admitted'},7440,'fresh'),7440)
    def test_windows_stop_targets_own_tree(self):
        process=Mock(pid=7440); process.poll.return_value=None
        with patch.object(observe.os,'name','nt'),patch.object(observe.subprocess,'run') as run:
            observe.stop_own_tree(process)
            self.assertEqual(run.call_args.args[0],['taskkill','/PID','7440','/T','/F'])
        process.wait.assert_called_once_with(timeout=30)
    def test_completed_process_is_never_killed(self):
        process=Mock();process.poll.return_value=0
        with patch.object(observe.subprocess,'run') as run:
            observe.stop_own_tree(process);run.assert_not_called()

if __name__=='__main__': unittest.main()
