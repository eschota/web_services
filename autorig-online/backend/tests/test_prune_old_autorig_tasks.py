import importlib.util,json,sqlite3,sys,tempfile,unittest
from pathlib import Path

SCRIPT=Path(__file__).parents[1]/"scripts"/"prune_old_autorig_tasks.py"
spec=importlib.util.spec_from_file_location("pruner",SCRIPT);pruner=importlib.util.module_from_spec(spec);sys.modules[spec.name]=pruner;spec.loader.exec_module(pruner)
CUTOFF="2026-09-08T17:26:03+00:00"

class PruneOldTasksTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory(dir=Path(__file__).parents[1]);self.root=Path(self.tmp.name);self.db=self.root/"tasks.db";self.rf=self.root/"renderfin.db";self.audit=self.root/"audit";self.audit.mkdir();self.refs=self.root/"ai-graphs";self.refs.mkdir();(self.refs/"graph.json").write_text('{"source_task_ids":["shared"]}')
        self.roots={name:self.root/name for name in pruner.DEFAULT_ROOTS}
        for path in self.roots.values():path.mkdir(parents=True)
        c=sqlite3.connect(self.db);c.executescript("""
        PRAGMA foreign_keys=ON;
        CREATE TABLE tasks(id TEXT PRIMARY KEY,status TEXT,created_at TEXT,updated_at TEXT,worker_api TEXT,processing_started_at TEXT,guid TEXT,input_url TEXT,output_urls TEXT,viewer_settings TEXT);
        CREATE TABLE artifact_cache_jobs(task_id TEXT);CREATE TABLE task_animation_corrections(task_id TEXT);CREATE TABLE task_likes(task_id TEXT);CREATE TABLE task_completion_emails(task_id TEXT);
        CREATE TABLE model_sale_offers(id INTEGER PRIMARY KEY,task_id TEXT REFERENCES tasks(id) ON DELETE RESTRICT);
        CREATE TABLE task_file_purchases(id INTEGER PRIMARY KEY,task_id TEXT REFERENCES tasks(id) ON DELETE CASCADE,amount REAL);
        CREATE TABLE rig_completion_events(task_id TEXT,completed_at TEXT);CREATE TABLE task_purchase_audit(task_id TEXT,amount REAL,receipt TEXT);CREATE TABLE scenes(id TEXT,models TEXT);
        """)
        old="2026-08-01 00:00:00";new="2026-09-09 00:00:00";boundary="2026-09-08 17:26:03"
        values=[("old","done",old,"u1",None,None,"g-old","https://autorig.online/u/tok-old/model.glb","[]","{}"),("new","done",new,"u2",None,None,"g-shared","https://autorig.online/u/tok-shared/new.glb","[]","{}"),("boundary","done",boundary,"u3",None,None,"g-bound","","[]","{}"),("active","processing",old,"u4",None,None,"g-active","","[]","{}"),("scene","done",old,"u5",None,None,"g-scene","","[]","{}"),("offer","done",old,"u6",None,None,"g-offer","","[]","{}"),("financial","done",old,"u6b",None,None,"g-fin","","[]","{}"),("shared","done",old,"u7",None,None,"g-shared","https://autorig.online/u/tok-shared/archive.glb","[]","{}"),("rf","done",old,"u8",None,None,"g-rf","","[]","{}")]
        c.executemany("INSERT INTO tasks VALUES(?,?,?,?,?,?,?,?,?,?)",values);c.execute("INSERT INTO scenes VALUES('s','{\"task_id\":\"scene\"}')");c.execute("INSERT INTO model_sale_offers(task_id) VALUES('offer')");c.execute("INSERT INTO rig_completion_events VALUES('old','2026-08-02')")
        c.execute("INSERT INTO task_file_purchases(task_id,amount) VALUES('financial',9.99)");c.execute("INSERT INTO task_purchase_audit VALUES('old',4.25,'immutable')")
        for table in pruner.OPS:c.execute(f"INSERT INTO {table} VALUES('old')")
        c.commit();c.close()
        r=sqlite3.connect(self.rf);r.execute("CREATE TABLE chargen_jobs(id TEXT,payload TEXT,stage TEXT,created_at TEXT)");r.execute("INSERT INTO chargen_jobs VALUES('j','{\"source_task_id\":\"rf\",\"stage\":\"hunyuan\"}','hunyuan','2026-08-01')");r.commit();r.close()
        self._files()
    def tearDown(self):self.tmp.cleanup()
    def _files(self):
        d=self.roots["tasks"]/"old";d.mkdir();(d/"a.zip").write_bytes(b"a")
        (self.roots["glb_cache"]/"old_model.glb").write_bytes(b"b");(self.roots["videos"]/"old.mp4").write_bytes(b"c");(self.roots["preflight"]/"old.jpg").write_bytes(b"d")
        d=self.roots["artifact_cache"]/"old";d.mkdir();(d/"x").write_bytes(b"e");(self.roots["deliverables"]/"g-old_model.zip").write_bytes(b"f")
        d=self.roots["uploads"]/"tok-old";d.mkdir();(d/"model.glb").write_bytes(b"g")
        d=self.roots["uploads"]/"tok-shared";d.mkdir();(d/"keep.glb").write_bytes(b"h");(self.roots["deliverables"]/"g-shared_model.zip").write_bytes(b"i")
    def manifest(self):return pruner.build_manifest(self.db,self.rf,pruner.cutoff(CUTOFF),{k:str(v) for k,v in self.roots.items()},[self.refs])
    def test_selection_cutoff_references_and_shared_artifacts(self):
        m=self.manifest();self.assertEqual([t["id"] for t in m["tasks"]],["old"]);paths={Path(f["path"]).name for f in m["files"]}
        self.assertIn("a.zip",paths);self.assertIn("old_model.glb",paths);self.assertNotIn("keep.glb",paths);self.assertNotIn("g-shared_model.zip",paths)
        self.assertTrue(all(set(entry["owners"])=={"old"} for entry in m["files"]))
    def test_apply_preserves_financial_audit_and_deletes_only_operational(self):
        m=self.manifest();journal=pruner.apply_manifest(m,self.audit);self.assertTrue(journal.is_file());c=sqlite3.connect(self.db)
        self.assertIsNone(c.execute("SELECT 1 FROM tasks WHERE id='old'").fetchone());self.assertEqual(c.execute("SELECT count(*) FROM rig_completion_events WHERE task_id='old'").fetchone()[0],1)
        self.assertEqual(c.execute("SELECT count(*) FROM task_file_purchases WHERE task_id='financial'").fetchone()[0],1)
        self.assertEqual(c.execute("SELECT * FROM task_purchase_audit WHERE task_id='old'").fetchone(),('old',4.25,'immutable'))
        for table in pruner.OPS:self.assertEqual(c.execute(f"SELECT count(*) FROM {table} WHERE task_id='old'").fetchone()[0],0)
        self.assertEqual(c.execute("PRAGMA foreign_key_check").fetchall(),[]);c.close();self.assertFalse((self.roots["videos"]/"old.mp4").exists());pruner.apply_manifest(m,self.audit)
    def test_cas_change_rolls_back_without_delete(self):
        m=self.manifest();c=sqlite3.connect(self.db);c.execute("UPDATE tasks SET updated_at='changed' WHERE id='old'");c.commit();c.close()
        with self.assertRaisesRegex(RuntimeError,"selection differs|CAS|backup does not contain"):pruner.apply_manifest(m,self.audit)
        c=sqlite3.connect(self.db);self.assertIsNotNone(c.execute("SELECT 1 FROM tasks WHERE id='old'").fetchone());c.close()
    def test_symlink_candidate_and_directory_are_never_unlinked(self):
        directory=self.roots["glb_cache"]/"old_dir";directory.mkdir();outside=self.root/"outside";outside.write_bytes(b"x");link=self.roots["glb_cache"]/"old_link"
        try:link.symlink_to(outside)
        except OSError as exc:self.skipTest(str(exc))
        with self.assertRaisesRegex(ValueError,"symlink|regular"):self.manifest()
        self.assertTrue(directory.is_dir());self.assertTrue(outside.exists())
    def test_symlink_directory_inside_root_is_rejected(self):
        outside=self.root/"outside-dir";outside.mkdir();(outside/"payload").write_bytes(b"x");link=self.roots["tasks"]/"old"
        for child in link.iterdir():child.unlink()
        link.rmdir()
        try:link.symlink_to(outside,target_is_directory=True)
        except OSError as exc:self.skipTest(str(exc))
        with self.assertRaisesRegex(ValueError,"symlink"):self.manifest()
    def test_renderfin_terminal_and_disagreement_rules(self):
        c=sqlite3.connect(self.rf);c.execute("DELETE FROM chargen_jobs");c.execute("INSERT INTO chargen_jobs VALUES('j','{\"source_task_id\":\"rf\",\"stage\":\"submitted\"}','submitted','x')");c.commit();c.close()
        self.assertIn("rf",{task["id"] for task in self.manifest()["tasks"]})
        c=sqlite3.connect(self.rf);c.execute("UPDATE chargen_jobs SET stage='hunyuan'");c.commit();c.close()
        self.assertNotIn("rf",{task["id"] for task in self.manifest()["tasks"]})
    def test_retained_logical_url_guid_protects_old_task(self):
        c=sqlite3.connect(self.db);c.execute("UPDATE tasks SET output_urls=? WHERE id='new'",('https://autorig.online/models/g-old%2Fviewer',));c.commit();c.close()
        manifest=self.manifest();self.assertNotIn("old",{task["id"] for task in manifest["tasks"]});self.assertIn("retained_task_payload",manifest["protected"]["old"])
    def test_historical_done_worker_binding_is_not_active(self):
        c=sqlite3.connect(self.db);c.execute("UPDATE tasks SET worker_api='http://historical',processing_started_at='2026-08-01' WHERE id='old'");c.commit();c.close()
        manifest=self.manifest();self.assertIn("old",{task["id"] for task in manifest["tasks"]});pruner.apply_manifest(manifest,self.audit)
        c=sqlite3.connect(self.db);self.assertIsNone(c.execute("SELECT 1 FROM tasks WHERE id='old'").fetchone());c.close()
    def test_all_owned_hardlink_names_removed_but_retained_name_survives(self):
        import os
        first=self.roots["glb_cache"]/"old_model.glb"
        second=self.roots["glb_cache"]/"old_second.glb"
        retained=self.roots["glb_cache"]/"new_keep.glb"
        os.link(first,second);os.link(first,retained)
        manifest=self.manifest()
        self.assertIn(str(first),{f["path"] for f in manifest["files"]})
        self.assertIn(str(second),{f["path"] for f in manifest["files"]})
        pruner.apply_manifest(manifest,self.audit)
        self.assertFalse(first.exists());self.assertFalse(second.exists())
        self.assertEqual(retained.read_bytes(),b"b")
    def test_changed_shared_reference_rolls_back(self):
        manifest=self.manifest();c=sqlite3.connect(self.db);c.execute("UPDATE tasks SET output_urls='[\"https://autorig.online/task/old\"]' WHERE id='new'");c.commit();c.close()
        with self.assertRaisesRegex(RuntimeError,"selection differs"):pruner.apply_manifest(manifest,self.audit)
        c=sqlite3.connect(self.db);self.assertIsNotNone(c.execute("SELECT 1 FROM tasks WHERE id='old'").fetchone());c.close()
    def test_commit_crash_window_reconciles_absent_tasks(self):
        manifest=self.manifest();digest=pruner.sha_bytes(pruner.canonical(manifest));backup=self.audit/f"tasks-before-prune-{digest}.sqlite3";pruner.backup_db(self.db,backup)
        state={"db_committed":False,"backup":str(backup),"backup_sha256":pruner.file_sha256(backup),"task_count":1,"task_ids_sha256":pruner.sha_bytes(pruner.canonical(["old"]))};pruner.atomic_json(self.audit/f"prune-journal-{digest}.json",state)
        c=sqlite3.connect(self.db);c.execute("PRAGMA foreign_keys=ON")
        for table in pruner.OPS:c.execute(f"DELETE FROM {table} WHERE task_id='old'")
        c.execute("DELETE FROM tasks WHERE id='old'");c.commit();c.close()
        state=pruner.apply_manifest(manifest,self.audit);record=json.loads(state.read_text());self.assertTrue(record["reconciled_after_commit"])
        self.assertFalse((self.roots["videos"]/"old.mp4").exists())
    def test_workload_lease_release_and_unresolved_states(self):
        c=sqlite3.connect(self.db);c.execute("ALTER TABLE tasks ADD COLUMN workload_lease_id TEXT");c.execute("ALTER TABLE tasks ADD COLUMN workload_lease_state TEXT")
        c.execute("UPDATE tasks SET workload_lease_id='historical',workload_lease_state='released' WHERE id='old'")
        c.execute("INSERT INTO tasks(id,status,created_at,updated_at,guid,input_url,output_urls,viewer_settings,workload_lease_id,workload_lease_state) VALUES('leased','done','2026-08-01','u','g-leased','','[]','{}','live','submission_unknown')")
        c.commit();c.close();manifest=self.manifest();selected={task["id"] for task in manifest["tasks"]}
        self.assertIn("old",selected);self.assertNotIn("leased",selected);self.assertEqual(manifest["protected"]["leased"],["workload_lease:submission_unknown"])
    def test_retained_reference_protection_reaches_fixed_point(self):
        c=sqlite3.connect(self.db);c.execute("INSERT INTO tasks VALUES('chain-b','done','2026-08-01','u',NULL,NULL,'g-chain-b','','[\"chain-c\"]','{}')")
        c.execute("INSERT INTO tasks VALUES('chain-c','done','2026-08-01','u',NULL,NULL,'g-chain-c','','[]','{}')")
        c.execute("UPDATE tasks SET output_urls='[\"chain-b\"]' WHERE id='new'");c.commit();c.close();manifest=self.manifest()
        self.assertNotIn("chain-b",{task["id"] for task in manifest["tasks"]});self.assertNotIn("chain-c",{task["id"] for task in manifest["tasks"]})
        self.assertIn("retained_task_payload",manifest["protected"]["chain-b"]);self.assertIn("retained_task_payload",manifest["protected"]["chain-c"])
    def test_cli_dryrun_hash_apply_and_backup(self):
        roots=json.dumps({k:str(v) for k,v in self.roots.items()});args=["--db",str(self.db),"--renderfin-db",str(self.rf),"--cutoff",CUTOFF,"--audit-dir",str(self.audit),"--roots-json",roots,"--reference-roots-json",json.dumps([str(self.refs)])]
        self.assertEqual(pruner.main(args),0);manifest=next(self.audit.glob("prune-manifest-*.json"));digest=pruner.sha_bytes(manifest.read_bytes());self.assertEqual(pruner.main(args+["--apply","--manifest-sha256",digest]),0);self.assertTrue(list(self.audit.glob("tasks-before-prune-*.sqlite3")))
    def test_cutoff_requires_timezone_and_boundary_is_retained(self):
        with self.assertRaises(ValueError):pruner.cutoff("2026-09-08T17:26:03")
        self.assertIn("boundary",{r["id"] for r in self._all_tasks()})
    def _all_tasks(self):
        c=sqlite3.connect(self.db);c.row_factory=sqlite3.Row;out=[dict(r) for r in c.execute("SELECT * FROM tasks")];c.close();return out
if __name__=="__main__":unittest.main()
