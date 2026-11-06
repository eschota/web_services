import asyncio,os,tempfile,unittest
from pathlib import Path
from urllib.parse import quote
import sys
ROOT=Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:sys.path.insert(0,str(ROOT))
from source_preflight_local import probe_owned_source
from source_preflight_policy import SourceDisposition,SourcePreflightResult,normalize_source_preflight,redirect_allowlist,resolve_source_redirect,retry_delay_seconds

class OwnedSourcePreflightTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp=tempfile.TemporaryDirectory(dir=ROOT);self.root=Path(self.temp.name);self.upload=self.root/"uploads";self.data=self.root/"renderfin";self.upload.mkdir();(self.data/"render").mkdir(parents=True);self.app="https://autorig.online"
    async def asyncTearDown(self):self.temp.cleanup()
    async def probe(self,url):return await probe_owned_source(url,app_url=self.app,upload_dir=self.upload,renderfin_data_dir=self.data)
    async def test_valid_upload_glb_and_renderfin_fbx(self):
        p=self.upload/"token"/"model.glb";p.parent.mkdir();p.write_bytes(b"glTF"+b"x"*100)
        q=self.data/"render"/"job"/"model.fbx";q.parent.mkdir();q.write_bytes(b"Kaydara FBX Binary"+b"x"*100)
        first=await self.probe("https://autorig.online/u/token/model.glb?download=1");second=await self.probe("https://autorig.online/renderfin/render/job/model.fbx")
        self.assertEqual((first.state,first.size,first.prefix[:4]),("ok",104,b"glTF"));self.assertEqual((second.state,second.prefix[:18]),("ok",b"Kaydara FBX Binary"))
    async def test_unicode_and_space_filename_decodes_once(self):
        path=self.upload/"token"/"модель 1.glb";path.parent.mkdir();path.write_bytes(b"glTF"+b"x"*8)
        encoded=quote("модель 1.glb")
        result=await self.probe(f"https://autorig.online/u/token/{encoded}")
        self.assertEqual((result.state,result.size),("ok",12))
    async def test_missing_owned_file_is_bounded_404_signal(self):
        result=await self.probe("https://autorig.online/u/token/missing.glb");self.assertEqual(result.state,"missing");self.assertIn("404",result.detail)
    async def test_wrong_origin_and_unsupported_owned_path_do_not_use_filesystem(self):
        self.assertIsNone(await self.probe("https://evil.test/u/token/model.glb"));self.assertIsNone(await self.probe("https://autorig.online/gallery/model.glb"))
    async def test_credentials_traversal_encoded_separator_backslash_and_nul_rejected(self):
        urls=("https://user:pass@autorig.online/u/a.glb","https://autorig.online/u/../a.glb","https://autorig.online/u/%2e%2e/a.glb","https://autorig.online/u/a%2fb.glb","https://autorig.online/u/a%5cb.glb","https://autorig.online/u/a%00.glb","https://autorig.online/u/a\\b.glb")
        for url in urls:
            with self.subTest(url=url):self.assertEqual((await self.probe(url)).state,"unsafe")
    async def test_symlink_escape_is_rejected(self):
        outside=self.root/"outside.glb";outside.write_bytes(b"glTF")
        link=self.upload/"link.glb"
        try:os.symlink(outside,link)
        except OSError as exc:self.skipTest(f"symlink unavailable: {exc}")
        result=await self.probe("https://autorig.online/u/link.glb");self.assertEqual(result.state,"unsafe")
    async def test_missing_configured_root_is_internal_not_bad_asset(self):
        result=await probe_owned_source("https://autorig.online/u/a.glb",app_url=self.app,upload_dir=self.root/"no-root",renderfin_data_dir=self.data)
        self.assertEqual(result.state,"internal")

class SourcePreflightPolicyUnitTests(unittest.TestCase):
    def test_typed_result_keeps_legacy_triplet_unpack(self):
        result=SourcePreflightResult(False,"down",SourceDisposition.TRANSIENT)
        self.assertEqual(tuple(result),(False,"down",False))
        legacy=normalize_source_preflight((False,"missing",False));self.assertEqual(legacy.disposition,SourceDisposition.BOUNDED)
    def test_persisted_backoff_schedule_caps_at_1800(self):
        self.assertEqual([retry_delay_seconds(i) for i in (1,2,3,4,100)],[60,300,900,1800,1800])
    def test_redirect_resolution_and_allowlist_are_logical_origin_only(self):
        allowed=redirect_allowlist("https://source.test/a.glb?old=1","https://autorig.online",["https://worker.test"],"[\"https://cdn.test\"]")
        self.assertEqual(resolve_source_redirect("https://source.test/a.glb?old=1","?new=2",allowed,set()),"https://source.test/a.glb?new=2")
        self.assertEqual(resolve_source_redirect("https://source.test/a.glb","https://cdn.test/b.glb",allowed,set()),"https://cdn.test/b.glb")
        for bad in ("http://127.0.0.1/x","http://169.254.169.254/x","https://evil.test/x","file:///x","https://u:p@source.test/x"):
            with self.assertRaises(ValueError):resolve_source_redirect("https://source.test/a.glb",bad,allowed,set())
    def test_redirect_loop_and_explicit_origin_json_validation(self):
        allowed=redirect_allowlist("https://source.test/a","https://autorig.online",[],"")
        with self.assertRaisesRegex(ValueError,"loop"):resolve_source_redirect("https://source.test/a","/b",allowed,{"https://source.test/b"})
        for raw in ("{}","[1]","[\"https://ok.test/path\"]","not-json"):
            with self.assertRaises(ValueError):redirect_allowlist("https://source.test/a","https://autorig.online",[],raw)

if __name__=="__main__":unittest.main()
