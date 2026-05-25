import importlib
import asyncio
import os
import sys
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import AsyncMock, patch
from sqlalchemy import delete

from starlette.requests import Request
from starlette.responses import Response


class CustomAnimationBillingTests(unittest.IsolatedAsyncioTestCase):
    @classmethod
    def setUpClass(cls):
        cls._tmp = tempfile.TemporaryDirectory(prefix="autorig-custom-anim-test-")
        db_path = Path(cls._tmp.name) / "test.db"
        os.environ["DATABASE_URL"] = f"sqlite+aiosqlite:///{db_path}"
        os.environ["APP_URL"] = "https://autorig.test"

        backend_dir = str(Path(__file__).resolve().parents[1])
        if backend_dir not in sys.path:
            sys.path.insert(0, backend_dir)

        # Ensure env vars are applied on fresh imports.
        for mod in ("config", "database", "models", "tasks", "auth", "main"):
            sys.modules.pop(mod, None)

        cls.database = importlib.import_module("database")
        cls.models = importlib.import_module("models")
        cls.main = importlib.import_module("main")

    @classmethod
    def tearDownClass(cls):
        try:
            asyncio.run(cls.database.engine.dispose())
        except Exception:
            pass
        cls._tmp.cleanup()

    async def asyncSetUp(self):
        await self.database.init_db()
        self.task_id = "11111111-2222-3333-4444-555555555555"

        async with self.database.AsyncSessionLocal() as db:
            owner = self.database.User(email="owner@example.com", name="Owner", balance_credits=0)
            buyer = self.database.User(email="buyer@example.com", name="Buyer", balance_credits=75)
            task = self.database.Task(
                id=self.task_id,
                owner_type="user",
                owner_id="owner@example.com",
                created_at=datetime.now(timezone.utc).replace(tzinfo=None),
                status="done",
            )
            guid = self.task_id
            walk_url = f"http://example.com/{guid}_Walking.fbx"
            all_url = f"http://example.com/{guid}_all_animations_unity.fbx"
            task.output_urls = [walk_url, all_url]
            task.ready_urls = [walk_url]

            db.add_all([owner, buyer, task])
            await db.commit()

    async def asyncTearDown(self):
        async with self.database.AsyncSessionLocal() as db:
            await db.execute(delete(self.database.TaskLike))
            await db.execute(delete(self.database.TaskFilePurchase))
            await db.execute(delete(self.database.TaskAnimationPurchase))
            await db.execute(delete(self.database.TaskAnimationBundlePurchase))
            await db.execute(delete(self.database.Task))
            await db.execute(delete(self.database.User))
            await db.commit()

    async def test_custom_animation_and_bundle_pricing(self):
        async with self.database.AsyncSessionLocal() as db:
            buyer = (
                await db.execute(
                    self.main.select(self.database.User).where(self.database.User.email == "buyer@example.com")
                )
            ).scalar_one()
            owner = (
                await db.execute(
                    self.main.select(self.database.User).where(self.database.User.email == "owner@example.com")
                )
            ).scalar_one()

            # Catalog pricing rules.
            catalog = await self.main.api_get_animation_catalog(self.task_id, user=buyer, db=db)
            self.assertEqual(catalog.pricing["single_animation_credits"], 1)
            self.assertEqual(catalog.pricing["all_animations_credits"], 10)

            walking = next((a for a in catalog.animations if a.id == "walking"), None)
            self.assertIsNotNone(walking)
            self.assertTrue(walking.available)

            # Single animation purchase: -1 credit.
            c0 = buyer.balance_credits
            r1 = await self.main.api_purchase_animation(
                self.task_id,
                self.models.AnimationPurchaseRequest(animation_id="walking"),
                user=buyer,
                db=db,
            )
            self.assertTrue(r1.success)
            self.assertEqual(buyer.balance_credits, c0 - 1)

            # Duplicate purchase is idempotent.
            c1 = buyer.balance_credits
            r2 = await self.main.api_purchase_animation(
                self.task_id,
                self.models.AnimationPurchaseRequest(animation_id="walking"),
                user=buyer,
                db=db,
            )
            self.assertTrue(r2.success)
            self.assertEqual(buyer.balance_credits, c1)

            # Unlock all custom animations: -10 credits.
            c2 = buyer.balance_credits
            r3 = await self.main.api_purchase_animation(
                self.task_id,
                self.models.AnimationPurchaseRequest(all=True),
                user=buyer,
                db=db,
            )
            self.assertTrue(r3.success)
            self.assertEqual(buyer.balance_credits, c2 - 10)

            # Full ZIP download costs one $3 entry pack.
            c3 = buyer.balance_credits
            r4 = await self.main.api_purchase_files(
                self.task_id,
                self.models.PurchaseRequest(all=True),
                request=self._fake_request(),
                response=Response(),
                user=buyer,
                db=db,
            )
            self.assertTrue(r4.success)
            self.assertEqual(buyer.balance_credits, c3 - 30)

            await db.refresh(owner)
            self.assertEqual(owner.balance_credits, 41)  # 1 + 10 + 30

    async def test_gumroad_mapping_animal_cost_and_bonus_disabled(self):
        self.assertEqual(self.main.GUMROAD_PRODUCT_CREDITS.get("oneclick-30-credits"), 30)
        self.assertEqual(self.main.GUMROAD_PRODUCT_CREDITS.get("autorig-100"), 100)
        self.assertTrue(self.main._is_autorig_credit_product("oneclick-30-credits"))
        self.assertTrue(self.main._is_autorig_credit_product("autorig-100"))
        self.assertFalse(self.main._is_autorig_credit_product("free3d-10credits"))
        self.assertFalse(self.main._is_autorig_credit_product("blender-plugin"))
        self.assertEqual(
            self.main._gumroad_credit_target_email(
                {
                    "email": "merchant-checkout@example.com",
                    "url_params[userid]": "Buyer@Example.com",
                }
            ),
            "buyer@example.com",
        )
        self.assertEqual(
            self.main._gumroad_credit_target_email({"email": "checkout@example.com"}),
            "checkout@example.com",
        )

        animal_task_id = "22222222-3333-4444-5555-666666666666"
        async with self.database.AsyncSessionLocal() as db:
            buyer = (
                await db.execute(
                    self.main.select(self.database.User).where(self.database.User.email == "buyer@example.com")
                )
            ).scalar_one()
            animal_task = self.database.Task(
                id=animal_task_id,
                owner_type="user",
                owner_id="owner@example.com",
                created_at=datetime.now(timezone.utc).replace(tzinfo=None),
                status="done",
                input_type="animal",
            )
            animal_task.output_urls = [f"http://example.com/{animal_task_id}_animal_rig.fbx"]
            animal_task.ready_urls = list(animal_task.output_urls)
            db.add(animal_task)
            await db.commit()

            state = await self.main.api_get_purchase_state(
                animal_task_id,
                request=self._fake_request(),
                response=Response(),
                user=buyer,
                db=db,
            )
            self.assertEqual(state.all_files_credits, 30)

            c0 = buyer.balance_credits
            purchased = await self.main.api_purchase_files(
                animal_task_id,
                self.models.PurchaseRequest(all=True),
                request=self._fake_request(),
                response=Response(),
                user=buyer,
                db=db,
            )
            self.assertTrue(purchased.success)
            self.assertEqual(buyer.balance_credits, c0 - 30)

            before_bonus = buyer.balance_credits
            bonus = await self.main.grant_youtube_bonus(user=buyer, db=db)
            self.assertFalse(bonus["ok"])
            self.assertTrue(bonus["disabled"])
            self.assertEqual(buyer.balance_credits, before_bonus)

    async def test_zz_catalog_synthesizes_when_only_all_animations_glb_and_worker_list_empty(self):
        """Regression: many tasks list *_all_animations.glb but not *_all_animations_unity.fbx."""
        task_id = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
        guid = "ffffffff-1111-2222-3333-444444444444"
        worker = "http://worker.example"
        bundle = f"{worker}/converter/glb/{guid}/{guid}_all_animations.glb"

        async with self.database.AsyncSessionLocal() as db:
            task = self.database.Task(
                id=task_id,
                owner_type="user",
                owner_id="owner@example.com",
                created_at=datetime.now(timezone.utc).replace(tzinfo=None),
                status="done",
                guid=guid,
                worker_api=f"{worker}/converter/glb/{guid}/",
                output_urls=[bundle],
                ready_urls=[bundle],
            )
            db.add(task)
            await db.commit()

        with patch.object(self.main, "_fetch_worker_animation_file_urls", new=AsyncMock(return_value=[])):
            async with self.database.AsyncSessionLocal() as db:
                catalog = await self.main.api_get_animation_catalog(task_id, user=None, db=db)
                walking = next((a for a in catalog.animations if a.id == "walking"), None)
                self.assertIsNotNone(walking)
                self.assertTrue(walking.available)
                self.assertTrue(walking.ready)

    async def test_zz_synthetic_fbx_path_uses_100k_when_task_urls_contain_100k(self):
        task_id = "bbbbbbbb-cccc-dddd-eeee-ffffffffffff"
        guid = "99999999-aaaa-bbbb-cccc-dddddddddddd"
        worker = "http://worker2.example"
        bundle = f"{worker}/converter/glb/{guid}/{guid}_all_animations.glb"
        hint = f"{worker}/converter/glb/{guid}/{guid}_100k/{guid}_all_animations_unity.fbx"

        async with self.database.AsyncSessionLocal() as db:
            task = self.database.Task(
                id=task_id,
                owner_type="user",
                owner_id="owner@example.com",
                created_at=datetime.now(timezone.utc).replace(tzinfo=None),
                status="done",
                guid=guid,
                worker_api=f"{worker}/converter/glb/{guid}/",
                output_urls=[bundle, hint],
                ready_urls=[bundle, hint],
            )
            db.add(task)
            await db.commit()

        with patch.object(self.main, "_fetch_worker_animation_file_urls", new=AsyncMock(return_value=[])):
            async with self.database.AsyncSessionLocal() as db:
                task_row = await self.main.get_task_by_id(db, task_id)
                manifest = self.main._load_animation_manifest()
                raw_items = manifest.get("animations") or []
                fm = await self.main._build_task_animation_file_map(task_row, raw_items)
                hit = fm.get("walking")
                self.assertIsNotNone(hit)
                self.assertIn("_100k", hit["url"])

    def _fake_request(self) -> Request:
        return Request(
            {
                "type": "http",
                "method": "POST",
                "path": f"/api/task/{self.task_id}/purchases",
                "headers": [],
                "query_string": b"",
                "client": ("127.0.0.1", 12345),
                "scheme": "http",
                "server": ("testserver", 80),
            }
        )


if __name__ == "__main__":
    unittest.main()
