import importlib
import os
import sys
import tempfile
import unittest
from datetime import datetime, UTC
from pathlib import Path

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
        cls._tmp.cleanup()

    async def asyncSetUp(self):
        await self.database.init_db()
        self.task_id = "11111111-2222-3333-4444-555555555555"

        async with self.database.AsyncSessionLocal() as db:
            owner = self.database.User(email="owner@example.com", name="Owner", balance_credits=0)
            buyer = self.database.User(email="buyer@example.com", name="Buyer", balance_credits=35)
            task = self.database.Task(
                id=self.task_id,
                owner_type="user",
                owner_id="owner@example.com",
                created_at=datetime.now(UTC).replace(tzinfo=None),
                status="done",
            )
            guid = self.task_id
            walk_url = f"http://example.com/{guid}_Walking.fbx"
            all_url = f"http://example.com/{guid}_all_animations_unity.fbx"
            task.output_urls = [walk_url, all_url]
            task.ready_urls = [walk_url]

            db.add_all([owner, buyer, task])
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

            # Legacy "download all files" endpoint now charges 10 credits.
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
            self.assertEqual(buyer.balance_credits, c3 - 10)

            await db.refresh(owner)
            self.assertEqual(owner.balance_credits, 21)  # 1 + 10 + 10

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
