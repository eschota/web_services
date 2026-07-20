import importlib
import asyncio
import os
import sys
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional
from urllib.parse import urlencode
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
            await db.execute(delete(self.database.TaskAnimalAnimationPackPurchase))
            await db.execute(delete(self.database.PurchaseCheckoutIntent))
            await db.execute(delete(self.database.GumroadPurchase))
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

            # Catalog pricing rules: any new animation purchase unlocks the whole task.
            catalog = await self.main.api_get_animation_catalog(self.task_id, user=buyer, db=db)
            self.assertEqual(catalog.pricing["purchase_scope"], "task")
            self.assertEqual(catalog.pricing["task_unlock_credits"], 10)
            self.assertEqual(catalog.pricing["single_animation_credits"], 10)
            self.assertEqual(catalog.pricing["all_animations_credits"], 10)

            walking = next((a for a in catalog.animations if a.id == "walking"), None)
            self.assertIsNotNone(walking)
            self.assertTrue(walking.available)
            self.assertEqual(walking.credits, 10)
            self.assertEqual(
                walking.download_url,
                f"/api/task/{self.task_id}/animations/download/walking",
            )
            self.assertEqual(
                walking.download_with_base_url,
                f"/api/task/{self.task_id}/animations/download-with-base/walking",
            )

            # Legacy single-animation purchase creates the full task unlock: -10 credits.
            c0 = buyer.balance_credits
            r1 = await self.main.api_purchase_animation(
                self.task_id,
                self.models.AnimationPurchaseRequest(animation_id="walking"),
                user=buyer,
                db=db,
            )
            self.assertTrue(r1.success)
            self.assertTrue(r1.purchased_all)
            self.assertEqual(buyer.balance_credits, c0 - 10)

            full_unlocks = (
                await db.execute(
                    self.main.select(self.database.TaskFilePurchase).where(
                        self.database.TaskFilePurchase.task_id == self.task_id,
                        self.database.TaskFilePurchase.user_email == "buyer@example.com",
                        self.database.TaskFilePurchase.file_index.is_(None),
                    )
                )
            ).scalars().all()
            self.assertEqual(len(full_unlocks), 1)
            self.assertEqual(full_unlocks[0].credits_spent, 10)

            single_rows = (
                await db.execute(
                    self.main.select(self.database.TaskAnimationPurchase).where(
                        self.database.TaskAnimationPurchase.task_id == self.task_id,
                        self.database.TaskAnimationPurchase.user_email == "buyer@example.com",
                    )
                )
            ).scalars().all()
            self.assertEqual(single_rows, [])

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

            # Legacy animation-bundle purchase is also idempotent after task unlock.
            c2 = buyer.balance_credits
            r3 = await self.main.api_purchase_animation(
                self.task_id,
                self.models.AnimationPurchaseRequest(all=True),
                user=buyer,
                db=db,
            )
            self.assertTrue(r3.success)
            self.assertEqual(buyer.balance_credits, c2)

            # Task ZIP purchase is idempotent after the same task unlock.
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
            self.assertEqual(buyer.balance_credits, c3)

            await db.refresh(owner)
            self.assertEqual(owner.balance_credits, 10)

    async def test_anonymous_task_owner_state_and_google_claim(self):
        anon_task_id = "33333333-4444-5555-6666-777777777777"
        anon_id = "anonymous-task-owner"
        async with self.database.AsyncSessionLocal() as db:
            task = self.database.Task(
                id=anon_task_id,
                owner_type="anon",
                owner_id=anon_id,
                created_at=datetime.now(timezone.utc).replace(tzinfo=None),
                status="processing",
            )
            db.add(task)
            await db.commit()

            request = self._fake_request(
                method="GET",
                path=f"/api/task/{anon_task_id}/purchases",
                headers=[(b"cookie", f"{self.main.ANON_COOKIE}={anon_id}".encode("ascii"))],
            )
            state = await self.main.api_get_purchase_state(
                anon_task_id,
                request=request,
                response=Response(),
                user=None,
                db=db,
            )
            self.assertTrue(state.is_owner)
            self.assertTrue(state.login_required)

            claimed = await self.main._claim_anonymous_task_for_user(
                db,
                anon_task_id,
                anon_id,
                "Owner@Example.com",
            )
            self.assertTrue(claimed)
            await db.refresh(task)
            self.assertEqual(task.owner_type, "user")
            self.assertEqual(task.owner_id, "owner@example.com")

            claimed_again = await self.main._claim_anonymous_task_for_user(
                db,
                anon_task_id,
                anon_id,
                "owner@example.com",
            )
            self.assertFalse(claimed_again)

    async def test_freestock_worker_paths_survive_gateway_decode(self):
        class DummyResponse:
            status_code = 200

            @staticmethod
            def json():
                return {
                    "folders": {
                        "animations": {
                            "files": [
                                {"rel_path": "nested/Happy Walk.fbx"},
                                {"rel_path": "Танец.fbx"},
                            ]
                        }
                    }
                }

        class DummyClient:
            async def __aenter__(self):
                return self

            async def __aexit__(self, *args):
                return False

            async def get(self, *args, **kwargs):
                return DummyResponse()

        with patch.object(self.main.httpx, "AsyncClient", return_value=DummyClient()):
            urls = await self.main._fetch_worker_animation_file_urls(
                "https://converter-f1.freestock.online/converter/glb",
                "guid",
            )
            direct_urls = await self.main._fetch_worker_animation_file_urls(
                "https://worker.example/converter/glb",
                "guid",
            )

        self.assertIn("nested/Happy%2520Walk.fbx", urls[0])
        self.assertIn("%25D0%25A2", urls[1])
        self.assertIn("nested/Happy%20Walk.fbx", direct_urls[0])

    async def test_disabled_viewer_modules_return_410_before_data_access(self):
        task_response = self.main._task_html_response("<html><body>task</body></html>")
        self.assertEqual(task_response.headers["cache-control"], "no-store, max-age=0")
        self.assertEqual(task_response.headers["pragma"], "no-cache")
        with patch.object(self.main, "ANIMATION_FITTING_ENABLED", False):
            self.assertEqual(
                self.main._disabled_viewer_feature_for_path("/api/admin/animation-fitting/jobs"),
                "Animation fitting",
            )
        with patch.object(self.main, "BONE_CORRECTION_ENABLED", False):
            self.assertEqual(
                self.main._disabled_viewer_feature_for_path(
                    f"/api/task/{self.task_id}/animation-corrections"
                ),
                "Bone correction",
            )
            with self.assertRaises(self.main.HTTPException) as bone_error:
                await self.main.api_get_task_animation_corrections(
                    self.task_id,
                    request=None,
                    response=None,
                    user=None,
                    db=None,
                )
        self.assertEqual(bone_error.exception.status_code, 410)

        with self.assertRaises(self.main.HTTPException) as fitting_error:
            await self.main.api_disabled_idle_ltx(self.task_id, "status")
        self.assertEqual(fitting_error.exception.status_code, 410)

    async def test_legacy_file_index_purchase_creates_task_unlock(self):
        async with self.database.AsyncSessionLocal() as db:
            buyer = (
                await db.execute(
                    self.main.select(self.database.User).where(self.database.User.email == "buyer@example.com")
                )
            ).scalar_one()

            c0 = buyer.balance_credits
            purchased = await self.main.api_purchase_files(
                self.task_id,
                self.models.PurchaseRequest(file_indices=[0]),
                request=self._fake_request(),
                response=Response(),
                user=buyer,
                db=db,
            )

            self.assertTrue(purchased.success)
            self.assertTrue(purchased.purchased_all)
            self.assertEqual(buyer.balance_credits, c0 - 10)

            rows = (
                await db.execute(
                    self.main.select(self.database.TaskFilePurchase).where(
                        self.database.TaskFilePurchase.task_id == self.task_id,
                        self.database.TaskFilePurchase.user_email == "buyer@example.com",
                    )
                )
            ).scalars().all()
            self.assertEqual(len(rows), 1)
            self.assertIsNone(rows[0].file_index)
            self.assertEqual(rows[0].credits_spent, 10)

    async def test_existing_legacy_animation_entitlements_still_grant_access(self):
        async with self.database.AsyncSessionLocal() as db:
            db.add(
                self.database.TaskAnimationPurchase(
                    task_id=self.task_id,
                    user_email="buyer@example.com",
                    animation_id="walking",
                    credits_spent=1,
                )
            )
            db.add(
                self.database.TaskAnimalAnimationPackPurchase(
                    task_id=self.task_id,
                    user_email="buyer@example.com",
                    animal_type="dog",
                    orientation="front",
                    credits_spent=10,
                )
            )
            await db.commit()

            buyer = (
                await db.execute(
                    self.main.select(self.database.User).where(self.database.User.email == "buyer@example.com")
                )
            ).scalar_one()
            purchased_ids, purchased_all = await self.main._get_animation_purchase_state(db, buyer, self.task_id)
            self.assertIn("walking", purchased_ids)
            self.assertFalse(purchased_all)
            self.assertTrue(
                await self.main._has_animal_animation_pack_purchase(
                    db,
                    buyer,
                    self.task_id,
                    "dog",
                    "front",
                )
            )

    async def test_gumroad_mapping_animal_cost_and_bonus_disabled(self):
        self.assertEqual(self.main.GUMROAD_PRODUCT_CREDITS.get("oneclick-30-credits"), 30)
        self.assertEqual(self.main.GUMROAD_PRODUCT_CREDITS.get("autorig-100"), 100)
        self.assertTrue(self.main._is_autorig_credit_product("oneclick-30-credits"))
        self.assertTrue(self.main._is_autorig_credit_product("autorig-100"))
        self.assertFalse(self.main._is_autorig_credit_product("free3d-10credits"))
        self.assertFalse(self.main._is_autorig_credit_product("blender-plugin"))
        self.assertEqual(
            self.main._gumroad_product_key_from_payload("", "Autorig - 100 Credits"),
            "autorig-100",
        )
        self.assertEqual(
            self.main._gumroad_product_key_from_payload("unexpected-product", "Autorig - 1000 Credits"),
            "autorig-1000",
        )
        self.assertEqual(
            self.main._gumroad_product_key_from_payload("unexpected-product", "Autorig - 30 Credits"),
            "oneclick-30-credits",
        )
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
            self.assertEqual(state.all_files_credits, 10)

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
            self.assertEqual(buyer.balance_credits, c0 - 10)

            before_bonus = buyer.balance_credits
            bonus = await self.main.grant_youtube_bonus(user=buyer, db=db)
            self.assertFalse(bonus["ok"])
            self.assertTrue(bonus["disabled"])
            self.assertEqual(buyer.balance_credits, before_bonus)

    async def test_credit_checkout_route_records_intent_and_redirects(self):
        async with self.database.AsyncSessionLocal() as db:
            buyer = (
                await db.execute(
                    self.main.select(self.database.User).where(self.database.User.email == "buyer@example.com")
                )
            ).scalar_one()
            req = self._fake_request(
                method="GET",
                path="/buy-credits/checkout/autorig-100",
                query_string=f"source=task_paywall_modal&task_id={self.task_id}&required_credits=10".encode(),
                headers=[(b"referer", b"https://autorig.test/task?id=111")],
            )

            with patch("telegram_bot.broadcast_credits_purchase_click", new=AsyncMock()) as tg:
                resp = await self.main.buy_credits_checkout(
                    "autorig-100",
                    request=req,
                    source="task_paywall_modal",
                    task_id=self.task_id,
                    required_credits=10,
                    user=buyer,
                    db=db,
                )
                await asyncio.sleep(0)

            self.assertEqual(resp.status_code, 303)
            self.assertEqual(
                resp.headers["location"],
                "https://u3d.gumroad.com/l/autorig-100?userid=buyer%40example.com",
            )
            tg.assert_called_once()

            rows = (
                await db.execute(
                    self.main.select(self.database.PurchaseCheckoutIntent).where(
                        self.database.PurchaseCheckoutIntent.user_email == "buyer@example.com"
                    )
                )
            ).scalars().all()
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0].product_permalink, "autorig-100")
            self.assertEqual(rows[0].source, "task_paywall_modal")
            self.assertEqual(rows[0].task_id, self.task_id)
            self.assertEqual(rows[0].required_credits, 10)

    async def test_credit_checkout_route_rejects_unknown_and_requires_login(self):
        async with self.database.AsyncSessionLocal() as db:
            req = self._fake_request(method="GET", path="/buy-credits/checkout/autorig-100")
            resp = await self.main.buy_credits_checkout(
                "autorig-100",
                request=req,
                user=None,
                db=db,
            )
            self.assertEqual(resp.status_code, 303)
            self.assertTrue(resp.headers["location"].startswith("/auth/login?next="))

            with self.assertRaises(self.main.HTTPException) as denied:
                await self.main.buy_credits_checkout(
                    "free3d-10credits",
                    request=req,
                    user=None,
                    db=db,
            )
            self.assertEqual(denied.exception.status_code, 404)

    async def test_blender_plugin_ab_selection_and_checkout_records_intent(self):
        fake_user = lambda user_id: type("FakeUser", (), {"id": user_id, "email": f"u{user_id}@example.com"})()
        self.assertEqual(self.main._select_blender_plugin_variant(fake_user(1)), ("blender-plugin-10", 10))
        self.assertEqual(self.main._select_blender_plugin_variant(fake_user(2)), ("blender-plugin-30", 30))
        self.assertEqual(self.main._select_blender_plugin_variant(fake_user(3)), ("blender-plugin-50", 50))
        self.assertEqual(self.main._select_blender_plugin_variant(fake_user(4)), ("blender-plugin", 100))

        async with self.database.AsyncSessionLocal() as db:
            buyer = (
                await db.execute(
                    self.main.select(self.database.User).where(self.database.User.email == "buyer@example.com")
                )
            ).scalar_one()
            expected_key, expected_price = self.main._select_blender_plugin_variant(buyer)

            req = self._fake_request(
                method="GET",
                path="/blender-plugin/checkout",
                query_string=b"source=buy_credits_page",
                headers=[(b"referer", b"https://autorig.test/buy-credits")],
            )
            with patch("telegram_bot.broadcast_credits_purchase_click", new=AsyncMock()) as tg:
                resp = await self.main.blender_plugin_checkout(
                    request=req,
                    source="buy_credits_page",
                    user=buyer,
                    db=db,
                )
                await asyncio.sleep(0)

            self.assertEqual(resp.status_code, 303)
            self.assertEqual(
                resp.headers["location"],
                f"https://u3d.gumroad.com/l/{expected_key}?userid=buyer%40example.com",
            )
            tg.assert_called_once()
            self.assertEqual(tg.call_args.kwargs["product_kind"], "plugin")
            self.assertEqual(tg.call_args.kwargs["price"], self.main._format_usd_price(expected_price))
            self.assertEqual(tg.call_args.kwargs["permalink"], expected_key)

            rows = (
                await db.execute(
                    self.main.select(self.database.PurchaseCheckoutIntent).where(
                        self.database.PurchaseCheckoutIntent.user_email == "buyer@example.com",
                        self.database.PurchaseCheckoutIntent.product_kind == "plugin",
                    )
                )
            ).scalars().all()
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0].product_permalink, expected_key)
            self.assertEqual(rows[0].source, "buy_credits_page")
            self.assertIsNone(rows[0].task_id)
            self.assertIsNone(rows[0].required_credits)

    async def test_gumroad_plugin_webhook_notifies_without_crediting(self):
        class DummyClient:
            async def __aenter__(self):
                return self

            async def __aexit__(self, *args):
                return False

            async def post(self, *args, **kwargs):
                class Resp:
                    status_code = 200

                return Resp()

        async with self.database.AsyncSessionLocal() as db:
            buyer = (
                await db.execute(
                    self.main.select(self.database.User).where(self.database.User.email == "buyer@example.com")
                )
            ).scalar_one()
            start_balance = buyer.balance_credits

        body = urlencode(
            {
                "sale_id": "plugin-sale-10",
                "email": "checkout@example.com",
                "url_params[userid]": "buyer@example.com",
                "product_permalink": "blender-plugin-10",
                "product_name": "Auto Animal Rig - Blender Plugin - $10",
                "price": "1000",
            }
        ).encode()
        req = self._fake_request(
            method="POST",
            path="/api-gumroad",
            headers=[(b"content-type", b"application/x-www-form-urlencoded")],
            body=body,
        )

        with (
            patch.object(self.main.httpx, "AsyncClient", return_value=DummyClient()),
            patch("telegram_bot.broadcast_credits_purchased", new=AsyncMock()) as tg,
        ):
            resp = await self.main.api_gumroad_ping(req)
            await asyncio.sleep(0)

        self.assertEqual(resp.status_code, 200)
        tg.assert_called_once()
        kwargs = tg.call_args.kwargs
        self.assertEqual(kwargs["product_kind"], "plugin")
        self.assertEqual(kwargs["package"], "Blender Plugin ABCD $10")
        self.assertEqual(kwargs["price"], "$10")
        self.assertEqual(kwargs["product"], "blender-plugin-10")
        self.assertEqual(kwargs["credits"], 0)

        async with self.database.AsyncSessionLocal() as db:
            buyer = (
                await db.execute(
                    self.main.select(self.database.User).where(self.database.User.email == "buyer@example.com")
                )
            ).scalar_one()
            self.assertEqual(buyer.balance_credits, start_balance)
            purchase = (
                await db.execute(
                    self.main.select(self.database.GumroadPurchase).where(
                        self.database.GumroadPurchase.sale_id == "plugin-sale-10"
                    )
                )
            ).scalar_one()
            self.assertEqual(purchase.email, "buyer@example.com")
            self.assertEqual(purchase.product_permalink, "blender-plugin-10")
            self.assertEqual(purchase.price, 1000)
            self.assertFalse(purchase.credited)
            self.assertEqual(purchase.credits_added, 0)

    async def test_gumroad_webhook_auto_unlocks_recent_task_checkout_intent(self):
        class DummyClient:
            async def __aenter__(self):
                return self

            async def __aexit__(self, *args):
                return False

            async def post(self, *args, **kwargs):
                class Resp:
                    status_code = 200

                return Resp()

        async with self.database.AsyncSessionLocal() as db:
            buyer = (
                await db.execute(
                    self.main.select(self.database.User).where(self.database.User.email == "buyer@example.com")
                )
            ).scalar_one()
            buyer.balance_credits = 0
            db.add(
                self.database.PurchaseCheckoutIntent(
                    user_email="buyer@example.com",
                    product_permalink="oneclick-30-credits",
                    product_kind="credits",
                    source="task_paywall_modal",
                    task_id=self.task_id,
                    required_credits=10,
                    created_at=datetime.now(timezone.utc).replace(tzinfo=None),
                )
            )
            await db.commit()

        body = urlencode(
            {
                "sale_id": "checkout-auto-unlock-sale",
                "email": "checkout@example.com",
                "url_params[userid]": "buyer@example.com",
                "product_permalink": "oneclick-30-credits",
                "product_name": "Autorig - 30 Credits",
                "price": "300",
            }
        ).encode()
        req = self._fake_request(
            method="POST",
            path="/api-gumroad",
            headers=[(b"content-type", b"application/x-www-form-urlencoded")],
            body=body,
        )

        with (
            patch.object(self.main.httpx, "AsyncClient", return_value=DummyClient()),
            patch("telegram_bot.broadcast_credits_purchased", new=AsyncMock()),
        ):
            resp = await self.main.api_gumroad_ping(req)
            await asyncio.sleep(0)

        self.assertEqual(resp.status_code, 200)
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
            purchase = (
                await db.execute(
                    self.main.select(self.database.GumroadPurchase).where(
                        self.database.GumroadPurchase.sale_id == "checkout-auto-unlock-sale"
                    )
                )
            ).scalar_one()
            unlock = (
                await db.execute(
                    self.main.select(self.database.TaskFilePurchase).where(
                        self.database.TaskFilePurchase.task_id == self.task_id,
                        self.database.TaskFilePurchase.user_email == "buyer@example.com",
                        self.database.TaskFilePurchase.file_index.is_(None),
                    )
                )
            ).scalar_one()
            intent = (
                await db.execute(
                    self.main.select(self.database.PurchaseCheckoutIntent).where(
                        self.database.PurchaseCheckoutIntent.user_email == "buyer@example.com"
                    )
                )
            ).scalar_one()

            self.assertTrue(purchase.credited)
            self.assertEqual(purchase.credits_added, 30)
            self.assertEqual(unlock.credits_spent, 10)
            self.assertEqual(buyer.balance_credits, 20)
            self.assertEqual(owner.balance_credits, 10)
            self.assertEqual(intent.gumroad_sale_id, "checkout-auto-unlock-sale")
            self.assertEqual(intent.auto_unlock_status, "unlocked")
            self.assertIsNotNone(intent.used_at)

    async def test_animal_animation_catalog_pack_purchase_and_download(self):
        task_id = "33333333-4444-5555-6666-777777777777"
        guid = "aaaaaaaa-1111-2222-3333-bbbbbbbbbbbb"
        fbx_name = f"{guid}_dog_front_all_animations_unity.fbx"
        fbx_url = f"http://worker.example/converter/glb/{guid}/{fbx_name}"
        worker_files = [
            {"name": fbx_name, "url": fbx_url, "size": 1234},
        ]
        matrix = {
            "dog:front": {
                "animal_slug": "dog",
                "orientation": "front",
                "status": "succeeded",
                "stage4_finalize": {
                    "after_actions": [
                        "Dog_default",
                        "Dog_dig",
                        "Dog_idle",
                        "Dog_run",
                        "Dog_trot",
                    ]
                },
            }
        }

        async with self.database.AsyncSessionLocal() as db:
            task = self.database.Task(
                id=task_id,
                owner_type="user",
                owner_id="owner@example.com",
                created_at=datetime.now(timezone.utc).replace(tzinfo=None),
                status="done",
                input_type="animal",
                guid=guid,
                worker_api=f"http://worker.example/converter/glb/{guid}/",
                viewer_settings='{"rig_v2_animal_detection":{"animal_type":"horse"}}',
            )
            db.add(task)
            await db.commit()

        with (
            patch.object(self.main, "_fetch_worker_model_files", new=AsyncMock(return_value=(True, worker_files, {}, None))),
            patch.object(self.main, "_fetch_animal_variant_matrix", new=AsyncMock(return_value=matrix)),
        ):
            async with self.database.AsyncSessionLocal() as db:
                buyer = (
                    await db.execute(
                        self.main.select(self.database.User).where(self.database.User.email == "buyer@example.com")
                    )
                ).scalar_one()

                catalog = await self.main.api_get_animation_catalog(
                    task_id,
                    animal_type="dog",
                    orientation="front",
                    user=buyer,
                    db=db,
                )
                self.assertEqual(catalog.pricing["animal_animation_pack_credits"], 10)
                self.assertFalse(catalog.pricing["animal_animation_pack_purchased"])
                self.assertEqual(len(catalog.animations), 5)
                self.assertEqual(catalog.animations[3].action_name, "Dog_run")
                self.assertEqual(catalog.animations[3].source_kind, "animal_variant_pack")
                self.assertEqual(catalog.animations[3].download_scope, "variant_pack")
                self.assertTrue(catalog.animations[3].preview_url.endswith("/animal_dog_front_dog_run"))

                with patch.object(self.main, "_proxy_model_file", new=AsyncMock(return_value="preview-ok")) as proxy:
                    preview = await self.main.api_preview_animation(
                        task_id,
                        "animal_dog_front_dog_run",
                        request=self._fake_request(),
                        response=Response(),
                        user=buyer,
                        db=db,
                    )
                self.assertEqual(preview, "preview-ok")
                proxy.assert_awaited_once()

                with self.assertRaises(self.main.HTTPException) as denied:
                    await self.main.api_download_animal_animation_pack(
                        task_id,
                        animal_type="dog",
                        orientation="front",
                        user=buyer,
                        db=db,
                    )
                self.assertEqual(denied.exception.status_code, 402)

                c0 = buyer.balance_credits
                purchased = await self.main.api_purchase_animation(
                    task_id,
                    self.models.AnimationPurchaseRequest(all=True, animal_type="dog", orientation="front"),
                    user=buyer,
                    db=db,
                )
                self.assertTrue(purchased.success)
                self.assertTrue(purchased.purchased_all)
                self.assertEqual(buyer.balance_credits, c0 - 10)
                unlock = (
                    await db.execute(
                        self.main.select(self.database.TaskFilePurchase).where(
                            self.database.TaskFilePurchase.task_id == task_id,
                            self.database.TaskFilePurchase.user_email == "buyer@example.com",
                            self.database.TaskFilePurchase.file_index.is_(None),
                        )
                    )
                ).scalar_one()
                self.assertEqual(unlock.credits_spent, 10)
                animal_rows = (
                    await db.execute(
                        self.main.select(self.database.TaskAnimalAnimationPackPurchase).where(
                            self.database.TaskAnimalAnimationPackPurchase.task_id == task_id,
                            self.database.TaskAnimalAnimationPackPurchase.user_email == "buyer@example.com",
                        )
                    )
                ).scalars().all()
                self.assertEqual(animal_rows, [])

                c1 = buyer.balance_credits
                duplicate = await self.main.api_purchase_animation(
                    task_id,
                    self.models.AnimationPurchaseRequest(animation_id="animal_dog_front_dog_run"),
                    user=buyer,
                    db=db,
                )
                self.assertTrue(duplicate.success)
                self.assertEqual(buyer.balance_credits, c1)

                with patch.object(self.main, "_download_worker_file_bytes", new=AsyncMock(return_value=b"fbx bytes")):
                    response = await self.main.api_download_animal_animation_pack(
                        task_id,
                        animal_type="dog",
                        orientation="front",
                        user=buyer,
                        db=db,
                    )
                self.assertEqual(response.media_type, "application/zip")
                Path(response.path).unlink(missing_ok=True)

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

    def _fake_request(
        self,
        method: str = "POST",
        path: Optional[str] = None,
        query_string: bytes = b"",
        headers: Optional[list[tuple[bytes, bytes]]] = None,
        body: bytes = b"",
    ) -> Request:
        sent = False

        async def receive():
            nonlocal sent
            if sent:
                return {"type": "http.disconnect"}
            sent = True
            return {"type": "http.request", "body": body, "more_body": False}

        return Request(
            {
                "type": "http",
                "method": method,
                "path": path or f"/api/task/{self.task_id}/purchases",
                "headers": headers or [],
                "query_string": query_string,
                "client": ("127.0.0.1", 12345),
                "scheme": "http",
                "server": ("testserver", 80),
            },
            receive=receive,
        )


if __name__ == "__main__":
    unittest.main()
