import importlib
import asyncio
import os
import sys
import tempfile
import unittest
import httpx
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional
from urllib.parse import urlencode
from unittest.mock import AsyncMock, patch
from sqlalchemy import delete

from starlette.requests import Request
from starlette.responses import Response


class _FakeStreamingUpstream:
    def __init__(self, chunks, *, error_after_chunk=None, first_chunk_delay=0):
        total_size = sum(len(chunk) for chunk in chunks)
        self.status_code = 200
        self.headers = {
            "content-length": str(total_size),
            "content-type": "application/x-blender",
            "etag": '"stable"',
            "last-modified": "Mon, 13 Jul 2026 15:52:17 GMT",
        }
        self._chunks = list(chunks)
        self._error_after_chunk = error_after_chunk
        self._first_chunk_delay = first_chunk_delay
        self.close_count = 0

    async def aiter_bytes(self, chunk_size=None):
        for index, chunk in enumerate(self._chunks):
            if index == 0 and self._first_chunk_delay:
                await asyncio.sleep(self._first_chunk_delay)
            yield chunk
            if self._error_after_chunk == index:
                raise RuntimeError("simulated midstream failure")

    async def aclose(self):
        self.close_count += 1


class _FakeProxyClient:
    def __init__(self, upstream):
        self.upstream = upstream
        self.close_count = 0
        self.send_follow_redirects = None

    def build_request(self, method, url):
        return {"method": method, "url": url}

    async def send(self, request, *, stream, follow_redirects):
        self.send_follow_redirects = follow_redirects
        return self.upstream

    async def aclose(self):
        self.close_count += 1


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

    async def test_animal_catalog_uses_exact_primary_manifest_when_matrix_row_is_missing(self):
        task_id = "4aa0a49d-e47c-4a45-b8d6-f5c9bb7f6750"
        guid = "3a63d32f-8bfe-4182-94c3-6e93021231d8"
        worker_root = f"http://worker.example/converter/glb/{guid}"
        fbx_name = f"{guid}_all_animations_unity.fbx"
        manifest_name = f"{guid}_all_animations.manifest.json"
        glb_name = f"{guid}_all_animations.glb"
        manifest_url = f"{worker_root}/{manifest_name}"
        files = [
            {
                "name": fbx_name,
                "folder": "root",
                "rel_path": fbx_name,
                "url": f"{worker_root}/{fbx_name}",
                "size": 1024,
            },
            {
                "name": manifest_name,
                "folder": "root",
                "rel_path": manifest_name,
                "url": manifest_url,
                "size": 989,
            },
            {
                "name": glb_name,
                "folder": "root",
                "rel_path": glb_name,
                "url": f"{worker_root}/{glb_name}",
                "size": 14577528,
            },
        ]
        payload = {
            "schema_version": 1,
            "artifact_type": "animal_all_animations_glb",
            "artifact_file": f"{guid}_all_animations.glb",
            "clip_count": 2,
            "exported_clip_names": ["Horse_default", "Horse_gallop"],
            "clips": [
                {"name": "Horse_default"},
                {"name": "Horse_gallop"},
            ],
        }
        parsed_actions = self.main._parse_exact_animal_animation_manifest(
            payload,
            expected_artifact_name=f"{guid}_all_animations.glb",
        )
        self.assertEqual(parsed_actions, ["Horse_default", "Horse_gallop"])

        async with self.database.AsyncSessionLocal() as db:
            task = self.database.Task(
                id=task_id,
                owner_type="user",
                owner_id="owner@example.com",
                created_at=datetime.now(timezone.utc).replace(tzinfo=None),
                status="done",
                input_type="animal",
                guid=guid,
                worker_api="http://worker.example/api-converter-glb",
                viewer_settings='{"rig_v2_animal_detection":{"animal_type":"horse"}}',
            )
            purchase = self.database.TaskAnimalAnimationPackPurchase(
                task_id=task_id,
                user_email="buyer@example.com",
                animal_type="horse",
                orientation="front",
                credits_spent=10,
            )
            db.add_all([task, purchase])
            await db.commit()
            await db.refresh(task)

            requests = []

            def manifest_handler(request):
                requests.append(request)
                return httpx.Response(200, json=payload)

            async with httpx.AsyncClient(
                transport=httpx.MockTransport(manifest_handler),
                follow_redirects=False,
            ) as manifest_client:
                fetched_actions = await self.main._fetch_exact_animal_animation_manifest_actions(
                    task,
                    files,
                    "horse",
                    "front",
                    client=manifest_client,
                )

        self.assertEqual(fetched_actions, ["Horse_default", "Horse_gallop"])
        self.assertEqual(len(requests), 1)
        self.assertEqual(requests[0].method, "GET")
        self.assertEqual(str(requests[0].url), manifest_url)

        fetch_actions = AsyncMock(return_value=fetched_actions)
        with (
            patch.object(self.main, "_fetch_worker_model_files", new=AsyncMock(return_value=(True, files, {}, None))),
            patch.object(self.main, "_fetch_animal_variant_matrix", new=AsyncMock(return_value={})),
            patch.object(self.main, "_fetch_exact_animal_animation_manifest_actions", new=fetch_actions),
        ):
            async with self.database.AsyncSessionLocal() as db:
                buyer = (
                    await db.execute(
                        self.main.select(self.database.User).where(self.database.User.email == "buyer@example.com")
                    )
                ).scalar_one()
                credits_before = buyer.balance_credits
                catalog = await self.main.api_get_animation_catalog(task_id, user=buyer, db=db)

        self.assertEqual([item.action_name for item in catalog.animations], ["Horse_default", "Horse_gallop"])
        self.assertTrue(catalog.purchased_all)
        self.assertTrue(all(item.purchased for item in catalog.animations))
        self.assertEqual(buyer.balance_credits, credits_before)
        fetch_actions.assert_awaited_once()

    async def test_animal_catalog_manifest_fallback_rejects_malformed_and_ambiguous_paths(self):
        task_id = "9ae1639b-ea0b-40c1-87aa-f15be7deaa30"
        guid = "c80cadb2-7e34-40e4-82ac-8189432f19bd"
        worker_root = f"http://worker.example/converter/glb/{guid}"
        fbx_name = f"{guid}_all_animations_unity.fbx"
        manifest_name = f"{guid}_all_animations.manifest.json"
        manifest_item = {
            "name": manifest_name,
            "folder": "root",
            "rel_path": manifest_name,
            "url": f"{worker_root}/{manifest_name}",
            "size": 900,
        }
        glb_name = f"{guid}_all_animations.glb"
        artifact_item = {
            "name": glb_name,
            "folder": "root",
            "rel_path": glb_name,
            "url": f"{worker_root}/{glb_name}",
            "size": 14577528,
        }
        files = [
            {
                "name": fbx_name,
                "folder": "root",
                "rel_path": fbx_name,
                "url": f"{worker_root}/{fbx_name}",
                "size": 1024,
            },
            manifest_item,
            dict(manifest_item),
            artifact_item,
        ]

        async with self.database.AsyncSessionLocal() as db:
            task = self.database.Task(
                id=task_id,
                owner_type="user",
                owner_id="owner@example.com",
                created_at=datetime.now(timezone.utc).replace(tzinfo=None),
                status="done",
                input_type="animal",
                guid=guid,
                worker_api="http://worker.example/api-converter-glb",
                viewer_settings='{"rig_v2_animal_detection":{"animal_type":"horse"}}',
            )
            db.add(task)
            await db.commit()
            await db.refresh(task)

            self.assertIsNone(
                self.main._select_exact_animal_animation_manifest(task, files, "horse", "front")
            )
            nested = dict(manifest_item, folder="logs", rel_path=f"logs/{manifest_name}")
            self.assertIsNone(
                self.main._select_exact_animal_animation_manifest(
                    task,
                    [nested, artifact_item],
                    "horse",
                    "front",
                )
            )
            self.assertIsNone(
                self.main._select_exact_animal_animation_manifest(
                    task,
                    [manifest_item, dict(artifact_item, size=0)],
                    "horse",
                    "front",
                )
            )

        malformed = {
            "schema_version": 1,
            "artifact_type": "animal_all_animations_glb",
            "artifact_file": f"{guid}_all_animations.glb",
            "clip_count": 1,
            "exported_clip_names": ["../Horse_gallop"],
            "clips": [{"name": "../Horse_gallop"}],
        }
        self.assertEqual(
            self.main._parse_exact_animal_animation_manifest(
                malformed,
                expected_artifact_name=f"{guid}_all_animations.glb",
            ),
            [],
        )

        valid_inventory = [manifest_item, artifact_item]

        def malformed_handler(_request):
            return httpx.Response(200, json=malformed)

        async with httpx.AsyncClient(
            transport=httpx.MockTransport(malformed_handler),
            follow_redirects=False,
        ) as manifest_client:
            malformed_actions = await self.main._fetch_exact_animal_animation_manifest_actions(
                task,
                valid_inventory,
                "horse",
                "front",
                client=manifest_client,
            )
        self.assertEqual(malformed_actions, [])

        oversized_calls = 0

        def oversized_handler(_request):
            nonlocal oversized_calls
            oversized_calls += 1
            return httpx.Response(
                200,
                headers={
                    "Content-Length": str(self.main._PRIMARY_ANIMAL_ANIMATION_MANIFEST_MAX_BYTES + 1),
                },
                content=b"{}",
            )

        async with httpx.AsyncClient(
            transport=httpx.MockTransport(oversized_handler),
            follow_redirects=False,
        ) as manifest_client:
            oversized_actions = await self.main._fetch_exact_animal_animation_manifest_actions(
                task,
                valid_inventory,
                "horse",
                "front",
                client=manifest_client,
            )
        self.assertEqual(oversized_actions, [])
        self.assertEqual(oversized_calls, 1)

        class OversizedAsyncStream(httpx.AsyncByteStream):
            def __init__(self, payload_size):
                self.close_count = 0
                self.payload_size = payload_size

            async def __aiter__(self):
                yield b"x" * self.payload_size

            async def aclose(self):
                self.close_count += 1

        oversized_stream = OversizedAsyncStream(
            self.main._PRIMARY_ANIMAL_ANIMATION_MANIFEST_MAX_BYTES + 100
        )

        def streamed_oversized_handler(_request):
            return httpx.Response(
                200,
                headers={"Transfer-Encoding": "chunked"},
                stream=oversized_stream,
            )

        async with httpx.AsyncClient(
            transport=httpx.MockTransport(streamed_oversized_handler),
            follow_redirects=False,
        ) as manifest_client:
            streamed_oversized_actions = await self.main._fetch_exact_animal_animation_manifest_actions(
                task,
                valid_inventory,
                "horse",
                "front",
                client=manifest_client,
            )
        self.assertEqual(streamed_oversized_actions, [])
        self.assertEqual(oversized_stream.close_count, 1)

        redirect_calls = 0

        def redirect_handler(_request):
            nonlocal redirect_calls
            redirect_calls += 1
            return httpx.Response(302, headers={"Location": "http://other.example/manifest.json"})

        async with httpx.AsyncClient(
            transport=httpx.MockTransport(redirect_handler),
            follow_redirects=True,
        ) as manifest_client:
            redirect_actions = await self.main._fetch_exact_animal_animation_manifest_actions(
                task,
                valid_inventory,
                "horse",
                "front",
                client=manifest_client,
            )
        self.assertEqual(redirect_actions, [])
        self.assertEqual(redirect_calls, 1)

        with (
            patch.object(self.main, "_fetch_worker_model_files", new=AsyncMock(return_value=(True, files, {}, None))),
            patch.object(self.main, "_fetch_animal_variant_matrix", new=AsyncMock(return_value={})),
            patch.object(
                self.main,
                "_fetch_exact_animal_animation_manifest_actions",
                new=AsyncMock(return_value=[]),
            ),
        ):
            async with self.database.AsyncSessionLocal() as db:
                catalog = await self.main.api_get_animation_catalog(task_id, user=None, db=db)
        self.assertEqual([item.action_name for item in catalog.animations], ["Horse_default"])

    async def test_animal_catalog_matrix_actions_remain_authoritative_over_primary_manifest(self):
        task_id = "9cce8781-4e17-4d7c-a804-57673e93fc46"
        guid = "a91bcab6-267d-4b17-83ed-32da32dbc0ac"
        fbx_name = f"{guid}_all_animations_unity.fbx"
        fbx_url = f"http://worker.example/converter/glb/{guid}/{fbx_name}"
        files = [{"name": fbx_name, "url": fbx_url, "size": 1024}]
        matrix = {
            "horse:front": {
                "animal_slug": "horse",
                "orientation": "front",
                "stage4_finalize": {
                    "after_actions": ["Horse_default", "Horse_trot"],
                },
            },
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
                worker_api="http://worker.example/api-converter-glb",
                viewer_settings='{"rig_v2_animal_detection":{"animal_type":"horse"}}',
            )
            db.add(task)
            await db.commit()

        fetch_actions = AsyncMock(return_value=["Horse_default", "Horse_gallop"])
        with (
            patch.object(self.main, "_fetch_worker_model_files", new=AsyncMock(return_value=(True, files, {}, None))),
            patch.object(self.main, "_fetch_animal_variant_matrix", new=AsyncMock(return_value=matrix)),
            patch.object(self.main, "_fetch_exact_animal_animation_manifest_actions", new=fetch_actions),
        ):
            async with self.database.AsyncSessionLocal() as db:
                catalog = await self.main.api_get_animation_catalog(task_id, user=None, db=db)

        self.assertEqual([item.action_name for item in catalog.animations], ["Horse_default", "Horse_trot"])
        fetch_actions.assert_not_awaited()

    async def test_variant_blend_probe_rejects_zero_then_accepts_stable_nonzero_metadata(self):
        url = "http://worker.example/converter/glb/guid/guid_rigged.blend"
        calls = 0

        def handler(request):
            nonlocal calls
            self.assertEqual(request.method, "HEAD")
            calls += 1
            if calls == 1:
                return httpx.Response(200, headers={"Content-Length": "0", "ETag": '"writing"'})
            return httpx.Response(
                200,
                headers={
                    "Content-Length": "105937432",
                    "ETag": '"stable"',
                    "Last-Modified": "Mon, 13 Jul 2026 15:52:17 GMT",
                },
            )

        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            first = await self.main._probe_stable_remote_file(
                url,
                client=client,
                stability_delay_seconds=0,
            )
            stable = await self.main._probe_stable_remote_file(
                url,
                client=client,
                stability_delay_seconds=0,
            )

        self.assertIsNone(first)
        self.assertEqual(stable["content_length"], 105937432)
        self.assertEqual(stable["etag"], '"stable"')
        self.assertEqual(calls, 3)

    async def test_guarded_model_proxy_rejects_zero_length_and_empty_stream(self):
        url = "http://worker.example/converter/glb/guid/guid_rigged.blend"

        async def assert_rejected(headers):
            def handler(_request):
                return httpx.Response(200, headers=headers, content=b"")

            client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
            snapshot = {
                "content_length": 10,
                "etag": '"stable"',
                "last_modified": "Mon, 13 Jul 2026 15:52:17 GMT",
            }
            with patch.object(self.main.httpx, "AsyncClient", return_value=client):
                with self.assertRaises(self.main.HTTPException) as rejected:
                    await self.main._proxy_model_file(
                        url,
                        "guid_rigged.blend",
                        as_attachment=True,
                        required_snapshot=snapshot,
                    )
            self.assertEqual(rejected.exception.status_code, 503)
            self.assertEqual(rejected.exception.headers["Retry-After"], "3")
            self.assertEqual(rejected.exception.headers["Cache-Control"], "no-store")

        await assert_rejected(
            {
                "Content-Length": "0",
                "ETag": '"stable"',
                "Last-Modified": "Mon, 13 Jul 2026 15:52:17 GMT",
            }
        )
        await assert_rejected(
            {
                "Content-Length": "10",
                "ETag": '"stable"',
                "Last-Modified": "Mon, 13 Jul 2026 15:52:17 GMT",
            }
        )

    async def test_guarded_model_proxy_streams_exact_bytes_without_redirect_and_cleans_once(self):
        upstream = _FakeStreamingUpstream([b"abc", b"def"])
        client = _FakeProxyClient(upstream)
        factory_kwargs = {}

        def client_factory(**kwargs):
            factory_kwargs.update(kwargs)
            return client

        snapshot = {
            "content_length": 6,
            "etag": '"stable"',
            "last_modified": "Mon, 13 Jul 2026 15:52:17 GMT",
        }
        with patch.object(self.main.httpx, "AsyncClient", new=client_factory):
            response = await self.main._proxy_model_file(
                "http://worker.example/model.blend",
                "model.blend",
                as_attachment=True,
                required_snapshot=snapshot,
            )

        chunks = []
        async for chunk in response.body_iterator:
            chunks.append(chunk)
        self.assertEqual(b"".join(chunks), b"abcdef")
        self.assertFalse(factory_kwargs["follow_redirects"])
        self.assertFalse(client.send_follow_redirects)
        self.assertEqual(upstream.close_count, 1)
        self.assertEqual(client.close_count, 1)

        await response.background()
        self.assertEqual(upstream.close_count, 1)
        self.assertEqual(client.close_count, 1)

    async def test_guarded_model_proxy_midstream_error_closes_resources_once(self):
        upstream = _FakeStreamingUpstream([b"abc", b"def"], error_after_chunk=0)
        client = _FakeProxyClient(upstream)
        snapshot = {
            "content_length": 6,
            "etag": '"stable"',
            "last_modified": "Mon, 13 Jul 2026 15:52:17 GMT",
        }
        with patch.object(self.main.httpx, "AsyncClient", return_value=client):
            response = await self.main._proxy_model_file(
                "http://worker.example/model.blend",
                "model.blend",
                as_attachment=True,
                required_snapshot=snapshot,
            )

        received = []
        with self.assertRaisesRegex(RuntimeError, "midstream"):
            async for chunk in response.body_iterator:
                received.append(chunk)
        self.assertEqual(b"".join(received), b"abc")
        self.assertEqual(upstream.close_count, 1)
        self.assertEqual(client.close_count, 1)

        await response.background()
        self.assertEqual(upstream.close_count, 1)
        self.assertEqual(client.close_count, 1)

    async def test_guarded_model_proxy_first_byte_timeout_is_retryable_and_closes(self):
        upstream = _FakeStreamingUpstream([b"x"], first_chunk_delay=0.1)
        client = _FakeProxyClient(upstream)
        snapshot = {
            "content_length": 1,
            "etag": '"stable"',
            "last_modified": "Mon, 13 Jul 2026 15:52:17 GMT",
        }
        with (
            patch.object(self.main.httpx, "AsyncClient", return_value=client),
            patch.object(self.main, "_ANIMAL_VARIANT_FIRST_BYTE_TIMEOUT_SECONDS", 0.01),
        ):
            with self.assertRaises(self.main.HTTPException) as timed_out:
                await self.main._proxy_model_file(
                    "http://worker.example/model.blend",
                    "model.blend",
                    as_attachment=True,
                    required_snapshot=snapshot,
                )

        self.assertEqual(timed_out.exception.status_code, 503)
        self.assertEqual(timed_out.exception.headers["Retry-After"], "3")
        self.assertEqual(upstream.close_count, 1)
        self.assertEqual(client.close_count, 1)

    async def test_repeated_stable_variant_download_uses_existing_purchase_without_charging(self):
        task_id = "29e252f3-43ec-45b6-8383-161a6d72b4f6"
        guid = "d005554b-1e46-46f5-8786-0124f9c96d14"
        source_url = f"http://worker.example/converter/glb/{guid}/{guid}_rigged.blend"
        snapshot = {
            "content_length": 105937432,
            "etag": '"stable"',
            "last_modified": "Mon, 13 Jul 2026 15:52:17 GMT",
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
                worker_api="http://worker.example/api-converter-glb",
                viewer_settings='{"rig_v2_animal_detection":{"animal_type":"horse"}}',
            )
            purchase = self.database.TaskFilePurchase(
                task_id=task_id,
                user_email="buyer@example.com",
                file_index=None,
                credits_spent=10,
            )
            db.add_all([task, purchase])
            await db.commit()

        proxy = AsyncMock(return_value="download-ok")
        with (
            patch.object(
                self.main,
                "_resolve_animal_variant_source",
                new=AsyncMock(return_value=(source_url, f"{guid}_rigged.blend")),
            ),
            patch.object(self.main, "_probe_stable_remote_file", new=AsyncMock(return_value=snapshot)),
            patch.object(self.main, "_proxy_model_file", new=proxy),
        ):
            async with self.database.AsyncSessionLocal() as db:
                buyer = (
                    await db.execute(
                        self.main.select(self.database.User).where(self.database.User.email == "buyer@example.com")
                    )
                ).scalar_one()
                credits_before = buyer.balance_credits
                first = await self.main.api_animal_variant_download(
                    task_id,
                    "horse",
                    "front",
                    "blend",
                    user=buyer,
                    db=db,
                )
                second = await self.main.api_animal_variant_download(
                    task_id,
                    "horse",
                    "front",
                    "blend",
                    user=buyer,
                    db=db,
                )
                purchases = (
                    await db.execute(
                        self.main.select(self.database.TaskFilePurchase).where(
                            self.database.TaskFilePurchase.task_id == task_id,
                            self.database.TaskFilePurchase.user_email == "buyer@example.com",
                            self.database.TaskFilePurchase.file_index.is_(None),
                        )
                    )
                ).scalars().all()

        self.assertEqual(first, "download-ok")
        self.assertEqual(second, "download-ok")
        self.assertEqual(buyer.balance_credits, credits_before)
        self.assertEqual(len(purchases), 1)
        self.assertEqual(proxy.await_count, 2)
        for call in proxy.await_args_list:
            self.assertEqual(call.kwargs["required_snapshot"], snapshot)

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
