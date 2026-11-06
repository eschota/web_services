"""A Gumroad membership sale must switch the subscription on at once.

Before this test the webhook ran every AutoRig product through the credit-pack
branch: the monthly plan has no row in GUMROAD_PRODUCT_CREDITS, so the buyer was
credited the price in cents (2000 credits for $20) and the subscription itself
was only applied at the buyer's next login, which a logged-in buyer never does.
"""
import asyncio
import os
import sys
import tempfile
import unittest
from pathlib import Path
from urllib.parse import urlencode

BACKEND = Path(__file__).resolve().parents[1]
if str(BACKEND) not in sys.path:
    sys.path.insert(0, str(BACKEND))

_DB_DIR = tempfile.mkdtemp(prefix="autorig-subwebhook-")
os.environ["DATABASE_URL"] = f"sqlite+aiosqlite:///{_DB_DIR}/t.db"
os.environ.setdefault("GUMROAD_PROXY_TARGET", "http://127.0.0.1:9/")


def sale_payload(email, *, sale_id="sale-1", recurring=False, **extra):
    data = {
        "sale_id": sale_id,
        "sale_timestamp": "2026-10-01T10:00:00Z",
        "email": email,
        "url_params[userid]": email,
        "permalink": "autorig-unlimited-monthly",
        "product_permalink": "https://u3d.gumroad.com/l/autorig-unlimited-monthly",
        "short_product_id": "xkfczu",
        "product_name": "AutoRig Unlimited Monthly",
        "price": "2000",
        "recurrence": "monthly",
        "subscription_id": "sub-123",
        "is_recurring_charge": "true" if recurring else "false",
        "refunded": "false",
        "test": "false",
    }
    data.update(extra)
    return urlencode(data).encode()


class SubscriptionWebhookTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        import database
        import main
        import telegram_bot

        # A test sale must not reach the owner's Telegram or the free3d
        # upstream the webhook normally forwards to.
        async def silent(*args, **kwargs):
            return None
        telegram_bot.broadcast_credits_purchased = silent
        main.GUMROAD_PROXY_TARGET = "http://127.0.0.1:9/"
        cls.database, cls.main = database, main
        asyncio.run(database.init_db())

    def _post(self, body):
        from starlette.requests import Request

        async def receive():
            return {"type": "http.request", "body": body, "more_body": False}

        scope = {"type": "http", "method": "POST", "path": "/webhook/gumroad", "headers": [
            (b"content-type", b"application/x-www-form-urlencoded")], "query_string": b""}
        return asyncio.run(self.main.api_gumroad_ping(Request(scope, receive)))

    def _user(self, email, balance=5):
        async def go():
            async with self.database.AsyncSessionLocal() as db:
                user = self.database.User(email=email, balance_credits=balance)
                db.add(user)
                await db.commit()
        asyncio.run(go())

    def _reload(self, email):
        from sqlalchemy import select

        async def go():
            async with self.database.AsyncSessionLocal() as db:
                return (await db.execute(select(self.database.User).where(
                    self.database.User.email == email))).scalar_one()
        return asyncio.run(go())

    def test_sale_activates_the_subscription_immediately(self):
        from subscription_access import user_has_active_subscription
        self._user("buyer@example.com")
        self._post(sale_payload("buyer@example.com"))
        user = self._reload("buyer@example.com")
        self.assertTrue(user_has_active_subscription(user),
                        f"status={user.autorig_subscription_status} end={user.autorig_subscription_period_end}")

    def test_sale_does_not_turn_the_price_into_credits(self):
        self._user("credits@example.com", balance=5)
        self._post(sale_payload("credits@example.com", sale_id="sale-2"))
        self.assertEqual(self._reload("credits@example.com").balance_credits, 5)

    def test_monthly_renewal_extends_and_adds_no_credits(self):
        from subscription_access import user_has_active_subscription
        self._user("renew@example.com", balance=0)
        self._post(sale_payload("renew@example.com", sale_id="sale-3"))
        first_end = self._reload("renew@example.com").autorig_subscription_period_end
        self._post(sale_payload("renew@example.com", sale_id="sale-4", recurring=True,
                                sale_timestamp="2026-11-01T10:00:00Z"))
        user = self._reload("renew@example.com")
        self.assertTrue(user_has_active_subscription(user))
        self.assertGreaterEqual(user.autorig_subscription_period_end, first_end)
        self.assertEqual(user.balance_credits, 0)

    def test_legacy_credit_pack_still_credits(self):
        # The subscription branch must not swallow the credit packs still sold.
        self._user("pack@example.com", balance=0)
        self._post(sale_payload("pack@example.com", sale_id="sale-6",
                                permalink="autorig-100",
                                product_permalink="https://u3d.gumroad.com/l/autorig-100",
                                product_name="Autorig - 100 Credits", price="1000",
                                subscription_id="", recurrence=""))
        self.assertEqual(self._reload("pack@example.com").balance_credits, 100)

    def test_buyer_without_an_account_gets_it_at_first_login(self):
        # Webhook first, account later: the stored sale is replayed at login.
        from subscription_access import user_has_active_subscription
        self._post(sale_payload("later@example.com", sale_id="sale-5"))

        async def login():
            import auth
            async with self.database.AsyncSessionLocal() as db:
                return await auth.get_or_create_user(db, "later@example.com")
        asyncio.run(login())
        self.assertTrue(user_has_active_subscription(self._reload("later@example.com")))


if __name__ == "__main__":
    unittest.main()
