import sys
import unittest
from datetime import datetime, timedelta
from pathlib import Path
from types import SimpleNamespace


BACKEND_DIR = Path(__file__).resolve().parents[1]
STATIC_DIR = BACKEND_DIR.parent / "static"
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from subscription_access import (  # noqa: E402
    add_calendar_month,
    apply_subscription_event,
    subscription_summary,
    user_has_active_subscription,
)


class SubscriptionEntitlementTests(unittest.TestCase):
    def _user(self):
        return SimpleNamespace(
            autorig_subscription_status="none",
            autorig_subscription_id=None,
            autorig_subscription_started_at=None,
            autorig_subscription_period_end=None,
            autorig_subscription_updated_at=None,
        )

    def test_calendar_month_clamps_month_end(self):
        self.assertEqual(
            add_calendar_month(datetime(2026, 1, 31, 12, 30)),
            datetime(2026, 2, 28, 12, 30),
        )

    def test_paid_month_is_active_without_manufacturing_credits(self):
        user = self._user()
        charged_at = datetime(2026, 9, 5, 8, 0)
        state = apply_subscription_event(
            user,
            subscription_id="sub-123",
            sale_at=charged_at,
            now=charged_at,
        )
        self.assertEqual(state, "active")
        self.assertEqual(user.autorig_subscription_period_end, datetime(2026, 10, 5, 8, 0))
        self.assertTrue(user_has_active_subscription(user, now=charged_at + timedelta(days=29)))
        self.assertFalse(user_has_active_subscription(user, now=datetime(2026, 10, 5, 8, 0)))

    def test_cancellation_keeps_access_until_gumroad_end_date(self):
        user = self._user()
        now = datetime(2026, 9, 5, 8, 0)
        end = datetime(2026, 9, 22, 8, 0)
        apply_subscription_event(
            user,
            subscription_id="sub-123",
            sale_at=now - timedelta(days=13),
            cancelled_at=end,
            now=now,
        )
        self.assertEqual(user.autorig_subscription_status, "canceling")
        self.assertTrue(subscription_summary(user, now=now)["cancel_at_period_end"])
        self.assertTrue(user_has_active_subscription(user, now=now))
        self.assertFalse(user_has_active_subscription(user, now=end))

    def test_refund_revokes_access_immediately(self):
        user = self._user()
        now = datetime(2026, 9, 5, 8, 0)
        apply_subscription_event(user, subscription_id="sub-123", sale_at=now, now=now)
        apply_subscription_event(
            user,
            subscription_id="sub-123",
            sale_at=now,
            refunded=True,
            now=now + timedelta(hours=1),
        )
        self.assertEqual(user.autorig_subscription_status, "refunded")
        self.assertFalse(user_has_active_subscription(user, now=now + timedelta(hours=1)))


class SubscriptionFrontendContractTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.buy_html = (STATIC_DIR / "buy-credits.html").read_text(encoding="utf-8")
        cls.task_html = (STATIC_DIR / "task.html").read_text(encoding="utf-8")
        cls.header_js = (STATIC_DIR / "js" / "header.js").read_text(encoding="utf-8")

    def test_public_pricing_has_one_monthly_checkout(self):
        visible = self.buy_html.split('<div id="panel-billing-crypto"', 1)[0]
        self.assertGreaterEqual(visible.count('/buy-credits/checkout/'), 1)
        checkout_fragments = [
            part.split('?', 1)[0].split('"', 1)[0]
            for part in visible.split('/buy-credits/checkout/')[1:]
        ]
        self.assertEqual(set(checkout_fragments), {"autorig-unlimited-monthly"})
        self.assertIn('/buy-credits/checkout/autorig-unlimited-monthly', visible)
        self.assertIn('$20', visible)
        self.assertNotIn('/buy-credits/checkout/oneclick-30-credits', visible)
        self.assertNotIn('/buy-credits/checkout/autorig-100', visible)

    def test_task_paywall_and_visibility_use_subscription_contract(self):
        self.assertIn("permalink: 'autorig-unlimited-monthly'", self.task_html)
        self.assertNotIn("permalink: 'oneclick-30-credits'", self.task_html)
        self.assertIn('id="task-visibility-btn"', self.task_html)
        self.assertIn('/visibility`, {', self.task_html)

    def test_header_shows_unlimited_instead_of_a_fake_credit_balance(self):
        self.assertIn("if (data.user.subscription_active)", self.header_js)
        self.assertIn("creditsCount.textContent = '∞'", self.header_js)


if __name__ == "__main__":
    unittest.main()
