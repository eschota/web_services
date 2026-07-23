import sys
import unittest
from unittest.mock import patch
from datetime import datetime
from pathlib import Path

from fastapi import HTTPException


BACKEND_DIR = Path(__file__).resolve().parents[1]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from model_sale_offers import (
    ModelSaleOffer,
    _accept_page,
    _amount_cents,
    _public_offer,
    _token_hash,
)
from model_sale_emails import ADMIN_SALE_EMAIL, send_new_offer_emails, send_offer_accepted_emails


class ModelSaleOfferContractTests(unittest.TestCase):
    def test_amount_accepts_presets_and_two_decimal_custom_value(self):
        self.assertEqual(_amount_cents(20), 2000)
        self.assertEqual(_amount_cents("50.00"), 5000)
        self.assertEqual(_amount_cents("1.25"), 125)

    def test_amount_rejects_zero_negative_excess_precision_and_excessive_value(self):
        for value in (0, -1, "0.99", "1.001", "1000000.01", "nan", True):
            with self.subTest(value=value):
                with self.assertRaises(HTTPException):
                    _amount_cents(value)

    def test_public_offer_does_not_expose_emails_or_token(self):
        offer = ModelSaleOffer(
            id=7,
            task_id="task-1",
            author_email="author@example.com",
            buyer_email="buyer@example.com",
            amount_cents=2000,
            currency="USD",
            status="pending",
            accept_token_sha256=_token_hash("secret"),
            submission_key="submission-1",
            token_expires_at=datetime.utcnow(),
            created_at=datetime.utcnow(),
        )
        payload = _public_offer(offer)
        serialized = repr(payload)
        self.assertNotIn("author@example.com", serialized)
        self.assertNotIn("buyer@example.com", serialized)
        self.assertNotIn(_token_hash("secret"), serialized)

    def test_review_page_uses_post_confirmation(self):
        offer = ModelSaleOffer(
            task_id="task-1",
            author_email="author@example.com",
            buyer_email="buyer@example.com",
            amount_cents=5000,
            currency="USD",
            status="pending",
            accept_token_sha256=_token_hash("secret"),
            submission_key="submission-2",
            token_expires_at=datetime.utcnow(),
            created_at=datetime.utcnow(),
        )
        html = _accept_page(token="secret", offer=offer).body.decode("utf-8")
        self.assertIn('method="post"', html)
        self.assertIn('action="/api/model-sale/accept"', html)
        self.assertNotIn("author@example.com", html)

    def test_offer_and_acceptance_notify_required_recipients(self):
        calls = []

        def fake_send(to_email, subject, html):
            calls.append((to_email, subject, html))
            return True, "provider-id"

        async def scenario():
            with patch("model_sale_emails._send", side_effect=fake_send):
                await send_new_offer_emails(
                    task_id="task-1",
                    author_email="author@example.com",
                    buyer_email="buyer@example.com",
                    amount_cents=10000,
                    accept_token="secret",
                )
                await send_offer_accepted_emails(
                    task_id="task-1",
                    author_email="author@example.com",
                    buyer_email="buyer@example.com",
                    amount_cents=10000,
                )

        import asyncio
        asyncio.run(scenario())
        self.assertEqual(
            [call[0] for call in calls],
            ["author@example.com", ADMIN_SALE_EMAIL, "buyer@example.com", ADMIN_SALE_EMAIL],
        )


if __name__ == "__main__":
    unittest.main()
