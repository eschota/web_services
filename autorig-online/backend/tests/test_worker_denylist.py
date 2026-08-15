import asyncio
import os
import unittest
from unittest.mock import patch

import workers


def run(coro):
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(coro)
    finally:
        loop.close()


class WorkerDenylistTests(unittest.TestCase):
    def test_disabled_worker_matches_case_and_trailing_slash(self):
        with patch.dict(
            os.environ,
            {
                "AUTORIG_DISABLED_WORKERS": (
                    " HTTPS://CONVERTER-F7.FREESTOCK.ONLINE/api-converter-glb/ "
                )
            },
        ):
            self.assertTrue(
                workers.is_worker_disabled(
                    "https://converter-f7.freestock.online/api-converter-glb"
                )
            )

    def test_fallback_pool_never_returns_disabled_worker(self):
        disabled = "https://converter-f7.freestock.online/api-converter-glb"
        with patch.dict(os.environ, {"AUTORIG_DISABLED_WORKERS": disabled}):
            configured = run(workers.get_configured_workers_with_weight(None))
        urls = [url for url, _weight in configured]
        self.assertNotIn(disabled, urls)
        self.assertGreater(len(urls), 0)

    def test_empty_denylist_preserves_default_pool(self):
        with patch.dict(os.environ, {"AUTORIG_DISABLED_WORKERS": ""}):
            configured = run(workers.get_configured_workers_with_weight(None))
        self.assertEqual([url for url, _weight in configured], workers.WORKERS)
