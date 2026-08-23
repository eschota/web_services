import asyncio
import socket
import unittest

import scheduler_wakeup


class SchedulerWakeTests(unittest.TestCase):
    def test_pending_wake_is_not_erased_by_duplicate(self):
        async def scenario():
            wake_queue = asyncio.Queue(maxsize=1)
            scheduler_wakeup.enqueue_wake(wake_queue)
            scheduler_wakeup.enqueue_wake(wake_queue)
            self.assertTrue(
                await scheduler_wakeup.wait_for_wake(wake_queue, timeout=0.05)
            )
            self.assertTrue(wake_queue.empty())

        asyncio.run(scenario())

    def test_loopback_udp_wakes_backend_queue(self):
        async def scenario():
            wake_queue = asyncio.Queue(maxsize=1)
            probe = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            probe.bind((scheduler_wakeup.WAKE_HOST, 0))
            port = int(probe.getsockname()[1])
            probe.close()
            transport = await scheduler_wakeup.start_wake_listener(
                wake_queue, port=port
            )
            self.assertIsNotNone(transport)
            try:
                self.assertTrue(scheduler_wakeup.notify_scheduler(port=port))
                self.assertTrue(
                    await scheduler_wakeup.wait_for_wake(wake_queue, timeout=1.0)
                )
            finally:
                transport.close()
                await asyncio.sleep(0)

        asyncio.run(scenario())

    def test_unrecognized_datagram_does_not_wake_scheduler(self):
        async def scenario():
            wake_queue = asyncio.Queue(maxsize=1)
            protocol = scheduler_wakeup.SchedulerWakeProtocol(wake_queue)
            protocol.datagram_received(b"wrong-message", ("127.0.0.1", 1))
            self.assertFalse(
                await scheduler_wakeup.wait_for_wake(wake_queue, timeout=0.01)
            )

        asyncio.run(scenario())


if __name__ == "__main__":
    unittest.main()
