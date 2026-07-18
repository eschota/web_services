import unittest

from worker_labels import worker_label_from_url


class WorkerLabelsTests(unittest.TestCase):
    def test_freestock_hostname_label(self):
        self.assertEqual(
            ("F11", "converter F11, FreeStock HTTPS gateway"),
            worker_label_from_url(
                "https://converter-f11.freestock.online/api-converter-glb"
            ),
        )

    def test_legacy_port_label(self):
        label = worker_label_from_url(
            "http://5.129.157.224:5132/api-converter-glb"
        )
        self.assertIsNotNone(label)
        self.assertEqual("F1", label[0])


if __name__ == "__main__":
    unittest.main()
