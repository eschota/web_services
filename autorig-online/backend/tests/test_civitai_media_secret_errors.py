import asyncio
import os
import pathlib
import unittest
from unittest.mock import AsyncMock, patch

import httpx
from renderfin import video_input


class CivitaiSecretErrorTests(unittest.TestCase):
    def test_malformed_environment_token_is_never_quoted(self):
        secret = 'private-test-token\nmalformed'
        with patch.dict(os.environ, {'CIVITAI_API_TOKEN': secret}):
            with self.assertRaises(video_input.VideoInputError) as caught:
                video_input._civitai_headers('https://image.civitai.com/example.webm')
        self.assertNotIn('private-test-token', str(caught.exception))

    def test_transport_error_cannot_disclose_headers_or_cause(self):
        class Client:
            def stream(self, *args, **kwargs):
                raise httpx.LocalProtocolError('invalid Authorization Bearer private-test-token')
        target = pathlib.Path(__file__).parent / 'unused-media-error-test.mp4'
        with patch.object(video_input, '_assert_public_dns', AsyncMock()):
            with self.assertRaises(video_input.VideoInputError) as caught:
                asyncio.run(video_input._download(Client(),
                    'https://image.civitai.com/example.webm', target))
        self.assertNotIn('private-test-token', str(caught.exception))
        self.assertTrue(caught.exception.__suppress_context__)


if __name__ == '__main__':
    unittest.main()
