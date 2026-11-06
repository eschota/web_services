import unittest
from unittest import mock

from renderfin import model_eligibility
from renderfin.models import RenderPrompt, RenderServer


class Response:
    def __init__(self, payload): self.payload = payload
    def raise_for_status(self): return None
    def json(self): return self.payload


class Client:
    def __init__(self, files): self.files = files
    async def get(self, url, **kwargs):
        kind = url.rsplit("/", 1)[-1]
        slot = "unet_name" if kind == "UNETLoader" else (
            "ckpt_name" if kind == "CheckpointLoaderSimple" else "lora_name")
        return Response({kind: {"input": {"required": {slot: [self.files.get(kind, [])]}}}})


class EligibilityTests(unittest.IsolatedAsyncioTestCase):
    def test_combo_schema_options_are_read(self):
        payload = {"UNETLoader": {"input": {"required": {
            "unet_name": ["COMBO", {"options": ["flux2.safetensors"]}]
        }}}}
        self.assertEqual(model_eligibility._names(
            payload, "UNETLoader", "unet_name"), {"flux2.safetensors"})

    async def asyncSetUp(self):
        model_eligibility._cache.clear()
        self.server = RenderServer(render_server_name="w", render_server_url="http://127.0.0.1:8188")

    async def test_selected_files_must_both_exist(self):
        client = Client({"CheckpointLoaderSimple": ["pony.safetensors"],
                         "LoraLoader": ["style.safetensors"], "UNETLoader": [],
                         "LoraLoaderModelOnly": []})
        prompt = RenderPrompt(checkpoint="pony.safetensors", lora="style.safetensors")
        self.assertTrue(await model_eligibility.can_load(client, self.server, prompt))
        prompt.lora = "missing.safetensors"
        self.assertFalse(await model_eligibility.can_load(client, self.server, prompt))

    async def test_unet_checkpoint_inventory_is_supported(self):
        client = Client({"CheckpointLoaderSimple": [], "UNETLoader": ["flux2.safetensors"],
                         "LoraLoader": [], "LoraLoaderModelOnly": []})
        self.assertTrue(await model_eligibility.can_load(
            client, self.server, RenderPrompt(checkpoint="flux2.safetensors")))

    async def test_no_selection_needs_no_probe(self):
        self.assertTrue(await model_eligibility.can_load(Client({}), self.server, RenderPrompt()))

    async def test_inventory_failure_is_not_treated_as_eligible(self):
        client = mock.AsyncMock()
        client.get.side_effect = ValueError("bad inventory")
        self.assertFalse(await model_eligibility.can_load(
            client, self.server, RenderPrompt(checkpoint="x.safetensors")))


if __name__ == "__main__": unittest.main()
