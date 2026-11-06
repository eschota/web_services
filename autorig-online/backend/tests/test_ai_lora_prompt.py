"""`<lora:NAME:WEIGHT>` prompt tags and LoRA stacks (ai_lora_prompt)."""

from __future__ import annotations

import sys
import unittest
from pathlib import Path

BACKEND = Path(__file__).resolve().parents[1]
if str(BACKEND) not in sys.path:
    sys.path.insert(0, str(BACKEND))

import ai_lora_prompt as lp  # noqa: E402

LORAS = [
    {"kind": "lora", "file": "add-detail-xl.safetensors", "title": "Detail Tweaker XL",
     "aliases": ["Detail Tweaker XL", "v1.0"], "family": "sdxl"},
    {"kind": "lora", "file": "darth-vader-pxl.safetensors",
     "title": "Masked characters from Star Wars - Darth Vader",
     "aliases": ["Darth Vader"], "family": "pony"},
    {"kind": "lora", "file": "Pixar_Toon.safetensors", "title": "Pixar CGI Toon Style",
     "family": "ltx23"},
    {"kind": "lora", "file": "v1.0.safetensors", "title": "Other", "family": "sdxl"},
]


class ParsePromptTests(unittest.TestCase):
    def test_a_prompt_without_tags_is_returned_untouched(self):
        text = "a  cat,  (red:1.2)\n  second line "
        self.assertEqual(lp.parse_prompt(text), (text, []))

    def test_weights_default_to_one(self):
        clean, refs = lp.parse_prompt("a cat <lora:add-detail-xl>")
        self.assertEqual(clean, "a cat")
        self.assertEqual((refs[0].name, refs[0].strength_model, refs[0].strength_clip),
                         ("add-detail-xl", 1.0, 1.0))

    def test_one_weight_sets_both_strengths(self):
        _, refs = lp.parse_prompt("<lora:x:0.6> a")
        self.assertEqual((refs[0].strength_model, refs[0].strength_clip), (0.6, 0.6))

    def test_two_weights_are_text_encoder_then_model_like_a1111(self):
        _, refs = lp.parse_prompt("<lora:x:0.3:0.9> a")
        self.assertEqual((refs[0].strength_clip, refs[0].strength_model), (0.3, 0.9))

    def test_named_arguments(self):
        _, refs = lp.parse_prompt("<lora:x:unet=0.7:te=0.2:dyn=16> a")
        self.assertEqual((refs[0].strength_model, refs[0].strength_clip), (0.7, 0.2))

    def test_lyco_and_locon_are_aliases(self):
        _, refs = lp.parse_prompt("<lyco:a:0.5> <LoCon:b> <LORA:c:1>")
        self.assertEqual([r.name for r in refs], ["a", "b", "c"])

    def test_negative_weights_are_allowed(self):
        _, refs = lp.parse_prompt("<lora:slider:-1.5> a")
        self.assertEqual(refs[0].strength_model, -1.5)

    def test_bad_weights_are_errors_not_silence(self):
        for text in ("<lora:x:abc>", "<lora:x:9>", "<lora::1>", "<lora:x:foo=1>"):
            with self.subTest(text=text), self.assertRaises(lp.LoraSyntaxError):
                lp.parse_prompt(text)

    def test_removed_tags_take_their_separator_with_them(self):
        base = "score_9, darth vader, cinematic lighting, photorealistic"
        for tagged in (
            "<lora:a:0.8>, <lora:b:0.6>, " + base,
            base + ", <lora:a:0.8>, <lora:b:0.6>",
            base + " <lora:a:0.8> <lora:b:0.6>",
            "<lora:a:0.8> " + base.replace("darth vader, ", "darth vader, <lora:b:0.6>, "),
            "score_9, <lora:a:0.8>, darth vader, cinematic lighting,<lora:b:0.6>, photorealistic",
        ):
            with self.subTest(tagged=tagged):
                clean, refs = lp.parse_prompt(tagged)
                self.assertEqual(clean, base)
                self.assertEqual(len(refs), 2)

    def test_a_line_of_only_tags_disappears(self):
        clean, _ = lp.parse_prompt("a cat\n<lora:a> <lora:b:0.5>\nb dog")
        self.assertEqual(clean, "a cat\nb dog")

    def test_lines_without_tags_keep_their_spacing(self):
        clean, _ = lp.parse_prompt("  indented line\na <lora:x>")
        self.assertEqual(clean, "indented line\na")


class ParseStackTests(unittest.TestCase):
    def test_tag_string(self):
        refs = lp.parse_stack("<lora:a:0.8> <lora:b:0.5:1>")
        self.assertEqual([(r.name, r.strength_model, r.strength_clip, r.source) for r in refs],
                         [("a", 0.8, 0.8, "stack"), ("b", 1.0, 0.5, "stack")])

    def test_list_of_objects(self):
        refs = lp.parse_stack([{"name": "a", "strength": 0.7},
                               {"file": "b.safetensors", "strength_model": 1, "strength_clip": 0.2},
                               "<lora:c:0.1>"])
        self.assertEqual([(r.name, r.strength_model, r.strength_clip) for r in refs],
                         [("a", 0.7, 0.7), ("b.safetensors", 1.0, 0.2), ("c", 0.1, 0.1)])

    def test_empty_values(self):
        self.assertEqual(lp.parse_stack(None), [])
        self.assertEqual(lp.parse_stack(""), [])
        self.assertEqual(lp.parse_stack([]), [])

    def test_text_that_is_not_a_tag_is_refused(self):
        with self.assertRaises(lp.LoraSyntaxError):
            lp.parse_stack("add-detail-xl 0.8")

    def test_to_tags_round_trips(self):
        stack = [{"name": "a.safetensors", "strength_model": 0.8, "strength_clip": 0.8},
                 {"name": "b.safetensors", "strength_model": 1.0, "strength_clip": 0.5}]
        refs = lp.parse_stack(lp.to_tags(stack))
        self.assertEqual([(r.name, r.strength_model, r.strength_clip) for r in refs],
                         [("a", 0.8, 0.8), ("b", 1.0, 0.5)])


class ResolveTests(unittest.TestCase):
    def test_by_file_stem_title_and_alias_case_insensitively(self):
        for name in ("add-detail-xl", "ADD-DETAIL-XL.safetensors", "detail tweaker xl"):
            self.assertEqual(lp.resolve_name(name, LORAS)["file"], "add-detail-xl.safetensors")
        self.assertEqual(lp.resolve_name("darth vader", LORAS)["file"],
                         "darth-vader-pxl.safetensors")

    def test_an_exact_file_name_beats_an_alias(self):
        # "v1.0" is an alias of one LoRA and the stem of another's file.
        self.assertEqual(lp.resolve_name("v1.0.safetensors", LORAS)["file"], "v1.0.safetensors")

    def test_an_ambiguous_name_is_an_error(self):
        with self.assertRaises(lp.LoraResolutionError):
            lp.resolve_name("v1.0", LORAS)

    def test_an_unknown_name_says_so_and_suggests(self):
        with self.assertRaises(lp.LoraResolutionError) as ctx:
            lp.resolve_name("add-detail", LORAS)
        self.assertIn("add-detail-xl", ctx.exception.suggestions)
        self.assertIn("/lora", str(ctx.exception))


class BuildStackTests(unittest.TestCase):
    def test_stack_order_is_node_stack_then_new_prompt_tags(self):
        stack, override = lp.build_stack(
            stack_refs=lp.parse_stack("<lora:darth-vader-pxl:0.8>"),
            prompt_refs=lp.parse_prompt("x <lora:add-detail-xl:0.6>")[1], loras=LORAS)
        self.assertIsNone(override)
        self.assertEqual([(s.file, s.strength_model) for s in stack],
                         [("darth-vader-pxl.safetensors", 0.8), ("add-detail-xl.safetensors", 0.6)])

    def test_a_prompt_tag_overrides_the_stack_strength_in_place(self):
        stack, _ = lp.build_stack(
            stack_refs=lp.parse_stack("<lora:darth-vader-pxl:0.8> <lora:add-detail-xl:1>"),
            prompt_refs=lp.parse_prompt("<lora:darth-vader-pxl:0.3:0.5> x")[1], loras=LORAS)
        self.assertEqual([(s.file, s.strength_model, s.strength_clip) for s in stack],
                         [("darth-vader-pxl.safetensors", 0.5, 0.3),
                          ("add-detail-xl.safetensors", 1.0, 1.0)])

    def test_a_tag_naming_the_single_lora_only_restates_its_strength(self):
        stack, override = lp.build_stack(
            stack_refs=[], prompt_refs=lp.parse_prompt("<lora:add-detail-xl:0.4> x")[1],
            loras=LORAS, single_lora="add-detail-xl.safetensors")
        self.assertEqual(stack, [])
        self.assertEqual(override, 0.4)

    def test_zero_strength_entries_are_validated_then_dropped(self):
        stack, _ = lp.build_stack(stack_refs=lp.parse_stack("<lora:add-detail-xl:0>"),
                                  prompt_refs=[], loras=LORAS)
        self.assertEqual(stack, [])
        with self.assertRaises(lp.LoraResolutionError):
            lp.build_stack(stack_refs=lp.parse_stack("<lora:nope:0>"), prompt_refs=[], loras=LORAS)

    def test_node_params_and_prompt_tags_give_the_same_stack(self):
        from_params, _ = lp.build_stack(
            stack_refs=lp.parse_stack([{"name": "darth-vader-pxl", "strength": 0.8},
                                       {"name": "add-detail-xl", "strength": 0.6}]),
            prompt_refs=[], loras=LORAS)
        clean, refs = lp.parse_prompt("a <lora:darth-vader-pxl:0.8> <lora:add-detail-xl:0.6>")
        from_prompt, _ = lp.build_stack(stack_refs=[], prompt_refs=refs, loras=LORAS)
        self.assertEqual(clean, "a")
        self.assertEqual([s.as_payload() for s in from_params], [s.as_payload() for s in from_prompt])

    def test_too_many_loras(self):
        many = [{"kind": "lora", "file": f"l{i}.safetensors"} for i in range(10)]
        with self.assertRaises(lp.LoraSyntaxError):
            lp.build_stack(stack_refs=lp.parse_stack(" ".join(f"<lora:l{i}>" for i in range(10))),
                           prompt_refs=[], loras=many)


if __name__ == "__main__":
    unittest.main()
