"""LoRA trigger words: read from descriptions, put in front of the prompt (owner, 2026-09-29)."""
import unittest

import ai_lora_manager as manager
import ai_model_defaults as defaults

PRONE_BONE = (
    "<p><strong>Triggers</strong></p><ul><li><p><em>a man and woman are having sex in the prone bone "
    "position.</em></p></li><li><p><em>viewed from behind</em></p></li><li><p><em>viewed from the front"
    "</em></p></li><li><p><em>viewed from the side</em></p></li><li><p><em>a pov shot</em></p></li><li><p>"
    "<em>viewed from a three quarter angle</em></p></li><li><p>another man is in front of her. he is "
    "spitroasting her</p></li></ul><p></p>")
PRONE_BONE_WORDS = [
    "a man and woman are having sex in the prone bone position.", "viewed from behind",
    "viewed from the front", "viewed from the side", "a pov shot", "viewed from a three quarter angle",
    "another man is in front of her. he is spitroasting her"]
PRONE_ENTRY = {"triggers": [PRONE_BONE_WORDS[0]]}


class DescriptionTriggerTests(unittest.TestCase):
    def test_label_over_a_list_gives_every_item_verbatim(self):
        self.assertEqual(manager.triggers_from_description(PRONE_BONE), PRONE_BONE_WORDS)

    def test_inline_trigger_line(self):
        text = ("<ul><li><p><strong>Training:</strong> 2,000 steps</p></li><li><p><strong>Trigger:</strong> "
                "<code>btssetstyle</code></p></li></ul><p>Include the trigger word in your prompt.</p>")
        self.assertEqual(manager.triggers_from_description(text), ["btssetstyle"])

    def test_code_block_under_the_label_stops_before_prose(self):
        text = ("<p>Trigger word:</p><pre><code>walking from behind</code></pre>"
                "<p>Perhaps increasing the number of sampling steps can lead to improvement</p>")
        self.assertEqual(manager.triggers_from_description(text), ["walking from behind"])

    def test_lines_stop_at_the_blank_line(self):
        text = ("<p><br />Trigger word :<br />Remoteorgasm,<br /><br />Based on a 'remote controlled "
                "sextoy play'<br />But basically able to enhance female reaction to pleasure</p>")
        self.assertEqual(manager.triggers_from_description(text), ["Remoteorgasm"])

    def test_token_with_an_explanation_keeps_the_token(self):
        self.assertEqual(manager.triggers_from_description(
            "<p>Trigger: <strong>SPOONLG.</strong> first word of the prompt.</p>"), ["SPOONLG"])

    def test_prose_and_keyword_lists_give_nothing(self):
        for text in (
            "<p>This LoRA does not need a trigger word.</p>",
            "<h2>Trigger</h2><p>No trigger needed, just describe the motion.</p>",
            "<p>Trigger words: none</p>",
            "<p>The trigger is optional: sks</p>",
            "<h2>Trigger Words &amp; Keywords</h2><p>Incorporate these triggers into your descriptions:</p>"
            "<ul><li><p><strong>Core Triggers:</strong> <code>nude</code>, <code>topless</code></p></li></ul>",
            "<h3>Triggers</h3><p>Always use: 2dan1m and 2D-animated.</p>",
            "<p>Triggers</p><ul><li>sks style</li><li>use it at strength 0.8</li></ul>",
            "<p><strong>Trigger: </strong><em>He/She whisper \"</em>Insert dialogue here.<em>\"</em></p>",
        ):
            self.assertEqual(manager.triggers_from_description(text), [], text)

    def test_comma_list_of_short_tokens_is_split(self):
        self.assertEqual(manager.triggers_from_description("<p>Trigger words: nude, topless, bare breasts</p>"),
                         ["nude", "topless", "bare breasts"])


class TriggerInsertionTests(unittest.TestCase):
    def test_sentence_trigger_is_its_own_sentence(self):
        prompt, added = defaults.apply_triggers("Sura lies on a couch.", [PRONE_ENTRY])
        self.assertEqual(prompt, PRONE_BONE_WORDS[0] + " Sura lies on a couch.")
        self.assertEqual(added, [PRONE_BONE_WORDS[0]])

    def test_prompt_holding_the_sentence_without_its_stop_gets_no_copy(self):
        text = "a man and woman are having sex in the prone bone position, Sura on a couch"
        prompt, added = defaults.apply_triggers(text, [PRONE_ENTRY], ["viewed from behind"])
        self.assertEqual(prompt, "viewed from behind, " + text)
        self.assertEqual(added, ["viewed from behind"])

    def test_extra_phrases_follow_the_automatic_trigger_verbatim(self):
        prompt, added = defaults.apply_triggers("Sura on a couch", [PRONE_ENTRY],
                                                defaults.split_trigger_text("viewed from behind | a pov shot"))
        self.assertEqual(prompt, PRONE_BONE_WORDS[0] + " viewed from behind, a pov shot, Sura on a couch")
        self.assertEqual(added, [PRONE_BONE_WORDS[0], "viewed from behind", "a pov shot"])

    def test_token_triggers_keep_the_former_format(self):
        bounce = {"triggers": ["her breast is bouncing up and down"]}
        self.assertEqual(defaults.add_triggers("Sura, a woman", [None, bounce]),
                         "her breast is bouncing up and down, Sura, a woman")
        self.assertEqual(defaults.add_triggers("sura reclines", [{"triggers": ["btssetstyle"]}]),
                         "btssetstyle, sura reclines")
        self.assertEqual(defaults.add_triggers("sura reclines", [{"triggers": ["sura"]}]), "sura reclines")

    def test_a_phrase_inside_an_added_trigger_is_not_repeated(self):
        prompt, added = defaults.apply_triggers("x", [PRONE_ENTRY], ["prone bone position", "PRONE BONE"])
        self.assertEqual(added, [PRONE_BONE_WORDS[0]])
        self.assertEqual(prompt, PRONE_BONE_WORDS[0] + " x")

    def test_whole_words_only(self):
        self.assertEqual(defaults.add_triggers("Surabaya street", [{"triggers": ["sura"]}]),
                         "sura, Surabaya street")


class CatalogueTriggerTests(unittest.TestCase):
    def test_description_sentences_are_allowed_as_primary(self):
        entry = {"trained_words": ["x" * 80 + "."], "trained_words_source": "description"}
        self.assertEqual(manager.entry_triggers(entry), ["x" * 80 + "."])
        self.assertEqual(manager.entry_triggers(dict(entry, trained_words_source="trainedWords")), [])

    def test_auto_trigger_off_keeps_the_words_but_adds_nothing(self):
        entry = {"trained_words": PRONE_BONE_WORDS, "trained_words_source": "description", "auto_trigger": False}
        self.assertEqual(manager.entry_triggers(entry), [])

    def test_override_moves_the_primary_to_the_head(self):
        entry = {"trained_words": list(PRONE_BONE_WORDS), "trained_words_source": "description"}
        manager.apply_trigger_override(entry, primary="Viewed from behind")
        self.assertEqual(entry["trained_words"][0], "Viewed from behind")
        self.assertEqual(len(entry["trained_words"]), len(PRONE_BONE_WORDS))
        self.assertEqual(entry["trained_words_source"], "admin")
        with self.assertRaises(manager.ResolveError):
            manager.apply_trigger_override(entry, primary="see the description")


if __name__ == "__main__":
    unittest.main()
