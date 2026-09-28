"""Music (Stable Audio 3) routing and workflow build (renderfin.music)."""
from pathlib import Path

from renderfin import music, routing, templating
from renderfin.models import RenderPrompt
from renderfin.runtime_settings import apply_runtime_settings

WORKFLOWS = Path(__file__).resolve().parents[1] / "renderfin" / "assets" / "workflows"


def _build(prompt):
    workflow_file, _ = routing.resolve_workflow_file(prompt)
    workflow = templating.render_workflow_text(
        (WORKFLOWS / workflow_file).read_text(encoding="utf-8"),
        width=960, height=540, prompt=prompt.prompt, negative_prompt=prompt.negative_prompt,
        image_filename="", output_prefix="task", workflow_type=prompt.type,
        seed=prompt.noise_seed or None, checkpoint=prompt.checkpoint)
    apply_runtime_settings(workflow, prompt, 960, 540)
    music.apply_music_settings(workflow, prompt)
    return workflow


def test_music_routes_to_its_own_token_and_mp3():
    prompt = RenderPrompt(prompt="lofi", type="music_sa3")
    assert routing.scheduling_token(prompt) == "gen_music_sa3.json"
    assert routing.resolve_workflow_file(prompt) == ("gen_music_sa3.json", None)
    assert routing.output_extension(prompt) == ".mp3"
    small = RenderPrompt(prompt="lofi", type="music_sa3_small")
    assert routing.scheduling_token(small) == "gen_music_sa3_small.json"


def test_music_workflow_carries_length_steps_seed_and_prompt():
    prompt = RenderPrompt(prompt='calm "lofi"', negative_prompt="vocals", type="music_sa3",
                          audio_seconds=12.5, steps=4, noise_seed=77,
                          checkpoint="stable_audio_3_medium.safetensors")
    workflow = _build(prompt)
    by_class = {node["class_type"]: node["inputs"] for node in workflow.values()}
    # Short requests render 20 s and are cut to size (renderfin.music).
    assert by_class["EmptyLatentAudio"]["seconds"] == 20.0
    assert by_class["TrimAudioDuration"]["duration"] == 12.5
    save = next(n for n in workflow.values() if n["class_type"] == "SaveAudioMP3")
    assert workflow[save["inputs"]["audio"][0]]["class_type"] == "TrimAudioDuration"
    assert by_class["KSampler"]["steps"] == 4
    assert by_class["KSampler"]["seed"] == 77
    assert by_class["CheckpointLoaderSimple"]["ckpt_name"] == "stable_audio_3_medium.safetensors"
    assert by_class["SaveAudioMP3"]["filename_prefix"] == "task"
    texts = sorted(node["inputs"]["text"] for node in workflow.values()
                   if node["class_type"] == "CLIPTextEncode")
    assert texts == ['calm "lofi"', "vocals"]


def test_music_length_is_clamped_and_defaults_to_30():
    assert music.clamp_seconds(0) == 30.0
    assert music.clamp_seconds(9999) == music.MAX_SECONDS
    assert music.clamp_seconds(0.2) == 1.0


def test_long_music_is_rendered_at_its_own_length_without_a_cut():
    prompt = RenderPrompt(prompt="x", type="music_sa3", audio_seconds=45)
    workflow = _build(prompt)
    classes = [node["class_type"] for node in workflow.values()]
    assert "TrimAudioDuration" not in classes
    assert next(n for n in workflow.values()
                if n["class_type"] == "EmptyLatentAudio")["inputs"]["seconds"] == 45.0
