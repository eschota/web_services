"""One typed catalogue for the AI services under autorig.online.

Every service declares what it accepts and what it produces in the same small
vocabulary of entity types, so an answer from one service can be handed to any
service that accepts that type without either side knowing about the other.

An entity is `{type, value, mime?, label?, origin?}`. `value` is text for
`text`, and an http(s) URL for everything else, which is what makes a handoff
free: nothing is copied, the next service is simply given the address.

The catalogue is the single source of truth for the pages, so a service that
is not wired yet says so here rather than being quietly missing from the UI.
"""

from __future__ import annotations

import time
from typing import Dict, List, Optional

from fastapi import APIRouter

router = APIRouter()

# ---------------------------------------------------------------- entity types

TEXT = "text"
IMAGE = "image"
VIDEO = "video"
MEDIA = "media"
AVATAR = "avatar"
MODEL3D = "model3d"
AUDIO = "audio"
CONTROL_POSE = "control_pose"
CONTROL_DEPTH = "control_depth"
CONTROL_CANNY = "control_canny"
CONTROL_NORMAL = "control_normal"

ENTITY_TYPES: List[Dict[str, object]] = [
    {"id": AVATAR, "title": "Avatar", "carries": "saved private Avatar id and immutable version", "icon": "👤"},
    {"id": CONTROL_POSE, "title": "Pose control", "carries": "validated OpenPose map URL", "icon": "🧍"},
    {"id": CONTROL_DEPTH, "title": "Depth control", "carries": "validated depth map URL", "icon": "▧"},
    {"id": CONTROL_NORMAL, "title": "Normal control", "carries": "normal map URL (RGB surface orientation)", "icon": "◩"},
    {"id": CONTROL_CANNY, "title": "Canny control", "carries": "validated edge map URL", "icon": "▱"},
    {
        "id": TEXT,
        "title": "Text",
        "carries": "the string itself",
        "icon": "📝",
    },
    {
        # The one input node for pictures and clips (2026-09-26): any link,
        # upload or paste. A picture socket gets a clip's first frame.
        "id": MEDIA,
        "title": "Media",
        "carries": "any image or video: link (Civitai pages too), upload or paste",
        "icon": "🎞️",
    },
    {
        "id": IMAGE,
        "title": "Image",
        "carries": "a public URL to a PNG, JPEG or WebP",
        "icon": "🖼️",
    },
    {
        "id": VIDEO,
        "title": "Video",
        "carries": "a public URL to an MP4",
        "icon": "🎬",
    },
    {
        "id": MODEL3D,
        "title": "3D model",
        "carries": "a public URL to a GLB or FBX",
        "icon": "🧊",
    },
    {
        "id": AUDIO,
        "title": "Audio",
        "carries": "a public URL to an MP3",
        "icon": "🎵",
    },
]

# ------------------------------------------------------------- system prompts

# The standing instruction a Vision or Text node carries unless somebody edits
# it on that node. It exists because the commonest use of these two services in
# a graph is to write a prompt for an image model, and a language model that is
# not told so answers with its reasoning, a preamble, or both — which then
# lands in the image prompt verbatim.
#
# The node keeps it in `params._system_prompt`, and it is never part of the
# node's output: what flows on to the next node is the answer alone.
SYSTEM_PROMPT_DEFAULT = (
    "Always write only the output_text answer as plain text. "
    "Always in English. "
    "This prompt is for an AI image generation model."
)

# --------------------------------------------------------------------- services

# `status` is either "live" (wired end to end) or "planned" (declared so the
# handoff UI can show where a result could go, but not yet callable). A page
# shows a planned service greyed out instead of pretending it works.
def MULTIREF_INPUTS(total: int) -> List[Dict[str, object]]:
    """Reference sockets 2..total for a node whose `image` socket is picture 1.

    Each takes a picture or a video; a video stands for its first frame. The
    runner sends them as `reference_image_urls` in socket order, which is the
    order the prompt counts them in ("image 2", "image 3").
    """
    return [{"type": IMAGE, "field": f"reference_{index}", "required": False,
             "title": f"Image {index}", "ref_index": index, "also_accepts": [VIDEO]}
            for index in range(2, total + 1)]


SERVICES: List[Dict[str, object]] = [
    {
        # Extract Frames (2026-09-27) replaces "Video first frame" under the same
        # id, so saved graphs keep working: the first picture is still on
        # image_url_string. Scenes are found with Scene split's detector (its own
        # job; the Scene split pipeline is untouched).
        "id": "video_frame", "title": "Extract Frames", "path": "/nodes",
        "api": "/api/ai/video-tools/extract-frames", "status": "live", "list_source": True,
        "summary": ("Pictures from a video by template (Start-End per scene by default): a gallery / list for "
                    "per-item pipelines, plus the first picture for simple wiring."),
        "inputs": [{"type": VIDEO, "field": "video_url", "required": True, "title": "Source video"}],
        # One plain picture socket per extracted frame (primary, 2026-09-27):
        # a node wired to frame k gets exactly that picture and runs once.
        # Labels ("1 · S1 start") and visibility follow the result / probe.
        "outputs": [
            {"type": IMAGE, "field": "frame_1_url_string", "title": "Frame 1", "frame_socket": 1},
            {"type": IMAGE, "field": "frame_2_url_string", "title": "Frame 2", "frame_socket": 2},
            {"type": IMAGE, "field": "frame_3_url_string", "title": "Frame 3", "frame_socket": 3},
            {"type": IMAGE, "field": "frame_4_url_string", "title": "Frame 4", "frame_socket": 4},
            {"type": IMAGE, "field": "frame_5_url_string", "title": "Frame 5", "frame_socket": 5},
            {"type": IMAGE, "field": "frame_6_url_string", "title": "Frame 6", "frame_socket": 6},
            {"type": IMAGE, "field": "frame_7_url_string", "title": "Frame 7", "frame_socket": 7},
            {"type": IMAGE, "field": "frame_8_url_string", "title": "Frame 8", "frame_socket": 8},
            {"type": IMAGE, "field": "frame_9_url_string", "title": "Frame 9", "frame_socket": 9},
            {"type": IMAGE, "field": "frame_10_url_string", "title": "Frame 10", "frame_socket": 10},
            {"type": IMAGE, "field": "frame_11_url_string", "title": "Frame 11", "frame_socket": 11},
            {"type": IMAGE, "field": "frame_12_url_string", "title": "Frame 12", "frame_socket": 12},
            {"type": IMAGE, "field": "image_url_string", "title": "First picture"},
            {"type": IMAGE, "field": "frame_url_string", "title": "Pictures (each) · fan-out list", "per_item": True},
            {"type": TEXT, "field": "frame_info_string", "title": "Picture info (each)", "per_item": True},
            {"type": TEXT, "field": "frames_text_string", "title": "Pictures (text)"},
        ],
    },
    {
        # Scene-aware since 2026-09-27 (owner): adaptive cut detection on the
        # per-frame change score, then the MIDDLE frame of every scene, numbered
        # with timecodes, plus the scene list as text for Vision.
        "id": "video_storyboard", "title": "Video storyboard", "path": "/nodes",
        "api": "/api/ai/video-tools/scene-split", "status": "live",
        "summary": "Scenes found by cut detection: the middle frame of each scene with its number and timecodes.",
        "inputs": [{"type": VIDEO, "field": "video_url", "required": True, "title": "Source video"}],
        "outputs": [{"type": IMAGE, "field": "image_url_string", "title": "Storyboard (scene middles)"},
                    {"type": TEXT, "field": "scenes_text_string", "title": "Scenes (text)"}],
    },
    {
        # Loop head (2026-09-27): every node wired after a per-shot output runs
        # once per shot in the editor; Concat collects the list again.
        "id": "scene_split", "title": "Scene split · for each shot", "path": "/nodes",
        "api": "/api/ai/video-tools/scene-split", "status": "live", "list_source": True,
        "summary": ("Cuts a video into shots (adaptive cut detection, long shots split to the frame limit). "
                    "Nodes wired after an (each) output run once per shot."),
        "inputs": [{"type": VIDEO, "field": "video_url", "required": True, "title": "Source video"}],
        "outputs": [
            {"type": VIDEO, "field": "shot_clip_url_string", "title": "Shot clip (each)", "per_item": True},
            {"type": IMAGE, "field": "first_frame_url_string", "title": "First frame (each)", "per_item": True},
            {"type": IMAGE, "field": "middle_frame_url_string", "title": "Middle frame (each)", "per_item": True},
            {"type": TEXT, "field": "shot_info_string", "title": "Shot info (each)", "per_item": True},
            {"type": IMAGE, "field": "last_frame_url_string", "title": "Last frame (each, = next segment's first)",
             "per_item": True},
            {"type": IMAGE, "field": "storyboard_url_string", "title": "Storyboard (scene middles)"},
            {"type": TEXT, "field": "scenes_text_string", "title": "Scenes (text)"},
            {"type": TEXT, "field": "shots_json_string", "title": "Shot list (for Concat)"},
            {"type": VIDEO, "field": "source_video_url_string", "title": "Reference 24 fps + audio"},
        ],
    },
    {
        "id": "video_concat", "title": "Concat shots", "path": "/nodes",
        "api": "/api/ai/video-tools/concat", "status": "live", "list_sink": True,
        "summary": ("Joins per-shot clips in shot order: per shot the first clip that arrived (Clips, then 2, then 3), "
                    "trimmed to the shot length, one size and frame rate."),
        "inputs": [
            {"type": VIDEO, "field": "clip", "required": True, "title": "Clips (each shot)"},
            {"type": VIDEO, "field": "clip_2", "required": False, "title": "Clips 2 (if 1 missing)"},
            {"type": VIDEO, "field": "clip_3", "required": False, "title": "Clips 3 (if 1, 2 missing)"},
            {"type": TEXT, "field": "shots_json", "required": False, "title": "Shot list (from Scene split)"},
        ],
        "outputs": [{"type": VIDEO, "field": "video_url_string", "title": "Joined video"}],
    },
    {
        # Keyframe chain (2026-09-27): segments share their boundary frame;
        # Summary fits each to its length, drops the duplicate boundary frame
        # and joins them seamlessly, with the original audio when wired.
        "id": "video_summary", "title": "Summary · seamless join", "path": "/nodes",
        "api": "/api/ai/video-tools/concat", "status": "live", "list_sink": True,
        "summary": ("Joins keyframe-chain segments into one seamless clip: each fitted to its length (resample keeps "
                    "both keyframes), the shared boundary frame kept once, original audio under it."),
        "inputs": [
            {"type": VIDEO, "field": "clip", "required": True, "title": "Segments (each)"},
            {"type": VIDEO, "field": "clip_2", "required": False, "title": "Segments 2 (if 1 missing)"},
            {"type": TEXT, "field": "shots_json", "required": True, "title": "Shot list (from Scene split)"},
            {"type": VIDEO, "field": "source_url", "required": False, "title": "Audio from (original)"},
        ],
        "outputs": [{"type": VIDEO, "field": "video_url_string", "title": "Seamless video"}],
    },
    {
        "id": "audio_from_source", "title": "Audio from source", "path": "/nodes",
        "api": "/api/ai/video-tools/audio-mux", "status": "live",
        "summary": "Puts the original video's audio under the new picture.",
        "inputs": [
            {"type": VIDEO, "field": "video_url", "required": True, "title": "New picture"},
            {"type": VIDEO, "field": "source_url", "required": False, "title": "Audio from (original)"},
        ],
        "outputs": [{"type": VIDEO, "field": "video_url_string", "title": "Video with audio"}],
    },
    {
        "id": "video_control", "title": "Video motion transfer", "path": "/nodes",
        "api": "/api/video", "status": "live", "slow": True,
        "summary": "Animate a character keyframe using motion from the source video.",
        "inputs": [
            {"type": IMAGE, "field": "image", "required": True, "title": "Character keyframe"},
            {"type": VIDEO, "field": "control_video_url", "required": True, "title": "Driving video"},
            {"type": TEXT, "field": "prompt", "required": False, "title": "Scene and action"},
        ],
        "outputs": [{"type": VIDEO, "field": "video_url_string", "title": "Clip"}],
    },
    {
        "id": "avatar_image", "title": "Avatar scene", "path": "/avatars",
        "api": "/api/ai/avatar-image", "status": "live",
        "summary": "Place one or two saved characters into a scene using their reference images.",
        "inputs": [
            {"type": AVATAR, "field": "avatar", "required": True, "title": "Main character"},
            {"type": AVATAR, "field": "avatar_secondary", "required": False, "title": "Second character"},
            {"type": IMAGE, "field": "image", "required": False, "title": "Scene reference"},
            {"type": TEXT, "field": "prompt", "required": True, "title": "Scene and action"},
        ],
        "outputs": [{"type": IMAGE, "field": "image_url_string", "title": "Character keyframe"}],
    },
    {
        # One picture or one video in, a saved Avatar v2 out, with every view
        # it made on its own socket so each can be wired on (ai_avatar_build).
        "id": "avatar_build", "title": "Avatar builder", "path": "/avatars",
        "api": "/api/ai/avatar-build", "status": "live", "slow": True,
        "multi_output": True,
        "summary": ("A picture or a video in; a saved Avatar with front, face, full body, "
                    "three-quarter, profile and back views out, each checked against the source."),
        "inputs": [
            {"type": IMAGE, "field": "image", "required": True,
             "title": "Photo or video", "also_accepts": [VIDEO]},
            {"type": AVATAR, "field": "avatar", "required": False,
             "title": "Add a version to"},
        ],
        "outputs": [
            {"type": AVATAR, "field": "avatar_string", "title": "Avatar"},
            {"type": IMAGE, "field": "front_url_string", "title": "Front", "view": "front"},
            {"type": IMAGE, "field": "face_closeup_url_string", "title": "Face", "view": "face_closeup"},
            {"type": IMAGE, "field": "full_body_url_string", "title": "Full body", "view": "full_body"},
            {"type": IMAGE, "field": "three_quarter_left_url_string", "title": "3/4 left",
             "view": "three_quarter_left"},
            {"type": IMAGE, "field": "three_quarter_right_url_string", "title": "3/4 right",
             "view": "three_quarter_right"},
            {"type": IMAGE, "field": "profile_left_url_string", "title": "Profile left",
             "view": "profile_left"},
            {"type": IMAGE, "field": "profile_right_url_string", "title": "Profile right",
             "view": "profile_right"},
            {"type": IMAGE, "field": "back_url_string", "title": "Back", "view": "back"},
            {"type": IMAGE, "field": "sheet_url_string", "title": "Sheet"},
            {"type": IMAGE, "field": "source_frame_url_string", "title": "Source frame"},
            {"type": TEXT, "field": "description_string", "title": "Description"},
        ],
    },
    {
        # A saved Avatar on the builder's sockets (2026-09-27); with no Avatar
        # wired it builds one from the picture/video (whichever is connected).
        "id": "avatar_ready", "title": "Avatar (ready or build)", "path": "/nodes",
        "api": "/api/ai/avatar-ready", "status": "live", "multi_output": True,
        "summary": ("A saved Avatar (av_…@version, yours) out on the same sockets as the Avatar builder; "
                    "with no Avatar wired, builds one from the picture or video."),
        "inputs": [
            {"type": AVATAR, "field": "avatar", "required": False, "title": "Ready Avatar (av_…@N)"},
            {"type": IMAGE, "field": "image", "required": False, "title": "…or build from photo/video",
             "also_accepts": [VIDEO]},
        ],
        "outputs": [
            {"type": AVATAR, "field": "avatar_string", "title": "Avatar"},
            {"type": IMAGE, "field": "front_url_string", "title": "Front", "view": "front"},
            {"type": IMAGE, "field": "face_closeup_url_string", "title": "Face", "view": "face_closeup"},
            {"type": IMAGE, "field": "full_body_url_string", "title": "Full body", "view": "full_body"},
            {"type": IMAGE, "field": "three_quarter_left_url_string", "title": "3/4 left", "view": "three_quarter_left"},
            {"type": IMAGE, "field": "three_quarter_right_url_string", "title": "3/4 right", "view": "three_quarter_right"},
            {"type": IMAGE, "field": "profile_left_url_string", "title": "Profile left", "view": "profile_left"},
            {"type": IMAGE, "field": "profile_right_url_string", "title": "Profile right", "view": "profile_right"},
            {"type": IMAGE, "field": "back_url_string", "title": "Back", "view": "back"},
            {"type": IMAGE, "field": "sheet_url_string", "title": "Sheet"},
            {"type": IMAGE, "field": "source_frame_url_string", "title": "Source frame"},
            {"type": TEXT, "field": "description_string", "title": "Description"},
        ],
    },
    {
        # Wan-Animate-2 from a plain picture, no saved Avatar (2026-09-27).
        "id": "wan_image", "title": "Wan-Animate-2 · character picture", "path": "/nodes",
        "api": "/api/ai/wan-animate", "status": "live", "slow": True,
        "summary": "Animate a character picture (e.g. a Qwen keyframe) with the motion of a driving video (worker-4090).",
        "inputs": [
            {"type": IMAGE, "field": "image", "required": True, "title": "Character picture / keyframe"},
            {"type": VIDEO, "field": "control_video_url", "required": True, "title": "Driving video"},
            {"type": TEXT, "field": "prompt", "required": False, "title": "Motion verbs"},
            {"type": TEXT, "field": "character", "required": False, "title": "Character description"},
        ],
        "outputs": [{"type": VIDEO, "field": "video_url_string", "title": "Clip"}],
    },
    {
        "id": "avatar_video", "title": "Avatar video · Wan-Animate-2", "path": "/nodes",
        "api": "/api/ai/avatar-video", "status": "live", "slow": True,
        "summary": "Transfer action from a driving video to one or two saved Avatar characters.",
        "inputs": [
            {"type": AVATAR, "field": "avatar", "required": True, "title": "Main character"},
            {"type": AVATAR, "field": "avatar_secondary", "required": False, "title": "Second character"},
            {"type": VIDEO, "field": "control_video_url", "required": True, "title": "Driving video"},
            {"type": IMAGE, "field": "image", "required": False, "title": "Character keyframe"},
            {"type": TEXT, "field": "prompt", "required": False, "title": "Motion verbs"},
        ],
        "outputs": [{"type": VIDEO, "field": "video_url_string", "title": "Avatar clip"}],
    },
    {
        "id": "vision",
        "title": "Vision",
        "path": "/vision",
        "api": "/api/vision",
        "summary": "Ask a question about an image or a video and get an answer in text.",
        "status": "live",
        "system_prompt_capable": True,
        "system_prompt_default": SYSTEM_PROMPT_DEFAULT,
        "inputs": [
            # An image stays the declared requirement: it is what /vision is,
            # and the plain page starts on its own when one arrives. A video
            # substitutes for it — the service turns the clip into a picture
            # first — so it is offered as the alternative, not as a second
            # thing the node also needs.
            # One socket for the thing to look at. A clip wired here is sent
            # as the clip it is; the service turns it into a picture itself.
            {"type": IMAGE, "field": "image", "required": True,
             "title": "Photo or video", "also_accepts": [VIDEO]},
            # The separate video socket it replaces. Kept so saved graphs that
            # wire it by name keep working, and hidden until one does.
            {"type": VIDEO, "field": "video_url", "required": False,
             "title": "Video (older graphs)", "hide_when_empty": True},
            # Not required as a wire: the question is usually a fixed sentence,
            # and needing a whole node to hold it was the commonest way to end
            # up submitting a request with no prompt at all.
            {"type": TEXT, "field": "prompt", "required": False,
             "title": "What to ask about it"},
            # Facts to read the picture with (e.g. Scene split's scene list);
            # the editor appends it to the question.
            {"type": TEXT, "field": "context", "required": False,
             "title": "Context (added to the question)"},
        ],
        "outputs": [
            {"type": TEXT, "field": "answer_string", "title": "Answer"},
        ],
        "models_url": "/api/ai/models",
    },
    {
        "id": "text",
        "title": "Text",
        "path": "/text",
        "api": "/api/text2text",
        "summary": "Send a prompt to a language model and get text back.",
        "status": "live",
        "system_prompt_capable": True,
        "system_prompt_default": SYSTEM_PROMPT_DEFAULT,
        "inputs": [
            {"type": TEXT, "field": "prompt", "required": False,
             "title": "Instruction"},
            # The material to work on, kept apart from the instruction so a
            # text coming from another service can be processed rather than
            # merely passed to the model as the whole request.
            {"type": TEXT, "field": "input", "required": False,
             "title": "Text to work on"},
        ],
        "outputs": [
            {"type": TEXT, "field": "answer_string", "title": "Answer"},
        ],
        "models_url": "/api/ai/models",
    },
    {
        "id": "image",
        # Used only when the node's text is empty (owner, 2026-09-27); {images}
        # = "image 1, image 2, ...". Editable per node like Vision's.
        "system_prompt_capable": True,
        "system_prompt_default": ("Remix {images} into one coherent image: unify the style and lighting, "
                                  "combine the subjects and the story of all inputs; image 1 is the base "
                                  "scene and composition."),
        "title": "Image",
        "path": "/image",
        "api": "/api/image",
        "summary": "Generate a picture from a prompt, optionally guided by a reference image.",
        "status": "live",
        "inputs": [
            # Not required as a wire: the prompt can be typed on the node, the
            # same way vision and text take theirs. Required here meant a node
            # given only a reference picture could not run at all.
            {"type": TEXT, "field": "prompt", "required": False,
             "title": "What to draw"},
            {"type": IMAGE, "field": "image", "required": False,
             "title": "Reference image", "ref_index": 1, "also_accepts": [VIDEO]},
            {"type": CONTROL_POSE, "field": "control_pose", "required": False, "title": "Pose control"},
            {"type": CONTROL_DEPTH, "field": "control_depth", "required": False, "title": "Depth control"},
            {"type": CONTROL_CANNY, "field": "control_canny", "required": False, "title": "Canny control"},
            # More pictures composed into one. Since 2026-09-26 a picture edit
            # (FLUX.2 klein + picture) and any multi-picture request run on the
            # one edit model, Qwen-Image 2.1 turbo: 3 pictures in all. Kept
            # after every older socket so saved graphs keep their wiring.
            *MULTIREF_INPUTS(3),
        ],
        "outputs": [
            {"type": IMAGE, "field": "image_url_string", "title": "Picture"},
        ],
    },
    {
        "id": "video",
        "system_prompt_capable": True,
        "system_prompt_default": ("Animate the picture naturally: subtle, realistic motion that fits the "
                                  "scene; keep the subject, style and framing."),
        "title": "Video",
        "path": "/video",
        "api": "/api/video",
        "summary": "Animate a picture into a short clip. Minutes, not seconds.",
        # The farm's workers advertise the animation workflow, and Renderfin
        # picks it from the frame alone, so a picture is all this needs.
        "status": "live",
        "slow": True,
        "inputs": [
            {"type": IMAGE, "field": "image", "required": True,
             "title": "First frame"},
            # Optional, and the reason a loop is possible at all: give the last
            # frame and the clip travels to it; give the first frame again and
            # it comes back to where it started.
            {"type": IMAGE, "field": "image_url_end", "required": False,
             "title": "Last frame"},
            {"type": TEXT, "field": "prompt", "required": False,
             "title": "What should happen"},
        ],
        "outputs": [
            {"type": VIDEO, "field": "video_url_string", "title": "Clip"},
        ],
    },
    {
        "id": "3dmodel",
        "title": "3D model",
        "path": "/3dmodel",
        "api": "/api/3dmodel",
        "summary": "Turn a picture into a 3D model. Hunyuan3D on the farm's own GPUs.",
        # Runs on the converter nodes' own Hunyuan3D, which is installed and
        # idle there; `/api/generate/from-image` is the separate account-and-
        # credits product flow and is not what this uses.
        "status": "live",
        "slow": True,
        "inputs": [
            {"type": IMAGE, "field": "image", "required": True,
             "title": "Picture of the subject"},
        ],
        "outputs": [
            {"type": MODEL3D, "field": "model_url_string", "title": "3D model"},
            {"type": IMAGE, "field": "preview_url_string", "title": "Preview"},
        ],
    },
]


# ------------------------------------------------------------------ parameters

# What a caller may tune beyond the typed inputs. These are declared here, in
# the same catalogue, so a graph editor can draw the controls for a service it
# has never heard of; every one of them is optional and maps onto a field the
# API already accepts.
#
# Only knobs implemented by the selected model's workflow are presented.
# The checkpoint determines the video architecture and its trained schedule.
# Every map is also a plain PNG at the source size (pose: coloured skeleton on
# black, depth: grey, near = white, canny: white edges on black, normal: RGB),
# so it wires into any image socket as a reference picture too (ai_graph
# type check, ai-nodes.js typeFits); the dedicated control sockets use it
# natively where a model has that ControlNet.
for _channel, _title, _type in (("pose", "Pose", CONTROL_POSE), ("depth", "Depth", CONTROL_DEPTH), ("canny", "Canny", CONTROL_CANNY), ("normal", "Normal map", CONTROL_NORMAL)):
    SERVICES.append({
        "id": "control_" + _channel, "title": "ControlNet - " + _title,
        "path": "/nodes", "api": "/api/controlnet", "status": "live",
        "summary": "Extract a tested " + _title + " map for compatible image generation.",
        "inputs": [{"type": IMAGE, "field": "image", "required": True, "title": "Source image"}],
        "outputs": [{"type": _type, "field": "image_url_string", "title": _title + " map"}],
        "compatible_image_families": [] if _channel == "normal" else ["zimage"],
    })


# --------------------------------------------------- enhancement (three nodes)
#
# Deliberately three services rather than one with a mode switch: they take
# different settings, cost different amounts of GPU time, and are wired in
# different places in a graph — an upscale usually ends a chain, a face fix
# usually sits in the middle of one.
#
# Everything here runs on the image boxes (f5, f12, f15, Raptor). The
# video card, worker-4090, carries no ESRGAN weights and no TiledDiffusion, so
# video super-resolution is declared and disabled rather than quietly missing;
# `blocked_reason` is what the palette shows instead of a generic tooltip.
SERVICES.extend([
    {
        # Owner rule 2026-09-26: render within half-HD, enlarge at the end.
        "id": "upscale2x", "title": "Upscale 2×", "path": "/nodes",
        "api": "/api/upscale2x", "status": "live",
        "summary": "Fast 2x enlargement of a picture or a clip (RealESRGAN x2, per frame for video). Put it last.",
        "inputs": [{"type": IMAGE, "field": "image", "required": True,
                    "title": "Picture or clip", "also_accepts": [VIDEO]}],
        "outputs": [{"type": IMAGE, "field": "image_url_string", "title": "Picture ×2"},
                    {"type": VIDEO, "field": "video_url_string", "title": "Clip ×2"}],
    },
    {
        "id": "upscale", "title": "Upscale", "path": "/nodes",
        "api": "/api/upscale", "status": "live",
        "summary": "Enlarge a picture 2x or 4x, optionally re-rendering the new pixels.",
        "inputs": [
            {"type": IMAGE, "field": "image", "required": True, "title": "Picture to enlarge"},
            {"type": TEXT, "field": "prompt", "required": False, "title": "Subject (refine mode)"},
        ],
        "outputs": [{"type": IMAGE, "field": "image_url_string", "title": "Enlarged picture"}],
    },
    {
        "id": "detail_enhance", "title": "Detail enhancer", "path": "/nodes",
        "api": "/api/detail", "status": "live", "slow": True,
        "summary": "Add texture and micro-detail at the picture's own size.",
        "inputs": [
            {"type": IMAGE, "field": "image", "required": True, "title": "Picture to enrich"},
            {"type": TEXT, "field": "prompt", "required": False, "title": "Subject"},
        ],
        "outputs": [{"type": IMAGE, "field": "image_url_string", "title": "Enriched picture"}],
    },
    {
        "id": "face_fix", "title": "Face & detail fixer", "path": "/nodes",
        "api": "/api/facefix", "status": "live", "slow": True,
        "summary": "Re-render the face at a higher pixel density and stitch it back.",
        "inputs": [
            {"type": IMAGE, "field": "image", "required": True, "title": "Picture with a face"},
            {"type": TEXT, "field": "prompt", "required": False, "title": "Subject"},
        ],
        "outputs": [{"type": IMAGE, "field": "image_url_string", "title": "Repaired picture"}],
    },
    {
        "id": "upscale_video", "title": "Upscale video", "path": "/nodes",
        "api": "/api/upscale", "status": "planned", "slow": True,
        "summary": "Super-resolution for a clip, with the frames kept consistent.",
        "blocked_reason": (
            "Needs weights the farm does not have: SeedVR2 (seedvr2_ema_3b, ~6 GB) "
            "on worker-4090, or any ESRGAN file in its upscale_models folder, which "
            "is empty. The image boxes have the weights but only 8 GB of VRAM."
        ),
        "inputs": [
            {"type": VIDEO, "field": "video_url", "required": True, "title": "Clip to enlarge"},
        ],
        "outputs": [{"type": VIDEO, "field": "video_url_string", "title": "Enlarged clip"}],
    },
])


# ------------------------------------------------------------- Qwen-Image
#
# One node, not two. Qwen-Image and Qwen-Image-Edit are the same architecture
# and the same text encoder, and the only thing that decides which of them is
# asked is whether a picture is wired into the node: text alone draws, text
# plus a picture rewrites that picture. Splitting it into a generator and an
# editor would make the person choose a node for a decision the graph already
# states by its wiring.
#
# It runs as a GGUF quantisation through ComfyUI-GGUF, which is what lets a
# 20B MMDiT onto the 8 GB and 12 GB image boxes rather than onto the single
# 24 GB card, so two of these can render at the same time on two machines.
SERVICES.append({
    "id": "qwen_image", "title": "Qwen-Image", "path": "/nodes",
    "api": "/api/qwen-image", "status": "live",
    "system_prompt_capable": True,
    "system_prompt_default": (  # = ai_qwen_image_api.QWEN_SYSTEM_PROMPT_DEFAULT
        "Remix {images} into one coherent image: unify the style and lighting, "
        "combine the subjects and the story of all inputs; image 1 is the base scene and composition."),
    "summary": "The farm's one edit model: Qwen-Image 2.1 turbo (6 steps, 15-40 s). Rewrite the picture wired in (up to 3 pictures), or draw from a prompt alone.",
    "inputs": [
        {"type": TEXT, "field": "prompt", "required": False,
         "title": "What to draw, or what to change (optional with pictures)"},
        {"type": IMAGE, "field": "image", "required": False,
         "title": "Picture to edit", "ref_index": 1, "also_accepts": [VIDEO]},
        # Qwen-Image 2.1 turbo reads up to three pictures (image 1..3). It is
        # the farm's only edit model since 2026-09-26.
        *MULTIREF_INPUTS(3),
    ],
    "outputs": [
        {"type": IMAGE, "field": "image_url_string", "title": "Picture"},
    ],
})


# ------------------------------------------------------------------ Music
#
# Stable Audio 3 medium (ai_music_api, renderfin.music). Text, a picture or a
# clip in: the Vision model (or the Text model, for text alone) writes the
# Stable Audio prompt under the node's system prompt, and the length follows
# the clip. A clip also comes back with the music under it.
import ai_music_api as _music_api  # noqa: E402

SERVICES.append({
    "id": "music", "title": "Music · Stable Audio 3", "path": "/nodes",
    "api": "/api/music", "status": "live",
    "summary": ("Music from text, a picture or a clip: Vision writes the prompt, Stable Audio 3 "
                "medium renders stereo 44.1 kHz. Length follows the clip, else 30 s."),
    "system_prompt_capable": True,
    "system_prompt_default": _music_api.MUSIC_SYSTEM_PROMPT_DEFAULT,
    "inputs": [
        {"type": TEXT, "field": "prompt", "required": False, "title": "Idea or prompt"},
        {"type": IMAGE, "field": "image", "required": False,
         "title": "Picture or clip", "also_accepts": [VIDEO]},
    ],
    "outputs": [
        {"type": AUDIO, "field": "audio_url_string", "title": "Music"},
        {"type": VIDEO, "field": "video_url_string", "title": "Clip with music"},
        {"type": TEXT, "field": "prompt_string", "title": "Prompt used"},
    ],
    "models_url": "/api/ai/models",
})

PARAMS: Dict[str, List[Dict[str, object]]] = {
    "music": [
        {"name": "duration_s", "title": "Length, s", "type": "number", "min": 0, "max": 380,
         "step": 1, "default": 0,
         "help": "0 = the clip's length, else 30 s (Draft caps text/picture music at 30 s)"},
        {"name": "render_quality", "title": "Quality", "type": "select", "default": "",
         "help": "Blank follows the graph. Draft = 4 sampler steps, Full = 8",
         "options": [
             {"value": "", "title": "Follow the graph"},
             {"value": "preview", "title": "Draft — 4 steps"},
             {"value": "fast", "title": "6 steps"},
             {"value": "normal", "title": "Full — 8 steps"},
         ]},
        {"name": "instrumental", "title": "Vocals", "type": "select", "default": "true",
         "options": [{"value": "true", "title": "Instrumental"},
                     {"value": "false", "title": "Vocals allowed"}]},
        {"name": "reprompt", "title": "Prompt", "type": "select", "default": "true",
         "options": [{"value": "true", "title": "Written by Vision/Text from the inputs"},
                     {"value": "false", "title": "Use my text as it is"}]},
        {"name": "negative", "title": "Avoid", "type": "text", "default": ""},
        {"name": "model", "title": "👁 Prompt writer", "type": "select", "source": "ai_models",
         "default": "qwen35-9b-uncensored",
         "help": "Vision/Text model that writes the music prompt"},
        {"name": "seed", "title": "Seed", "type": "number", "min": 0, "max": 9007199254740991,
         "step": 1, "default": 0, "help": "0 gives a different piece each run"},
    ],
    "qwen_image": [
        # The node's own text (owner, 2026-09-27): visible in every view; a
        # wired prompt wins.
        {"name": "prompt", "title": "What to draw or change", "type": "textarea",
         "default": "", "help": "Leave empty if a prompt is wired in, or to remix the pictures"},
        # Automatic is the honest default: the wiring already says which of
        # the two models is meant. The explicit choices exist for the case
        # where a picture is wired in as a style reference but the person
        # wants a fresh composition anyway.
        {"name": "mode", "title": "Mode", "type": "select", "default": "auto",
         "options": [
             {"value": "auto", "title": "Automatic — edit when a picture is wired in"},
             {"value": "generate", "title": "Generate — ignore any picture"},
             {"value": "edit", "title": "Edit — a picture is required"},
         ]},
        {"name": "checkpoint", "title": "Model", "type": "model",
         "source": "checkpoints", "default": "",
         "help": "Leave empty: Qwen-Image 2.1 turbo is the only edit model. Old edit files are redirected to it"},
        # In edit mode these follow the picture that came in unless they are
        # set: an edit that silently reframed the source to 960x540 was the
        # single most confusing thing about the first version of this node.
        # The step is one pixel on purpose. The editor's follow-the-input-size
        # feature writes a source picture's exact dimensions into these two
        # controls and drops the value when the control rejects it, so a
        # coarser step left the node at a size half taken from the picture and
        # half left over from the default.
        {"name": "width", "title": "Width", "type": "number", "min": 256, "max": 2048,
         "step": 1, "default": 960,
         "help": "Editing follows the source picture unless width and height are both set"},
        {"name": "height", "title": "Height", "type": "number", "min": 256, "max": 2048,
         "step": 1, "default": 540,
         "help": "Editing follows the source picture unless width and height are both set"},
        {"name": "steps", "title": "Steps", "type": "range", "min": 0, "max": 60,
         "step": 1, "default": 0, "help": "Ignored by Qwen-Image 2.1 turbo (fixed 6 steps)"},
        {"name": "cfg", "title": "CFG", "type": "number", "default": 0, "min": 0,
         "max": 30, "step": 0.1, "help": "Ignored by Qwen-Image 2.1 turbo (no CFG)"},
        {"name": "negative_prompt", "title": "Avoid", "type": "text", "default": ""},
        {"name": "seed", "title": "Seed", "type": "number", "min": 0,
         "max": 9007199254740991, "step": 1, "default": 0,
         "help": "0 gives a different picture each run"},
    ],
    "upscale": [
        {"name": "scale", "title": "Factor", "type": "select", "default": "2",
         "options": [{"value": "2", "title": "2x"}, {"value": "4", "title": "4x"}],
         "help": "The output is the source size times this, up to 4096 px on the long edge"},
        {"name": "mode", "title": "Mode", "type": "select", "default": "fast",
         "options": [
             {"value": "fast", "title": "Fast — ESRGAN only"},
             {"value": "refine", "title": "Refine — ESRGAN then tiled re-render"},
         ],
         "help": "Refine invents real texture in the new pixels and costs minutes"},
        {"name": "model", "title": "Upscaler", "type": "select",
         "default": "4x_NMKD-Siax_200k.pth",
         "options": [
             {"value": "4x_NMKD-Siax_200k.pth", "title": "NMKD Siax 4x"},
             {"value": "RealESRGAN_x4.pth",
              "title": "RealESRGAN 4x — installed on f15 only", "disabled": True},
             {"value": "RealESRGAN_x2.pth",
              "title": "RealESRGAN 2x — installed on f15 only", "disabled": True},
         ],
         "help": "Only models present on every image worker can be chosen"},
    ],
    "detail_enhance": [
        {"name": "strength", "title": "Strength", "type": "range", "min": 0.05, "max": 1,
         "step": 0.05, "default": 0.35,
         "help": "How much texture to invent; above ~0.6 the subject starts to change"},
        {"name": "tile", "title": "Tiling", "type": "select", "default": "true",
         "options": [
             {"value": "true", "title": "Tiled — safe at any size"},
             {"value": "false", "title": "Single pass — small pictures only"},
         ]},
    ],
    "face_fix": [
        {"name": "fidelity", "title": "Fidelity", "type": "range", "min": 0.05, "max": 1,
         "step": 0.05, "default": 0.6,
         "help": "1 keeps the face it found, 0 rebuilds it"},
        {"name": "hands_eyes", "title": "Region", "type": "select", "default": "false",
         "options": [
             {"value": "false", "title": "Face only"},
             {"value": "true", "title": "Face and bare skin (hands, neck)"},
         ],
         "help": "The wider region is slower and rewrites more of the picture"},
    ],
    "avatar_build": [
        {"name": "render_quality", "title": "Quality", "type": "select", "default": "",
         "help": "Size of the drawn views. Blank follows the graph (Draft 1/4 by default); "
                 "a draft also skips the retry on the other engine",
         "options": [
             {"value": "", "title": "Follow the graph"},
             {"value": "preview", "title": "Draft ¼ — fast, low resolution"},
             {"value": "fast", "title": "½"},
             {"value": "normal", "title": "Full 1×"},
             {"value": "highquality", "title": "2× (cap 2048 px)"},
         ]},
        {"name": "outfit", "title": "Outfit", "type": "text", "default": "",
         "help": "Blank keeps what the source wears"},
        {"name": "display_name", "title": "Name", "type": "text", "default": "",
         "help": "Blank lets Vision name it"},
        {"name": "seed", "title": "Seed", "type": "number", "min": 0, "max": 2147483647,
         "step": 1, "default": 0, "help": "0 derives one from the source"},
        {"name": "model", "title": "👁 Describe", "type": "select", "source": "ai_models",
         "default": "qwen35-9b-uncensored",
         "help": "Vision model that writes the character description"},
        {"name": "judge_model", "title": "⚖ Judge", "type": "select", "source": "ai_models",
         "default": "qwen35-9b-uncensored",
         "help": "Vision model that checks each view against the source"},
        {"name": "engine", "title": "🎨 Views", "type": "select", "default": "auto",
         "help": "Model that draws the views",
         "options": [
             {"value": "auto", "title": "Auto · FLUX.2 klein 4B"},
             {"value": "klein", "title": "FLUX.2 klein 4B"},
             {"value": "qwen", "title": "Qwen-Image 2.1 turbo"},
         ]},
        {"name": "retry_engine", "title": "↻ Retry", "type": "select", "default": "auto",
         "help": "Model that redraws a view that failed the check",
         "options": [
             {"value": "auto", "title": "Auto · the other one"},
             {"value": "qwen", "title": "Qwen-Image 2.1 turbo"},
             {"value": "klein", "title": "FLUX.2 klein 4B"},
             {"value": "none", "title": "No retry"},
         ]},
    ],
    "video_frame": [
        {"name": "template", "title": "Template", "type": "select", "default": "start_end",
         "options": [{"value": "start_end", "title": "Start-End (per scene)"},
                     {"value": "start_middle_end", "title": "Start-Middle-End (per scene)"},
                     {"value": "start_only", "title": "Start only"},
                     {"value": "end_only", "title": "End only"},
                     {"value": "n_per_scene", "title": "N evenly spaced per scene"},
                     {"value": "every_n", "title": "Every N frames"}]},
        {"name": "detect_scenes", "title": "Detect scenes", "type": "select", "default": "on",
         "options": [{"value": "on", "title": "On (per scene)"}, {"value": "off", "title": "Off (whole clip)"}]},
        {"name": "n", "title": "N", "type": "number", "min": 1, "max": 64, "step": 1, "default": 4,
         "help": "N per scene, or the step for Every N frames"},
        {"name": "offset", "title": "Offset from cut", "type": "number", "min": 0, "max": 48, "step": 1, "default": 0,
         "help": "Skip this many frames after a cut and before the next (transition blur)"},
    ],
    "scene_split": [
        {"name": "sensitivity", "title": "Cut sensitivity (z)", "type": "range", "min": 4, "max": 40, "step": 1,
         "default": 12, "help": "A cut must stand this many robust deviations above the clip's typical change; lower = more cuts"},
        {"name": "local_ratio", "title": "Jump vs neighbours", "type": "range", "min": 2, "max": 20, "step": 0.5,
         "default": 6, "help": "A cut must be this many times the mean change of the surrounding half second"},
        {"name": "max_frames", "title": "Max shot frames", "type": "number", "min": 9, "max": 393, "step": 8,
         "default": 97, "help": "Longer shots are split into parts (97 = Wan-Animate / LTX limit at 24 fps)"},
        {"name": "min_shot_seconds", "title": "Min shot (s)", "type": "number", "min": 0, "max": 5, "step": 0.1,
         "default": 0.5, "help": "Cuts closer than this are ignored (flicker)"},
        {"name": "max_seconds", "title": "Only first N s", "type": "number", "min": 0, "max": 600, "step": 1,
         "default": 0, "help": "0 = whole video"},
    ],
    "video_summary": [
        {"name": "fit", "title": "Fit segments", "type": "select", "default": "resample",
         "options": [{"value": "resample", "title": "Resample (first+last frame renders)"},
                     {"value": "trim", "title": "Trim (start-frame renders)"}]},
        {"name": "fps", "title": "FPS", "type": "number", "min": 8, "max": 60, "step": 1, "default": 24},
    ],
    "video_concat": [
        {"name": "checkpoint", "title": "Model for all shots", "type": "model", "source": "checkpoints", "default": "",
         "help": "Empty = inherit. Set: overrides the model of every LTX / MiniMax video node wired into this Concat"},
        {"name": "lora", "title": "Style (LoRA) for all shots", "type": "model", "source": "loras", "default": ""},
        {"name": "lora_strength", "title": "Style strength", "type": "range", "min": 0, "max": 1.5, "step": 0.05,
         "default": 0, "help": "0 leaves the workflow's own strength"},
        {"name": "loras", "title": "LoRA stack for all shots", "type": "lora_stack", "default": "",
         "help": "Empty = inherit. Set: replaces the LoRAs of the video nodes wired into this Concat (filtered by model family)"},
        {"name": "out_width", "title": "Width", "type": "number", "min": 0, "max": 4096, "step": 2, "default": 0,
         "help": "0 = size of the first clip"},
        {"name": "out_height", "title": "Height", "type": "number", "min": 0, "max": 4096, "step": 2, "default": 0,
         "help": "0 = size of the first clip"},
        {"name": "fps", "title": "FPS", "type": "number", "min": 8, "max": 60, "step": 1, "default": 24},
    ],
    "wan_image": [
        {"name": "width", "title": "Width", "type": "number", "min": 256, "max": 2048, "step": 2, "default": 960},
        {"name": "height", "title": "Height", "type": "number", "min": 256, "max": 2048, "step": 2, "default": 544},
        {"name": "frame_count", "title": "Frames", "type": "range", "min": 9, "max": 97, "step": 8, "default": 97},
        {"name": "control_strength", "title": "Control strength", "type": "range", "min": 0, "max": 1, "step": 0.05, "default": 1},
        {"name": "seed", "title": "Seed", "type": "number", "min": 0, "max": 9007199254740991, "step": 1, "default": 0},
    ],
    "avatar_video": [
        {"name": "width", "title": "Width", "type": "number", "min": 256, "max": 2048,
         "step": 1, "default": 960,
         "help": "Custom size; width × height must not exceed the verified 524288 pixel area"},
        {"name": "height", "title": "Height", "type": "number", "min": 256, "max": 2048,
         "step": 1, "default": 540,
         "help": "Custom size; width × height must not exceed the verified 524288 pixel area"},
        {"name": "frame_count", "title": "Frames", "type": "range", "min": 9, "max": 97,
         "step": 8, "default": 97},
        {"name": "control_strength", "title": "Control strength", "type": "range",
         "min": 0, "max": 1, "step": 0.05, "default": 1},
        {"name": "seed", "title": "Seed", "type": "number", "min": 0,
         "max": 9007199254740991, "step": 1, "default": 0},
    ],
    "video_control": [
        {"name": "width", "title": "Width", "type": "number", "min": 256, "max": 2048, "step": 2, "default": 960},
        {"name": "height", "title": "Height", "type": "number", "min": 256, "max": 2048, "step": 2, "default": 540},
        {"name": "frame_count", "title": "Frames", "type": "number", "min": 9, "max": 393, "step": 8, "default": 97},
        {"name": "control_channel", "title": "Motion guide", "type": "select", "default": "canny",
         "options": [{"value": "canny", "title": "Canny"}, {"value": "pose", "title": "Pose"}, {"value": "depth", "title": "Depth"}]},
        {"name": "control_strength", "title": "Control strength", "type": "range", "min": 0, "max": 1, "step": 0.05, "default": 0.8},
        {"name": "seed", "title": "Seed", "type": "number", "min": 0, "max": 9007199254740991, "step": 1, "default": 0},
    ],
    "avatar_image": [
        {"name": "width", "title": "Width", "type": "number", "min": 256, "max": 2048, "step": 1, "default": 960},
        {"name": "height", "title": "Height", "type": "number", "min": 256, "max": 2048, "step": 1, "default": 540},
        {"name": "seed", "title": "Seed", "type": "number", "min": 0, "max": 9007199254740991, "step": 1, "default": 0},
    ],
    "vision": [
        {"name": "model", "title": "Model", "type": "select", "source": "ai_models",
         "default": "qwen35-9b-uncensored"},
        # Typed on the node when nothing is wired in; a wired question wins.
        {"name": "prompt", "title": "Question", "type": "textarea",
         "default": "Describe this picture.",
         "help": "What to ask about the image"},
        # Only consulted when a video is wired in. A single frame answers
        # "what is in this shot"; the storyboard is what answers "what
        # happens", which is the question people actually ask of a video.
        {"name": "video_mode", "title": "Video", "type": "select",
         "default": "storyboard",
         "options": [
             {"value": "storyboard", "title": "Whole video (frames start to end)"},
             {"value": "frame", "title": "First frame only"},
         ],
         "help": "Used when a video is connected instead of an image"},
        # Zero means automatic, and automatic is per model: a model that
        # reasons before answering needs a far bigger budget than one that
        # does not, and a single number for both starves one of them.
        {"name": "max_output_tokens", "title": "Answer length", "type": "number",
         "min": 0, "max": 4096, "step": 64, "default": 0,
         "help": "0 picks a budget to suit the model"},
    ],
    "text": [
        {"name": "model", "title": "Model", "type": "select", "source": "ai_models",
         "default": "qwen35-9b-uncensored"},
        # The instruction can be typed here instead of needing a node of its
        # own; a wired instruction wins over this one when both are present.
        {"name": "prompt", "title": "Instruction", "type": "textarea", "default": "",
         "help": "What to do with the text coming in, e.g. 'Summarise in one sentence'"},
        {"name": "max_output_tokens", "title": "Answer length", "type": "number",
         "min": 0, "max": 4096, "step": 64, "default": 0,
         "help": "0 picks a budget to suit the model"},
    ],
    "image": [
        # Typed on the node when nothing is wired in; a wired prompt wins.
        {"name": "prompt", "title": "What to draw", "type": "textarea",
         "default": "", "help": "Leave empty if a prompt is wired in"},
        {"name": "control_strength", "title": "Control strength", "type": "range", "min": 0, "max": 2, "step": 0.05, "default": 0.8},
        {"name": "control_start", "title": "Control start", "type": "range", "min": 0, "max": 1, "step": 0.05, "default": 0.0},
        {"name": "control_end", "title": "Control end", "type": "range", "min": 0, "max": 1, "step": 0.05, "default": 1.0},
        # Drawn as a picture list, not a text dropdown: a model is recognised
        # by what it produces, and the file name says nothing.
        {"name": "checkpoint", "title": "Model", "type": "model",
         "source": "checkpoints", "default": "",
         "help": "Leave empty for the workflow's own model"},
        {"name": "lora", "title": "Style (LoRA)", "type": "model",
         "source": "loras", "default": ""},
        {"name": "lora_strength", "title": "Style strength", "type": "range",
         "min": 0, "max": 1.5, "step": 0.05, "default": 0,
         "help": "0 leaves the workflow's own strength"},
        # A stack of LoRAs as Civitai-style tags, applied in order after the
        # single style LoRA. The same tags also work inside the prompt.
        {"name": "loras", "title": "LoRA stack", "type": "lora_stack", "default": "",
         "help": "<lora:NAME:WEIGHT> tags, applied in order; also accepted in the prompt"},
        {"name": "mode", "title": "Mode", "type": "select", "default": "",
         "options": [
             {"value": "", "title": "Plain"},
             {"value": "z_depth", "title": "From depth"},
             {"value": "t_pose", "title": "T-pose"},
             {"value": "open_pose", "title": "Open pose"},
             {"value": "inpaint", "title": "Inpaint — requires a separate Fill model", "disabled": True},
         ],
         "help": "The reference picture is read differently in each mode"},
        {"name": "negative_prompt", "title": "Avoid", "type": "text", "default": ""},
        {"name": "cfg", "title": "CFG", "type": "number", "default": 0, "min": 0, "max": 30, "step": 0.1},
        {"name": "sampler", "title": "Sampler", "type": "select", "default": "",
         "options": [{"value": "", "title": "Automatic"}, {"value": "euler", "title": "Euler"},
                     {"value": "euler_ancestral", "title": "Euler ancestral"},
                     {"value": "euler_ancestral_cfg_pp", "title": "Euler ancestral CFG++"},
                     {"value": "dpmpp_2m", "title": "DPM++ 2M"},
                     {"value": "dpmpp_2m_sde", "title": "DPM++ 2M SDE"},
                     {"value": "dpmpp_sde", "title": "DPM++ SDE"}]},
        {"name": "scheduler", "title": "Scheduler", "type": "select", "default": "",
         "options": [{"value": "", "title": "Automatic"}, {"value": "simple", "title": "Simple"},
                     {"value": "normal", "title": "Normal"}, {"value": "karras", "title": "Karras"},
                     {"value": "sgm_uniform", "title": "SGM uniform"}]},
        # The sizes models are actually demonstrated at, not a round-number
        # sample: a size taken off a model page needs an option to land in, or
        # the recommendation is silently dropped.
        {"name": "width", "title": "Width", "type": "select", "default": 960,
         "options": [{"value": 640, "title": "640"}, {"value": 540, "title": "540"}, {"value": 960, "title": "960"}, {"value": 768, "title": "768"}, {"value": 832, "title": "832"}, {"value": 896, "title": "896"}, {"value": 1024, "title": "1024"}, {"value": 1152, "title": "1152"}, {"value": 1216, "title": "1216"}, {"value": 1248, "title": "1248"}, {"value": 1280, "title": "1280"}, {"value": 1344, "title": "1344"}, {"value": 1536, "title": "1536"}]},
        {"name": "height", "title": "Height", "type": "select", "default": 540,
         "options": [{"value": 640, "title": "640"}, {"value": 540, "title": "540"}, {"value": 960, "title": "960"}, {"value": 768, "title": "768"}, {"value": 832, "title": "832"}, {"value": 896, "title": "896"}, {"value": 1024, "title": "1024"}, {"value": 1152, "title": "1152"}, {"value": 1216, "title": "1216"}, {"value": 1248, "title": "1248"}, {"value": 1280, "title": "1280"}, {"value": 1344, "title": "1344"}, {"value": 1536, "title": "1536"}]},
        # The range starts at zero because zero is a real choice here: it
        # means "leave the workflow's own value alone". A minimum of 4 would
        # be silently clamped up by the browser and every render would go out
        # with four steps, which ruins the picture.
        {"name": "steps", "title": "Steps", "type": "range", "min": 0, "max": 60,
         "step": 1, "default": 0, "help": "0 leaves the workflow's own value"},
        {"name": "creativity", "title": "Creativity", "type": "range", "min": 0,
         "max": 1, "step": 0.05, "default": 0, "help": "0 leaves the workflow's own value"},
        {"name": "seed", "title": "Seed", "type": "number", "min": 0, "max": 9007199254740991,
         "step": 1, "default": 0, "help": "0 gives a different picture each run"},
    ],
    "video": [
        {"name": "prompt", "title": "What should happen", "type": "textarea",
         "default": "", "help": "Leave empty if a prompt is wired in"},
        {"name": "width", "title": "Width", "type": "number", "default": 960, "min": 256, "max": 2048, "step": 2},
        {"name": "height", "title": "Height", "type": "number", "default": 540, "min": 256, "max": 2048, "step": 2},
        {"name": "checkpoint", "title": "Model", "type": "model",
         "source": "checkpoints", "default": "",
         "help": "Leave empty for the workflow's own model"},
        {"name": "lora", "title": "Style (LoRA)", "type": "model",
         "source": "loras", "default": ""},
        {"name": "lora_strength", "title": "Style strength", "type": "range",
         "min": 0, "max": 1.5, "step": 0.05, "default": 0,
         "help": "0 leaves the workflow's own strength"},
        # A stack of LoRAs as Civitai-style tags, applied in order after the
        # single style LoRA. The same tags also work inside the prompt.
        {"name": "loras", "title": "LoRA stack", "type": "lora_stack", "default": "",
         "help": "<lora:NAME:WEIGHT> tags, applied in order; also accepted in the prompt"},
        {"name": "frame_count", "title": "Frames", "type": "range", "min": 9, "max": 393,
         "step": 8, "default": 97, "help": "LTX uses 8n+1 frames: 9, 17, 25... at 24 fps"},
        {"name": "negative_prompt", "title": "Avoid", "type": "text", "default": ""},
        {"name": "cfg", "title": "CFG", "type": "number", "default": 0, "min": 0, "max": 30, "step": 0.1},
        {"name": "sampler", "title": "Sampler", "type": "select", "default": "",
         "options": [{"value": "", "title": "Automatic"}, {"value": "euler", "title": "Euler"},
                     {"value": "euler_ancestral", "title": "Euler ancestral"},
                     {"value": "euler_ancestral_cfg_pp", "title": "Euler ancestral CFG++"},
                     {"value": "dpmpp_2m", "title": "DPM++ 2M"},
                     {"value": "dpmpp_2m_sde", "title": "DPM++ 2M SDE"},
                     {"value": "dpmpp_sde", "title": "DPM++ SDE"}]},
        {"name": "scheduler", "title": "Scheduler", "type": "select", "default": "",
         "options": [{"value": "", "title": "Automatic"}, {"value": "simple", "title": "Simple"},
                     {"value": "normal", "title": "Normal"}, {"value": "karras", "title": "Karras"},
                     {"value": "sgm_uniform", "title": "SGM uniform"}]},
        # The range starts at zero because zero is a real choice here: it
        # means "leave the workflow's own value alone". A minimum of 4 would
        # be silently clamped up by the browser and every render would go out
        # with four steps, which ruins the picture.
        {"name": "steps", "title": "Steps", "type": "range", "min": 0, "max": 60,
         "step": 1, "default": 0, "help": "0 leaves the workflow's own value"},
        {"name": "creativity", "title": "Creativity", "type": "range", "min": 0,
         "max": 1, "step": 0.05, "default": 0, "help": "0 leaves the workflow's own value"},
        {"name": "seed", "title": "Seed", "type": "number", "min": 0, "max": 9007199254740991,
         "step": 1, "default": 0},
    ],
    "3dmodel": [
        {"name": "quality", "title": "Quality", "type": "select", "default": "standard",
         "options": [{"value": "fast", "title": "Fast"},
                     {"value": "standard", "title": "Standard"},
                     {"value": "high", "title": "High"}]},
        {"name": "background_method", "title": "Cut out the subject", "type": "select",
         "default": "auto",
         "options": [{"value": "auto", "title": "Automatic"},
                     {"value": "rembg", "title": "rembg"},
                     {"value": "none", "title": "Leave the background"}],
         "help": "Hunyuan fails outright when nothing survives the cut-out"},
    ],
}


def params_for(service_id: str) -> List[Dict[str, object]]:
    return PARAMS.get(service_id) or []


def service(service_id: str) -> Optional[Dict[str, object]]:
    for entry in SERVICES:
        if entry["id"] == service_id:
            return entry
    return None


def accepted_types(entry: Dict[str, object]) -> List[str]:
    return [str(item["type"]) for item in entry.get("inputs") or []]


def produced_types(entry: Dict[str, object]) -> List[str]:
    return [str(item["type"]) for item in entry.get("outputs") or []]


def targets_for(entity_type: str) -> List[Dict[str, str]]:
    """Services that can take this entity as an input, for the handoff menu.

    A planned service is included and flagged: knowing where a result is meant
    to go is useful before the destination is callable.
    """
    targets = []
    for entry in SERVICES:
        for item in entry.get("inputs") or []:
            if str(item["type"]) != entity_type:
                continue
            targets.append({
                "service_id": str(entry["id"]),
                "title": str(entry["title"]),
                "path": str(entry["path"]),
                "field": str(item["field"]),
                "status": str(entry["status"]),
                "input_title": str(item.get("title") or item["field"]),
            })
            break
    return targets


@router.get("/api/ai/services")
async def api_ai_services():
    """The typed catalogue the pages build themselves from."""
    return {
        "success_bool": True,
        "entity_types_array": ENTITY_TYPES,
        "services_array": [
            dict(entry, accepts_array=accepted_types(entry),
                 produces_array=produced_types(entry),
                 params_array=params_for(str(entry["id"])))
            for entry in SERVICES
        ],
        "handoff_object": {
            entity["id"]: targets_for(str(entity["id"]))
            for entity in ENTITY_TYPES
        },
        "server_time_unix_int": int(time.time()),
    }


# ------------------------------------------------------------ Camera orbit
#
# (2026-09-27) See the same subject from another camera position.
# Image: Qwen-Image 2.1 turbo with a written camera prompt (preset or
# yaw/pitch/zoom). Video: LTX-2.5 + CrossView-Prompt IC-LoRA re-shoots a clip
# from a new angle (worker-4090). ai_camera_orbit_api.
import ai_camera_orbit_api as _camera_api  # noqa: E402

SERVICES.append({
    "id": "camera_orbit_image", "title": "Camera orbit · image", "path": "/nodes",
    "api": "/api/camera-orbit/image", "status": "live",
    "summary": ("The same subject from another camera position: orbit left/right up to the back "
                "view, high/low angle, dolly in/out. Qwen-Image 2.1 turbo; a clip gives its first frame."),
    "inputs": [
        {"type": IMAGE, "field": "image", "required": True, "title": "Picture or clip",
         "also_accepts": [VIDEO]},
        {"type": TEXT, "field": "prompt", "required": False, "title": "Extra description"},
    ],
    "outputs": [
        {"type": IMAGE, "field": "image_url_string", "title": "New view"},
        {"type": TEXT, "field": "prompt_string", "title": "Camera prompt"},
    ],
})
SERVICES.append({
    "id": "camera_orbit_video", "title": "Camera orbit · video", "path": "/nodes",
    "api": "/api/camera-orbit/video", "status": "live", "slow": True,
    "summary": ("Re-shoot a clip from a new camera angle (same action): LTX-2.5 + CrossView IC-LoRA. "
                "Frontal sector up to ~60°; chain two nodes for a bigger move. worker-4090."),
    "inputs": [
        {"type": VIDEO, "field": "video_url", "required": True, "title": "Clip to re-shoot"},
    ],
    "outputs": [
        {"type": VIDEO, "field": "video_url_string", "title": "New angle clip"},
        {"type": TEXT, "field": "prompt_string", "title": "Camera prompt"},
    ],
})
PARAMS["camera_orbit_image"] = [
    {"name": "preset", "title": "Camera", "type": "select", "default": "orbit_right_45",
     "options": [{"value": k, "title": t} for k, t in _camera_api.IMAGE_PRESET_TITLES.items()]},
    {"name": "yaw", "title": "Yaw °", "type": "range", "min": -180, "max": 180, "step": 15, "default": 0,
     "help": "Custom only: + = camera orbits to the right, 180 = back view"},
    {"name": "pitch", "title": "Pitch °", "type": "range", "min": -60, "max": 90, "step": 5, "default": 0,
     "help": "Custom only: + = camera above looking down, - = below looking up"},
    {"name": "zoom", "title": "Zoom", "type": "range", "min": 0.25, "max": 4, "step": 0.25, "default": 1,
     "help": "Custom only: >1 closer (dolly in), <1 further (dolly out)"},
    {"name": "width", "title": "Width", "type": "number", "min": 256, "max": 2048, "step": 1, "default": 960},
    {"name": "height", "title": "Height", "type": "number", "min": 256, "max": 2048, "step": 1, "default": 540},
    {"name": "seed", "title": "Seed", "type": "number", "min": 0, "max": 9007199254740991, "step": 1,
     "default": 0, "help": "0 gives a different picture each run"},
]
PARAMS["camera_orbit_video"] = [
    {"name": "azimuth", "title": "Orbit", "type": "select", "default": "to the right",
     "options": [{"value": v, "title": v} for v in _camera_api.AZIMUTHS],
     "help": "slightly ~15°, to the ~30°, far ~50° (frontal sector only)"},
    {"name": "elevation", "title": "Height", "type": "select", "default": "same height",
     "options": [{"value": v, "title": v} for v in _camera_api.ELEVATIONS]},
    {"name": "distance", "title": "Distance", "type": "select", "default": "same distance",
     "options": [{"value": v, "title": v} for v in _camera_api.DISTANCES]},
    {"name": "strength", "title": "LoRA strength", "type": "range", "min": 0.5, "max": 2.5, "step": 0.1,
     "default": 1.5, "help": "1.5 measured best on distilled LTX-2.5; lower = closer to the source"},
    {"name": "frame_count", "title": "Frames", "type": "number", "min": 0, "max": 193, "step": 8, "default": 0,
     "help": "0 = the clip's length (8k+1, up to 193)"},
    {"name": "width", "title": "Width", "type": "number", "min": 256, "max": 2048, "step": 2, "default": 960},
    {"name": "height", "title": "Height", "type": "number", "min": 256, "max": 2048, "step": 2, "default": 544},
    {"name": "seed", "title": "Seed", "type": "number", "min": 0, "max": 9007199254740991, "step": 1, "default": 0},
]
