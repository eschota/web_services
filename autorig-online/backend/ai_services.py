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
AVATAR = "avatar"
MODEL3D = "model3d"
CONTROL_POSE = "control_pose"
CONTROL_DEPTH = "control_depth"
CONTROL_CANNY = "control_canny"

ENTITY_TYPES: List[Dict[str, object]] = [
    {"id": AVATAR, "title": "Avatar", "carries": "saved private Avatar id and immutable version", "icon": "👤"},
    {"id": CONTROL_POSE, "title": "Pose control", "carries": "validated OpenPose map URL", "icon": "🧍"},
    {"id": CONTROL_DEPTH, "title": "Depth control", "carries": "validated depth map URL", "icon": "▧"},
    {"id": CONTROL_CANNY, "title": "Canny control", "carries": "validated edge map URL", "icon": "▱"},
    {
        "id": TEXT,
        "title": "Text",
        "carries": "the string itself",
        "icon": "📝",
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
]

# --------------------------------------------------------------------- services

# `status` is either "live" (wired end to end) or "planned" (declared so the
# handoff UI can show where a result could go, but not yet callable). A page
# shows a planned service greyed out instead of pretending it works.
SERVICES: List[Dict[str, object]] = [
    {
        "id": "video_frame", "title": "Video first frame", "path": "/nodes",
        "api": "/api/ai/video-reference", "status": "live",
        "summary": "Extract the first frame of a driving video for scene and character editing.",
        "inputs": [{"type": VIDEO, "field": "video_url", "required": True, "title": "Source video"}],
        "outputs": [{"type": IMAGE, "field": "image_url_string", "title": "First frame"}],
    },
    {
        "id": "video_storyboard", "title": "Video storyboard", "path": "/nodes",
        "api": "/api/ai/video-reference", "status": "live",
        "summary": "Five chronological frames for Vision to describe the scene and action.",
        "inputs": [{"type": VIDEO, "field": "video_url", "required": True, "title": "Source video"}],
        "outputs": [{"type": IMAGE, "field": "image_url_string", "title": "Timeline image"}],
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
        "summary": "Ask a question about an image and get an answer in text.",
        "status": "live",
        "inputs": [
            {"type": IMAGE, "field": "image", "required": True,
             "title": "Image to look at"},
            # Not required as a wire: the question is usually a fixed sentence,
            # and needing a whole node to hold it was the commonest way to end
            # up submitting a request with no prompt at all.
            {"type": TEXT, "field": "prompt", "required": False,
             "title": "What to ask about it"},
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
        "title": "Image",
        "path": "/image",
        "api": "/api/image",
        "summary": "Generate a picture from a prompt, optionally guided by a reference image.",
        "status": "live",
        "inputs": [
            {"type": TEXT, "field": "prompt", "required": True,
             "title": "What to draw"},
            {"type": IMAGE, "field": "image", "required": False,
             "title": "Reference image"},
            {"type": CONTROL_POSE, "field": "control_pose", "required": False, "title": "Pose control"},
            {"type": CONTROL_DEPTH, "field": "control_depth", "required": False, "title": "Depth control"},
            {"type": CONTROL_CANNY, "field": "control_canny", "required": False, "title": "Canny control"},
        ],
        "outputs": [
            {"type": IMAGE, "field": "image_url_string", "title": "Picture"},
        ],
    },
    {
        "id": "video",
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
for _channel, _title, _type in (("pose", "Pose", CONTROL_POSE), ("depth", "Depth", CONTROL_DEPTH), ("canny", "Canny", CONTROL_CANNY)):
    SERVICES.append({
        "id": "control_" + _channel, "title": "ControlNet - " + _title,
        "path": "/nodes", "api": "/api/controlnet", "status": "live",
        "summary": "Extract a tested " + _title + " map for compatible image generation.",
        "inputs": [{"type": IMAGE, "field": "image", "required": True, "title": "Source image"}],
        "outputs": [{"type": _type, "field": "image_url_string", "title": _title + " map"}],
        "compatible_image_families": ["flux"],
    })


# --------------------------------------------------- enhancement (three nodes)
#
# Deliberately three services rather than one with a mode switch: they take
# different settings, cost different amounts of GPU time, and are wired in
# different places in a graph — an upscale usually ends a chain, a face fix
# usually sits in the middle of one.
#
# Everything here runs on the FLUX image boxes (f5, f12, f15, Raptor). The
# video card, worker-4090, carries no ESRGAN weights and no TiledDiffusion, so
# video super-resolution is declared and disabled rather than quietly missing;
# `blocked_reason` is what the palette shows instead of a generic tooltip.
SERVICES.extend([
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


PARAMS: Dict[str, List[Dict[str, object]]] = {
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
         "default": "bonsai2-27b"},
        # Typed on the node when nothing is wired in; a wired question wins.
        {"name": "prompt", "title": "Question", "type": "textarea",
         "default": "Describe this picture.",
         "help": "What to ask about the image"},
        # Zero means automatic, and automatic is per model: a model that
        # reasons before answering needs a far bigger budget than one that
        # does not, and a single number for both starves one of them.
        {"name": "max_output_tokens", "title": "Answer length", "type": "number",
         "min": 0, "max": 4096, "step": 64, "default": 0,
         "help": "0 picks a budget to suit the model"},
    ],
    "text": [
        {"name": "model", "title": "Model", "type": "select", "source": "ai_models",
         "default": "bonsai2-27b"},
        # The instruction can be typed here instead of needing a node of its
        # own; a wired instruction wins over this one when both are present.
        {"name": "prompt", "title": "Instruction", "type": "textarea", "default": "",
         "help": "What to do with the text coming in, e.g. 'Summarise in one sentence'"},
        {"name": "max_output_tokens", "title": "Answer length", "type": "number",
         "min": 0, "max": 4096, "step": 64, "default": 0,
         "help": "0 picks a budget to suit the model"},
    ],
    "image": [
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
