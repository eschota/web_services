"""Pydantic models mirroring the C# NoDeadLineWebServer render API (Render.cs)."""
from __future__ import annotations

import time
import uuid
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field, field_validator


class RenderPrompt(BaseModel):
    """Port of C# RenderPrompt (Render.cs:1061)."""

    prompt: str = ""
    negative_prompt: str = ""
    image_url: str = ""
    type: str = ""
    work_flow: str = ""
    main_size_width: int = 0
    main_size_height: int = 0
    aspect_ratio: float = 0
    frame_count: int = 60
    noise_seed: int = 0
    steps: int = 0
    creativity: float = 0
    user_name: str = "default_user"
    render_mode: str = ""

    @field_validator("frame_count")
    @classmethod
    def _clamp_frame_count(cls, v: int) -> int:
        return max(0, min(300, int(v or 0)))

    @field_validator("user_name")
    @classmethod
    def _safe_user_name(cls, v: str) -> str:
        v = (v or "").strip() or "default_user"
        # path-safety: user_name becomes a directory under render/
        cleaned = "".join(c for c in v if c.isalnum() or c in "-_.")
        if not cleaned.strip("."):
            return "default_user"
        return cleaned


class RenderServer(BaseModel):
    """Port of C# RenderServer (Render.cs:755). Doubles as the registration body."""

    render_server_name: str = ""
    render_server_url: str = ""
    gpu_name: str = ""
    status: str = "offline"
    available_workflows: List[str] = Field(default_factory=list)
    workflow_overrides: Dict[str, str] = Field(default_factory=dict)
    queue_size: int = 0
    current_render_task: Optional[str] = None
    average_render_time: float = 0
    render_operation: Optional[str] = None  # add_server | delete_server | info | set_status
    basic_auth: bool = False
    date_update: Optional[str] = None
    online_since_utc: Optional[str] = None


TASK_PENDING = "Pending"
TASK_RENDERING = "Rendering"
TASK_DONE = "Done"
TASK_ERROR = "Error"


class RenderTask(BaseModel):
    """Internal queue item persisted to sqlite."""

    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    prompt: RenderPrompt
    workflow: str = ""          # scheduling token (e.g. gen_image.json)
    workflow_file: str = ""     # actual template file resolved from prompt.type
    output_ext: str = ".png"
    status: str = TASK_PENDING
    server_name: str = ""
    comfy_prompt_id: str = ""
    output_url: str = ""
    output_path: str = ""
    extra_outputs: Dict[str, str] = Field(default_factory=dict)  # e.g. isolated -> path/url
    error: str = ""
    submit_failures: int = 0
    created_at: float = Field(default_factory=time.time)
    started_at: float = 0
    finished_at: float = 0

    def public_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "status": self.status,
            "status_string": self.status,
            "workflow": self.workflow,
            "workflow_file": self.workflow_file,
            "render_server_name": self.server_name,
            "prompt_id": self.comfy_prompt_id,
            "output_url": self.output_url,
            "output_url_string": self.output_url,
            "extra_outputs": self.extra_outputs,
            "error": self.error,
            "error_string": self.error,
            "user_name": self.prompt.user_name,
            "created_at": self.created_at,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
        }


CHARGEN_STAGE_PROMPT = "prompt"
CHARGEN_STAGE_FLUX = "flux_render"
CHARGEN_STAGE_AWAITING_IMAGE = "awaiting_image_approval"
CHARGEN_STAGE_HUNYUAN = "hunyuan"
CHARGEN_STAGE_TURNTABLE = "turntable"
CHARGEN_STAGE_READY = "ready"
CHARGEN_STAGE_FAILED = "failed"
CHARGEN_STAGE_DISCARDED = "discarded"
CHARGEN_STAGE_SUBMITTED = "submitted"


class CharacterGenJob(BaseModel):
    """Composite pipeline job: flux t_pose render -> hunyuan image_to_3d -> turntable video."""

    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    prompt: str = ""
    negative_prompt: str = ""
    mask_url: str = ""
    user_name: str = "autorig-bot"
    source_task_id: str = ""
    stage: str = CHARGEN_STAGE_FLUX
    flux_task_id: str = ""
    hunyuan_task_id: str = ""
    hunyuan_worker: str = ""
    # Telegram context: renderfin delivers results itself so they survive
    # bot restarts. `delivered` maps delivery kind -> the content marker that
    # was delivered (image/video url, error text), keeping it idempotent.
    telegram_chat_id: int = 0
    telegram_message_id: int = 0
    telegram_status_message_id: int = 0
    delivered: Dict[str, str] = Field(default_factory=dict)
    warning: str = ""
    image_url: str = ""       # full t_pose render
    isolated_url: str = ""    # alpha-isolated character
    glb_url: str = ""
    video_url: str = ""
    error: str = ""
    created_at: float = Field(default_factory=time.time)
    updated_at: float = Field(default_factory=time.time)

    def public_dict(self) -> Dict[str, Any]:
        return {
            "job_id": self.id,
            "stage": self.stage,
            "prompt": self.prompt,
            "negative_prompt": self.negative_prompt,
            "mask_url": self.mask_url,
            "user_name": self.user_name,
            "source_task_id": self.source_task_id,
            "image_url": self.image_url or None,
            "isolated_url": self.isolated_url or None,
            "glb_url": self.glb_url or None,
            "video_url": self.video_url or None,
            "error": self.error or None,
            "warning": self.warning or None,
            "telegram_chat_id": self.telegram_chat_id or None,
            "telegram_message_id": self.telegram_message_id or None,
            "delivered": dict(self.delivered or {}),
        }
