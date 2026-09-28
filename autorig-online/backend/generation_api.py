"""HTTP entry point for "turn my picture into a rigged character".

Kept out of the ordinary task-create endpoint on purpose: that path serves every
existing rig and convert task, and a generation upload has different inputs
(an image, not a model), a different owner rule (an account, because it costs
credits) and a different failure mode. Sharing it would have put all of that in
front of traffic that has nothing to do with generation.
"""
from __future__ import annotations

import os
import uuid
from typing import Optional
from urllib.parse import quote

from fastapi import APIRouter, Depends, File, HTTPException, Request, UploadFile
from sqlalchemy.ext.asyncio import AsyncSession

from generation_tasks import GENERATION_CREDITS, set_generation_meta
from subscription_access import user_has_active_subscription

router = APIRouter()

# Only formats the Hunyuan worker can actually fetch and decode.
ALLOWED_IMAGE_SUFFIXES = (".png", ".jpg", ".jpeg", ".webp")
MAX_IMAGE_BYTES = 25 * 1024 * 1024


def _image_suffix(filename: str) -> str:
    suffix = os.path.splitext(filename or "")[1].lower()
    return suffix if suffix in ALLOWED_IMAGE_SUFFIXES else ""


def register_generation_routes(app, deps: dict) -> None:
    """Wire the routes with the app's own dependencies, avoiding a circular import."""
    get_current_user = deps["get_current_user"]
    get_db = deps["get_db"]
    upload_dir_root = deps["upload_dir"]
    app_url = deps["app_url"]
    create_conversion_task = deps["create_conversion_task"]

    @app.post("/api/generate/from-image")
    async def api_generate_from_image(
        request: Request,
        file: Optional[UploadFile] = File(None),
        user=Depends(get_current_user),
        db: AsyncSession = Depends(get_db),
    ):
        """Upload a picture, get back a task that becomes a model and then a rig."""
        if user is None:
            # Credits live on accounts, so an anonymous caller has nothing to
            # spend; saying so is friendlier than a generic 401 later.
            raise HTTPException(
                status_code=401,
                detail="Sign in to generate a character from an image",
            )
        if file is None:
            raise HTTPException(status_code=400, detail="An image file is required")

        suffix = _image_suffix(file.filename or "")
        if not suffix:
            raise HTTPException(
                status_code=400,
                detail="Unsupported image format. Use PNG, JPG or WEBP.",
            )

        balance = int(getattr(user, "balance_credits", 0) or 0)
        subscription_active = user_has_active_subscription(user)
        if not subscription_active and balance < GENERATION_CREDITS:
            raise HTTPException(
                status_code=402,
                detail=f"Not enough credits: {GENERATION_CREDITS} required, {balance} available",
            )

        token = str(uuid.uuid4())
        target_dir = os.path.join(upload_dir_root, token)
        os.makedirs(target_dir, exist_ok=True)
        filename = f"source{suffix}"
        path = os.path.join(target_dir, filename)
        written = 0
        try:
            with open(path, "wb") as handle:
                while True:
                    chunk = await file.read(1024 * 1024)
                    if not chunk:
                        break
                    written += len(chunk)
                    if written > MAX_IMAGE_BYTES:
                        raise HTTPException(status_code=413, detail="Image too large (max 25MB)")
                    handle.write(chunk)
        except Exception:
            try:
                os.unlink(path)
            except OSError:
                pass
            raise
        if written == 0:
            raise HTTPException(status_code=400, detail="Uploaded image is empty")

        # The Hunyuan worker fetches this itself, so it has to be the public url.
        image_url = f"{app_url}/u/{token}/{quote(filename)}"

        task, error = await create_conversion_task(
            db,
            input_url=image_url,
            task_type="t_pose",
            owner_type="user",
            owner_id=user.email,
            pipeline_kind="generate",
            input_bytes=written,
        )
        if task is None:
            raise HTTPException(status_code=500, detail=error or "Could not create the task")

        # Charge only once the task exists, so a failed creation cannot bill.
        credits_charged = 0 if subscription_active else GENERATION_CREDITS
        if credits_charged:
            user.balance_credits = balance - credits_charged
        set_generation_meta(task, stage="detect", charged=credits_charged)
        await db.commit()
        print(
            f"[Generation] task {task.id} created for {user.email} "
            f"({credits_charged} credits, subscription={subscription_active}, {written} bytes)"
        )
        return {
            "task_id": task.id,
            "status": task.status,
            "credits_charged": credits_charged,
            "credits_remaining": user.balance_credits,
            "subscription_active": subscription_active,
            "progress_url": f"/task?id={task.id}",
        }
