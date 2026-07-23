from __future__ import annotations

import hashlib
import json
import math
import secrets
from datetime import datetime, timedelta
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from html import escape
from typing import Any, Optional

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import HTMLResponse
from pydantic import BaseModel
from sqlalchemy import Column, DateTime, ForeignKey, Index, Integer, String, Text, select, update
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.exc import IntegrityError

from config import is_admin_email
from database import Base, Task, User, get_db
from model_sale_emails import send_new_offer_emails, send_offer_accepted_emails


SALE_PRESETS_USD = (20, 50, 100)
SALE_TOKEN_TTL = timedelta(days=14)
SALE_REPEAT_WINDOW = timedelta(hours=24)
SALE_MAX_CENTS = 100_000_000


class ModelSaleOffer(Base):
    __tablename__ = "model_sale_offers"
    __table_args__ = (
        Index("ix_model_sale_offers_task_status", "task_id", "status"),
        Index("ix_model_sale_offers_buyer_created", "buyer_email", "created_at"),
    )

    id = Column(Integer, primary_key=True, autoincrement=True)
    task_id = Column(String(36), ForeignKey("tasks.id"), nullable=False, index=True)
    author_email = Column(String(255), nullable=False, index=True)
    buyer_email = Column(String(255), nullable=False, index=True)
    amount_cents = Column(Integer, nullable=False)
    currency = Column(String(3), nullable=False, default="USD")
    status = Column(String(16), nullable=False, default="pending", index=True)
    accept_token_sha256 = Column(String(64), nullable=False, unique=True, index=True)
    submission_key = Column(String(64), nullable=False, unique=True, index=True)
    token_expires_at = Column(DateTime, nullable=False)
    token_consumed_at = Column(DateTime, nullable=True)
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    accepted_at = Column(DateTime, nullable=True)
    offer_email_audit = Column(Text, nullable=True)
    acceptance_email_audit = Column(Text, nullable=True)


class SaleOfferCreate(BaseModel):
    amount_usd: Any


class SaleAcceptRequest(BaseModel):
    token: str


def _amount_cents(value: Any) -> int:
    if isinstance(value, bool):
        raise HTTPException(status_code=422, detail="amount_usd must be a valid USD amount")
    try:
        amount = Decimal(str(value))
    except (InvalidOperation, ValueError):
        raise HTTPException(status_code=422, detail="amount_usd must be a valid USD amount")
    if not amount.is_finite():
        raise HTTPException(status_code=422, detail="amount_usd must be finite")
    rounded = amount.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
    if rounded != amount:
        raise HTTPException(status_code=422, detail="amount_usd supports at most two decimal places")
    cents = int(rounded * 100)
    if cents < 100:
        raise HTTPException(status_code=422, detail="Minimum offer is $1.00")
    if cents > SALE_MAX_CENTS:
        raise HTTPException(status_code=422, detail="Offer amount is too large")
    return cents


def _token_hash(token: str) -> str:
    return hashlib.sha256(str(token or "").encode("utf-8")).hexdigest()


def _public_offer(offer: ModelSaleOffer) -> dict[str, Any]:
    return {
        "id": offer.id,
        "task_id": offer.task_id,
        "amount_usd": f"{offer.amount_cents / 100:.2f}",
        "currency": offer.currency,
        "status": offer.status,
        "created_at": offer.created_at.isoformat() + "Z",
        "accepted_at": offer.accepted_at.isoformat() + "Z" if offer.accepted_at else None,
    }


def _accept_page(*, token: str, offer: Optional[ModelSaleOffer], error: str = "") -> HTMLResponse:
    if error:
        body = f"<h1>Offer unavailable</h1><p>{escape(error)}</p>"
    elif offer is None:
        body = "<h1>Offer unavailable</h1>"
    else:
        amount = f"${offer.amount_cents / 100:.2f}"
        body = f"""
          <h1>Accept {escape(amount)} offer?</h1>
          <p>Task: {escape(offer.task_id)}</p>
          <p>Buyer: {escape(offer.buyer_email)}</p>
          <p>Confirming records your agreement and notifies AutoRig.online administration. It does not transfer files or payment.</p>
          <form method="post" action="/api/model-sale/accept">
            <input type="hidden" name="token" value="{escape(token)}">
            <button type="submit">Confirm sale agreement</button>
          </form>
        """
    return HTMLResponse(
        f"""<!doctype html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width">
        <title>Model sale offer - AutoRig.online</title><style>
        body{{font-family:Arial,sans-serif;background:#0a0a0f;color:#f5f5fa;padding:40px}}
        main{{max-width:640px;margin:auto;background:#1a1a24;padding:32px;border-radius:16px}}
        button{{background:#6366f1;color:white;border:0;border-radius:8px;padding:14px 22px;font-weight:700;cursor:pointer}}
        </style></head><body><main>{body}</main></body></html>"""
    )


def build_router(get_current_user) -> APIRouter:
    router = APIRouter()

    @router.get("/api/task/{task_id}/sale-offer-state")
    async def sale_offer_state(
        task_id: str,
        request: Request,
        user: Optional[User] = Depends(get_current_user),
        db: AsyncSession = Depends(get_db),
    ):
        task = await db.get(Task, task_id)
        if not task:
            raise HTTPException(status_code=404, detail="Task not found")
        is_admin = bool(user and is_admin_email(user.email))
        is_registered_owner = bool(user and task.owner_type == "user" and task.owner_id == user.email)
        is_anonymous_owner = bool(
            task.owner_type == "anon"
            and request.cookies.get("anon_id")
            and task.owner_id == request.cookies.get("anon_id")
        )
        is_owner = bool(is_registered_owner or is_anonymous_owner)
        can_download = bool(is_admin or is_owner)
        offer_available = bool(
            task.status == "done" and task.owner_type == "user" and bool(task.owner_id) and not can_download
        )
        can_offer = bool(
            user and offer_available
        )
        active_offer = None
        if user:
            result = await db.execute(
                select(ModelSaleOffer)
                .where(
                    ModelSaleOffer.task_id == task_id,
                    ModelSaleOffer.buyer_email == user.email,
                )
                .order_by(ModelSaleOffer.created_at.desc())
                .limit(1)
            )
            row = result.scalar_one_or_none()
            active_offer = _public_offer(row) if row else None
        return {
            "task_id": task_id,
            "authenticated": bool(user),
            "is_owner": is_owner,
            "is_admin": is_admin,
            "can_download": can_download,
            "offer_available": offer_available,
            "can_offer": can_offer,
            "offer_unavailable_reason": (
                "anonymous_author" if task.owner_type != "user"
                else "task_not_ready" if task.status != "done"
                else "owner_or_admin" if can_download
                else None
            ),
            "presets_usd": list(SALE_PRESETS_USD),
            "minimum_usd": 1,
            "active_offer": active_offer,
        }

    @router.post("/api/task/{task_id}/sale-offers", status_code=201)
    async def create_sale_offer(
        task_id: str,
        payload: SaleOfferCreate,
        user: Optional[User] = Depends(get_current_user),
        db: AsyncSession = Depends(get_db),
    ):
        if not user:
            raise HTTPException(status_code=401, detail="Authentication required")
        task = await db.get(Task, task_id)
        if not task:
            raise HTTPException(status_code=404, detail="Task not found")
        if task.status != "done":
            raise HTTPException(status_code=409, detail="Task is not completed")
        if task.owner_type != "user" or not task.owner_id:
            raise HTTPException(status_code=409, detail="Task author cannot receive email offers")
        if task.owner_id == user.email or is_admin_email(user.email):
            raise HTTPException(status_code=403, detail="Owner and administrator cannot offer on this task")
        cents = _amount_cents(payload.amount_usd)
        cutoff = datetime.utcnow() - SALE_REPEAT_WINDOW
        duplicate = await db.execute(
            select(ModelSaleOffer.id).where(
                ModelSaleOffer.task_id == task_id,
                ModelSaleOffer.buyer_email == user.email,
                ModelSaleOffer.amount_cents == cents,
                ModelSaleOffer.created_at >= cutoff,
            ).limit(1)
        )
        if duplicate.scalar_one_or_none() is not None:
            raise HTTPException(status_code=409, detail="The same offer was already sent in the last 24 hours")

        raw_token = secrets.token_urlsafe(32)
        submission_key = hashlib.sha256(
            f"{task_id}\n{user.email.lower()}\n{cents}\n{datetime.utcnow():%Y-%m-%d}".encode("utf-8")
        ).hexdigest()
        offer = ModelSaleOffer(
            task_id=task_id,
            author_email=task.owner_id,
            buyer_email=user.email,
            amount_cents=cents,
            currency="USD",
            status="pending",
            accept_token_sha256=_token_hash(raw_token),
            submission_key=submission_key,
            token_expires_at=datetime.utcnow() + SALE_TOKEN_TTL,
        )
        db.add(offer)
        try:
            await db.commit()
        except IntegrityError:
            await db.rollback()
            raise HTTPException(status_code=409, detail="The same offer was already sent today")
        await db.refresh(offer)
        audit = await send_new_offer_emails(
            task_id=task_id,
            author_email=task.owner_id,
            buyer_email=user.email,
            amount_cents=cents,
            accept_token=raw_token,
        )
        offer.offer_email_audit = json.dumps(audit, ensure_ascii=False)
        await db.commit()
        if not audit.get("author_sent") or not audit.get("admin_sent"):
            raise HTTPException(
                status_code=502,
                detail={"message": "Offer saved, but one or more emails failed", "offer": _public_offer(offer)},
            )
        return {"success": True, "offer": _public_offer(offer)}

    @router.get("/model-sale/accept")
    async def review_sale_offer(token: str, db: AsyncSession = Depends(get_db)):
        result = await db.execute(
            select(ModelSaleOffer).where(ModelSaleOffer.accept_token_sha256 == _token_hash(token))
        )
        offer = result.scalar_one_or_none()
        if not offer:
            return _accept_page(token="", offer=None, error="The link is invalid.")
        if offer.status != "pending" or offer.token_consumed_at:
            return _accept_page(token="", offer=None, error="This offer has already been processed.")
        if offer.token_expires_at < datetime.utcnow():
            return _accept_page(token="", offer=None, error="This offer link has expired.")
        return _accept_page(token=token, offer=offer)

    @router.post("/api/model-sale/accept")
    async def accept_sale_offer(
        request: Request,
        db: AsyncSession = Depends(get_db),
    ):
        content_type = str(request.headers.get("content-type") or "").lower()
        if "application/json" in content_type:
            try:
                raw_token = str((await request.json()).get("token") or "")
            except Exception:
                raw_token = ""
        else:
            try:
                raw_token = str((await request.form()).get("token") or "")
            except Exception:
                raw_token = ""
        if not raw_token:
            raise HTTPException(status_code=422, detail="token is required")
        now = datetime.utcnow()
        token_sha = _token_hash(raw_token)
        result = await db.execute(
            update(ModelSaleOffer)
            .where(
                ModelSaleOffer.accept_token_sha256 == token_sha,
                ModelSaleOffer.status == "pending",
                ModelSaleOffer.token_consumed_at.is_(None),
                ModelSaleOffer.token_expires_at >= now,
            )
            .values(status="accepted", accepted_at=now, token_consumed_at=now)
        )
        if result.rowcount != 1:
            await db.rollback()
            raise HTTPException(status_code=409, detail="Offer is invalid, expired, or already processed")
        await db.commit()
        accepted = await db.execute(
            select(ModelSaleOffer).where(ModelSaleOffer.accept_token_sha256 == token_sha)
        )
        offer = accepted.scalar_one()
        audit = await send_offer_accepted_emails(
            task_id=offer.task_id,
            author_email=offer.author_email,
            buyer_email=offer.buyer_email,
            amount_cents=offer.amount_cents,
        )
        offer.acceptance_email_audit = json.dumps(audit, ensure_ascii=False)
        await db.commit()
        if not audit.get("buyer_sent") or not audit.get("admin_sent"):
            raise HTTPException(
                status_code=502,
                detail={"message": "Offer accepted, but one or more emails failed", "offer": _public_offer(offer)},
            )
        return HTMLResponse(
            "<!doctype html><html><body style='font-family:Arial;padding:40px'><h1>Offer accepted</h1>"
            "<p>AutoRig.online administration and the buyer have been notified. No files were transferred automatically.</p>"
            "</body></html>"
        )

    return router
