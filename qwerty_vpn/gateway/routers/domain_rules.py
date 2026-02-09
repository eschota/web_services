"""GET/POST /api/domain-rules - manage domain filtering rules per client."""

import json
from datetime import datetime
from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel
from typing import List
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from database import get_db
from auth import verify_api_key
from models import DomainRule, Client, DomainMode

router = APIRouter()


class DomainRulesRequest(BaseModel):
    client_id: str
    domains: List[str]
    mode: str = "whitelist"  # whitelist or blacklist


@router.get("/api/domain-rules")
async def get_domain_rules(
    client_id: str = Query(..., description="Client identifier"),
    db: AsyncSession = Depends(get_db),
    auth: dict = Depends(verify_api_key),
):
    """Get domain rules for a client."""

    # Find client
    client_result = await db.execute(
        select(Client).where(Client.client_id == client_id)
    )
    client = client_result.scalar_one_or_none()

    if not client:
        return {"domains": [], "mode": "whitelist"}

    # Get domain rules
    rules_result = await db.execute(
        select(DomainRule).where(DomainRule.client_id == client.id)
    )
    rule = rules_result.scalar_one_or_none()

    if not rule:
        return {"domains": [], "mode": "whitelist"}

    try:
        domains = json.loads(rule.domains)
    except (json.JSONDecodeError, TypeError):
        domains = []

    return {
        "domains": domains,
        "mode": rule.mode.value if rule.mode else "whitelist",
    }


@router.post("/api/domain-rules")
async def save_domain_rules(
    body: DomainRulesRequest,
    db: AsyncSession = Depends(get_db),
    auth: dict = Depends(verify_api_key),
):
    """Save domain rules for a client."""

    # Validate mode
    if body.mode not in ("whitelist", "blacklist"):
        raise HTTPException(status_code=400, detail="Mode must be 'whitelist' or 'blacklist'")

    # Find or create client
    client_result = await db.execute(
        select(Client).where(Client.client_id == body.client_id)
    )
    client = client_result.scalar_one_or_none()

    if not client:
        raise HTTPException(status_code=404, detail=f"Client '{body.client_id}' not found")

    # Find or create domain rule
    rules_result = await db.execute(
        select(DomainRule).where(DomainRule.client_id == client.id)
    )
    rule = rules_result.scalar_one_or_none()

    mode_enum = DomainMode.whitelist if body.mode == "whitelist" else DomainMode.blacklist

    if rule:
        rule.domains = json.dumps(body.domains)
        rule.mode = mode_enum
        rule.updated_at = datetime.utcnow()
    else:
        rule = DomainRule(
            client_id=client.id,
            domains=json.dumps(body.domains),
            mode=mode_enum,
        )
        db.add(rule)

    await db.commit()

    return {"status": "ok", "domains": body.domains, "mode": body.mode}
