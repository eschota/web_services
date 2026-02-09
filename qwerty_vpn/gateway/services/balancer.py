"""Load balancer for VPS nodes - weighted round-robin with load awareness."""

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from models import VPSNode, VPSStat, VPSStatus
from typing import Optional
import random


class Balancer:
    _rr_index: int = 0

    @classmethod
    async def get_best_vps(
        cls, db: AsyncSession, country: Optional[str] = None
    ) -> Optional[VPSNode]:
        """Select the best VPS node using weighted round-robin + load awareness."""

        # Build query: only online nodes
        query = select(VPSNode).where(VPSNode.status == VPSStatus.online)

        if country:
            query = query.where(VPSNode.country == country.upper())

        result = await db.execute(query)
        nodes = list(result.scalars().all())

        if not nodes:
            return None

        if len(nodes) == 1:
            return nodes[0]

        # Get latest stats for each node
        scored_nodes = []
        for node in nodes:
            stat_result = await db.execute(
                select(VPSStat)
                .where(VPSStat.vps_id == node.id)
                .order_by(VPSStat.timestamp.desc())
                .limit(1)
            )
            stat = stat_result.scalar_one_or_none()

            # Calculate load ratio (0.0 = idle, 1.0 = full)
            if stat and node.max_capacity_gbps > 0:
                load_ratio = stat.traffic_gbps_last_hour / node.max_capacity_gbps
            else:
                load_ratio = 0.0

            # Skip overloaded nodes (>90%)
            if load_ratio > 0.9:
                continue

            # Weight: base weight * (1 - load_ratio)
            effective_weight = node.weight * (1.0 - load_ratio)
            scored_nodes.append((node, effective_weight))

        if not scored_nodes:
            # All nodes overloaded, pick random from original list
            return random.choice(nodes)

        # Weighted selection
        total_weight = sum(w for _, w in scored_nodes)
        if total_weight <= 0:
            return scored_nodes[0][0]

        # Weighted round-robin
        cls._rr_index = (cls._rr_index + 1) % len(scored_nodes)

        # Sort by weight descending, pick by round-robin index within weight groups
        scored_nodes.sort(key=lambda x: x[1], reverse=True)

        # Weighted random for better distribution
        r = random.uniform(0, total_weight)
        cumulative = 0.0
        for node, weight in scored_nodes:
            cumulative += weight
            if r <= cumulative:
                return node

        return scored_nodes[0][0]
