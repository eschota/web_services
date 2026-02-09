from datetime import datetime
from sqlalchemy import Column, Integer, String, Float, DateTime, ForeignKey, Text, Enum as SAEnum
from sqlalchemy.orm import relationship
from database import Base
import enum


class VPSStatus(str, enum.Enum):
    online = "online"
    offline = "offline"
    degraded = "degraded"


class DomainMode(str, enum.Enum):
    whitelist = "whitelist"
    blacklist = "blacklist"


class VPSNode(Base):
    __tablename__ = "vps_nodes"

    id = Column(Integer, primary_key=True, autoincrement=True)
    ip = Column(String(45), nullable=False)
    country = Column(String(10), nullable=False, default="US")
    proxy_port = Column(Integer, nullable=False, default=3128)
    socks_port = Column(Integer, nullable=False, default=1080)
    proxy_username = Column(String(64), nullable=False)
    proxy_password = Column(String(128), nullable=False)
    status = Column(SAEnum(VPSStatus), default=VPSStatus.offline, nullable=False)
    max_capacity_gbps = Column(Float, default=1.0)
    weight = Column(Integer, default=1)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    stats = relationship("VPSStat", back_populates="vps_node", cascade="all, delete-orphan")
    sessions = relationship("Session", back_populates="vps_node", cascade="all, delete-orphan")


class VPSStat(Base):
    __tablename__ = "vps_stats"

    id = Column(Integer, primary_key=True, autoincrement=True)
    vps_id = Column(Integer, ForeignKey("vps_nodes.id"), nullable=False)
    active_connections = Column(Integer, default=0)
    traffic_gbps_current = Column(Float, default=0.0)
    traffic_gbps_last_hour = Column(Float, default=0.0)
    traffic_gbps_avg = Column(Float, default=0.0)
    cpu_load = Column(Float, default=0.0)
    memory_usage = Column(Float, default=0.0)
    timestamp = Column(DateTime, default=datetime.utcnow)

    vps_node = relationship("VPSNode", back_populates="stats")


class Client(Base):
    __tablename__ = "clients"

    id = Column(Integer, primary_key=True, autoincrement=True)
    client_id = Column(String(64), unique=True, nullable=False)
    api_key = Column(String(128), nullable=False)
    name = Column(String(128), default="")
    created_at = Column(DateTime, default=datetime.utcnow)

    domain_rules = relationship("DomainRule", back_populates="client", cascade="all, delete-orphan")
    sessions = relationship("Session", back_populates="client", cascade="all, delete-orphan")


class DomainRule(Base):
    __tablename__ = "domain_rules"

    id = Column(Integer, primary_key=True, autoincrement=True)
    client_id = Column(Integer, ForeignKey("clients.id"), nullable=False)
    domains = Column(Text, default="[]")  # JSON list of domains
    mode = Column(SAEnum(DomainMode), default=DomainMode.whitelist, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    client = relationship("Client", back_populates="domain_rules")


class Session(Base):
    __tablename__ = "sessions"

    id = Column(Integer, primary_key=True, autoincrement=True)
    client_id = Column(Integer, ForeignKey("clients.id"), nullable=False)
    vps_id = Column(Integer, ForeignKey("vps_nodes.id"), nullable=False)
    proxy_host = Column(String(45), nullable=False)
    proxy_port = Column(Integer, nullable=False)
    started_at = Column(DateTime, default=datetime.utcnow)
    ended_at = Column(DateTime, nullable=True)

    client = relationship("Client", back_populates="sessions")
    vps_node = relationship("VPSNode", back_populates="sessions")
