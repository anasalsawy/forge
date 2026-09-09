"""SQLAlchemy ORM models for the dual-lobe Postgres source of truth.

All tables with a ``tenant_id`` are RLS-protected. ``provider_registry`` is
globally readable (enabled providers), ``outbox``/``b_jobs`` are control-plane
tables written by the gateway and consumed by the worker.
"""
from __future__ import annotations

import uuid
from datetime import datetime, timezone

from sqlalchemy import (
    BigInteger,
    Boolean,
    CheckConstraint,
    DateTime,
    ForeignKey,
    Integer,
    SmallInteger,
    Text,
    UniqueConstraint,
    func,
    text,
)
from sqlalchemy.dialects.postgresql import ARRAY, JSONB, UUID
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

CLAIM_STATUSES = ("claimed", "pending", "verified", "supported", "inferred", "unverified", "contradicted")
JOB_STATUSES = ("pending", "processing", "done", "failed")
ATTEMPT_STATUSES = ("SUCCESS", "FAILED", "INCOMPLETE", "TIMEOUT")
EVIDENCE_STATUSES = ("PASS", "FAIL", "PENDING")


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


class Base(DeclarativeBase):
    pass


class Tenant(Base):
    __tablename__ = "tenants"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    slug: Mapped[str] = mapped_column(Text, unique=True, nullable=False)
    name: Mapped[str] = mapped_column(Text, nullable=False, server_default="")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())


class ApiKey(Base):
    __tablename__ = "api_keys"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    tenant_id: Mapped[int] = mapped_column(BigInteger, ForeignKey("tenants.id", ondelete="CASCADE"), nullable=False)
    key_hash: Mapped[str] = mapped_column(Text, unique=True, nullable=False)
    label: Mapped[str] = mapped_column(Text, nullable=False, server_default="")
    scopes: Mapped[list[str]] = mapped_column(ARRAY(Text), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    revoked_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)


class Run(Base):
    __tablename__ = "runs"
    __table_args__ = (UniqueConstraint("tenant_id", "external_run_id", name="uq_runs_tenant_external"),)

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, server_default=text("gen_random_uuid()"))
    tenant_id: Mapped[int] = mapped_column(BigInteger, ForeignKey("tenants.id", ondelete="CASCADE"), nullable=False)
    external_run_id: Mapped[str] = mapped_column(Text, nullable=False, server_default="")
    status: Mapped[str] = mapped_column(Text, nullable=False, server_default="active")
    goal: Mapped[str] = mapped_column(Text, nullable=False, server_default="")
    current_floor: Mapped[str] = mapped_column(Text, nullable=False, server_default="")
    current_attempt: Mapped[int] = mapped_column(Integer, nullable=False, server_default="1")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), onupdate=utcnow)
    ended_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)


class ProviderAttempt(Base):
    __tablename__ = "provider_attempts"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, server_default=text("gen_random_uuid()"))
    tenant_id: Mapped[int] = mapped_column(BigInteger, ForeignKey("tenants.id", ondelete="CASCADE"), nullable=False)
    run_id: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True), ForeignKey("runs.id", ondelete="SET NULL"), nullable=True)
    call_id: Mapped[str] = mapped_column(Text, nullable=False, server_default="")
    provider_alias: Mapped[str] = mapped_column(Text, nullable=False, server_default="")
    logical_model: Mapped[str] = mapped_column(Text, nullable=False, server_default="")
    stream: Mapped[bool] = mapped_column(Boolean, nullable=False, server_default="false")
    status: Mapped[str] = mapped_column(Text, nullable=False, server_default="SUCCESS")
    request_hash: Mapped[str] = mapped_column(Text, nullable=False, server_default="")
    latency_ms: Mapped[int | None] = mapped_column(Integer, nullable=True)
    prompt_tokens: Mapped[int | None] = mapped_column(Integer, nullable=True)
    completion_tokens: Mapped[int | None] = mapped_column(Integer, nullable=True)
    error_kind: Mapped[str] = mapped_column(Text, nullable=False, server_default="")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    __table_args__ = (CheckConstraint("status IN ('SUCCESS','FAILED','INCOMPLETE','TIMEOUT')", name="ck_attempt_status"),)


class Event(Base):
    __tablename__ = "events"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, server_default=text("gen_random_uuid()"))
    tenant_id: Mapped[int] = mapped_column(BigInteger, ForeignKey("tenants.id", ondelete="CASCADE"), nullable=False)
    run_id: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True), ForeignKey("runs.id", ondelete="SET NULL"), nullable=True)
    seq: Mapped[int] = mapped_column(BigInteger, primary_key=False, autoincrement=False, server_default=text("nextval('event_seq')"))
    kind: Mapped[str] = mapped_column(Text, nullable=False)
    ts: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    actor: Mapped[str] = mapped_column(Text, nullable=False, server_default="")
    payload: Mapped[dict] = mapped_column(JSONB, nullable=False, server_default=text("'{}'::jsonb"))
    idempotency_key: Mapped[str | None] = mapped_column(Text, unique=True, nullable=True)


class Claim(Base):
    __tablename__ = "claims"
    __table_args__ = (
        CheckConstraint("severity_weight IN (0,1,2,3,5)", name="ck_claim_severity_weight"),
        CheckConstraint(
            "status IN ('claimed','pending','verified','supported','inferred','unverified','contradicted')",
            name="ck_claim_status",
        ),
    )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, server_default=text("gen_random_uuid()"))
    tenant_id: Mapped[int] = mapped_column(BigInteger, ForeignKey("tenants.id", ondelete="CASCADE"), nullable=False)
    run_id: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True), ForeignKey("runs.id", ondelete="CASCADE"), nullable=True)
    source_event_id: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True), ForeignKey("events.id", ondelete="SET NULL"), nullable=True)
    claim_text: Mapped[str] = mapped_column(Text, nullable=False)
    claim_type: Mapped[str] = mapped_column(Text, nullable=False, server_default="generic")
    severity_weight: Mapped[int] = mapped_column(SmallInteger, nullable=False, server_default="1")
    status: Mapped[str] = mapped_column(Text, nullable=False, server_default="claimed")
    created_by_role: Mapped[str] = mapped_column(Text, nullable=False, server_default="")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), onupdate=utcnow)
    verified_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    evidence_ids: Mapped[list[uuid.UUID]] = mapped_column(ARRAY(UUID(as_uuid=True)), nullable=False, server_default=text("'{}'::uuid[]"))


class Evidence(Base):
    __tablename__ = "evidence"
    __table_args__ = (CheckConstraint("validation_status IN ('PASS','FAIL','PENDING')", name="ck_evidence_status"),)

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, server_default=text("gen_random_uuid()"))
    tenant_id: Mapped[int] = mapped_column(BigInteger, ForeignKey("tenants.id", ondelete="CASCADE"), nullable=False)
    run_id: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True), ForeignKey("runs.id", ondelete="CASCADE"), nullable=True)
    claim_id: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True), ForeignKey("claims.id", ondelete="SET NULL"), nullable=True)
    kind: Mapped[str] = mapped_column(Text, nullable=False, server_default="")
    validation_status: Mapped[str] = mapped_column(Text, nullable=False, server_default="PENDING")
    source: Mapped[str] = mapped_column(Text, nullable=False, server_default="")
    artifact_ref: Mapped[str] = mapped_column(Text, nullable=False, server_default="")
    artifact_hash: Mapped[str] = mapped_column(Text, nullable=False, server_default="")
    tool_name: Mapped[str] = mapped_column(Text, nullable=False, server_default="")
    observed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    idempotency_key: Mapped[str | None] = mapped_column(Text, unique=True, nullable=True)


class BState(Base):
    __tablename__ = "b_state"
    __table_args__ = (UniqueConstraint("tenant_id", "run_id", "revision", name="uq_bstate_tenant_run_rev"),)

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    tenant_id: Mapped[int] = mapped_column(BigInteger, ForeignKey("tenants.id", ondelete="CASCADE"), nullable=False)
    run_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("runs.id", ondelete="CASCADE"), nullable=False)
    revision: Mapped[int] = mapped_column(Integer, nullable=False)
    pulse: Mapped[str] = mapped_column(Text, nullable=False, server_default="pre")
    payload: Mapped[dict] = mapped_column(JSONB, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())


class BJob(Base):
    __tablename__ = "b_jobs"
    __table_args__ = (
        CheckConstraint("status IN ('pending','processing','done','failed')", name="ck_bjob_status"),
        UniqueConstraint("job_key", name="uq_bjobs_key"),
    )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, server_default=text("gen_random_uuid()"))
    tenant_id: Mapped[int] = mapped_column(BigInteger, ForeignKey("tenants.id", ondelete="CASCADE"), nullable=False)
    run_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("runs.id", ondelete="CASCADE"), nullable=False)
    kind: Mapped[str] = mapped_column(Text, nullable=False)
    job_key: Mapped[str] = mapped_column(Text, nullable=False)
    payload: Mapped[dict] = mapped_column(JSONB, nullable=False)
    status: Mapped[str] = mapped_column(Text, nullable=False, server_default="pending")
    attempts: Mapped[int] = mapped_column(Integer, nullable=False, server_default="0")
    max_attempts: Mapped[int] = mapped_column(Integer, nullable=False, server_default="5")
    lock_until: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    processed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)


class Outbox(Base):
    __tablename__ = "outbox"
    __table_args__ = (UniqueConstraint("event_key", name="uq_outbox_event_key"),)

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    tenant_id: Mapped[int] = mapped_column(BigInteger, ForeignKey("tenants.id", ondelete="CASCADE"), nullable=False)
    aggregate: Mapped[str] = mapped_column(Text, nullable=False)
    event_key: Mapped[str] = mapped_column(Text, nullable=False)
    payload: Mapped[dict] = mapped_column(JSONB, nullable=False)
    retries: Mapped[int] = mapped_column(Integer, nullable=False, server_default="0")
    published_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())


class ProviderRegistry(Base):
    __tablename__ = "provider_registry"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    alias: Mapped[str] = mapped_column(Text, unique=True, nullable=False)
    kind: Mapped[str] = mapped_column(Text, nullable=False, server_default="chat_completions")
    base_url: Mapped[str] = mapped_column(Text, nullable=False, server_default="")
    model: Mapped[str] = mapped_column(Text, nullable=False)
    capabilities: Mapped[dict] = mapped_column(JSONB, nullable=False, server_default=text("'{}'::jsonb"))
    enabled: Mapped[bool] = mapped_column(Boolean, nullable=False, server_default="true")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())