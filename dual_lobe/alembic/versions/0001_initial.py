"""initial dual-lobe schema: tenants, keys, runs, attempts, ledger, B pipeline, RLS

Revision ID: 0001
Revises:
Create Date: 2026-09-09

"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0001"
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

TABLES = [
    "tenants",
    "api_keys",
    "runs",
    "provider_attempts",
    "events",
    "claims",
    "evidence",
    "b_state",
    "b_jobs",
    "outbox",
]


def upgrade() -> None:
    bind = op.get_bind()
    # Extension for gen_random_uuid() (core since PG13, explicit for safety).
    try:
        op.execute("CREATE EXTENSION IF NOT EXISTS pgcrypto")
    except Exception:
        pass

    op.execute("CREATE SEQUENCE IF NOT EXISTS event_seq")

    op.create_table(
        "tenants",
        sa.Column("id", sa.BigInteger(), autoincrement=True, primary_key=True),
        sa.Column("slug", sa.Text(), nullable=False, unique=True),
        sa.Column("name", sa.Text(), nullable=False, server_default=sa.text("''::text")),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()")),
    )

    op.create_table(
        "api_keys",
        sa.Column("id", sa.BigInteger(), autoincrement=True, primary_key=True),
        sa.Column("tenant_id", sa.BigInteger(), sa.ForeignKey("tenants.id", ondelete="CASCADE"), nullable=False),
        sa.Column("key_hash", sa.Text(), nullable=False, unique=True),
        sa.Column("label", sa.Text(), nullable=False, server_default=sa.text("''::text")),
        sa.Column("scopes", postgresql.ARRAY(sa.Text()), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()")),
        sa.Column("revoked_at", sa.DateTime(timezone=True), nullable=True),
    )

    op.create_table(
        "runs",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True, server_default=sa.text("gen_random_uuid()")),
        sa.Column("tenant_id", sa.BigInteger(), sa.ForeignKey("tenants.id", ondelete="CASCADE"), nullable=False),
        sa.Column("external_run_id", sa.Text(), nullable=False, server_default=sa.text("''::text")),
        sa.Column("status", sa.Text(), nullable=False, server_default=sa.text("'active'::text")),
        sa.Column("goal", sa.Text(), nullable=False, server_default=sa.text("''::text")),
        sa.Column("current_floor", sa.Text(), nullable=False, server_default=sa.text("''::text")),
        sa.Column("current_attempt", sa.Integer(), nullable=False, server_default="1"),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()")),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.text("now()")),
        sa.Column("ended_at", sa.DateTime(timezone=True), nullable=True),
        sa.UniqueConstraint("tenant_id", "external_run_id", name="uq_runs_tenant_external"),
    )

    op.create_table(
        "provider_attempts",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True, server_default=sa.text("gen_random_uuid()")),
        sa.Column("tenant_id", sa.BigInteger(), sa.ForeignKey("tenants.id", ondelete="CASCADE"), nullable=False),
        sa.Column("run_id", postgresql.UUID(as_uuid=True), sa.ForeignKey("runs.id", ondelete="SET NULL"), nullable=True),
        sa.Column("call_id", sa.Text(), nullable=False, server_default=sa.text("''::text")),
        sa.Column("provider_alias", sa.Text(), nullable=False, server_default=sa.text("''::text")),
        sa.Column("logical_model", sa.Text(), nullable=False, server_default=sa.text("''::text")),
        sa.Column("stream", sa.Boolean(), nullable=False, server_default=sa.text("false")),
        sa.Column("status", sa.Text(), nullable=False, server_default=sa.text("'SUCCESS'::text")),
        sa.Column("request_hash", sa.Text(), nullable=False, server_default=sa.text("''::text")),
        sa.Column("latency_ms", sa.Integer(), nullable=True),
        sa.Column("prompt_tokens", sa.Integer(), nullable=True),
        sa.Column("completion_tokens", sa.Integer(), nullable=True),
        sa.Column("error_kind", sa.Text(), nullable=False, server_default=sa.text("''::text")),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()")),
        sa.Column("finished_at", sa.DateTime(timezone=True), nullable=True),
        sa.CheckConstraint("status IN ('SUCCESS','FAILED','INCOMPLETE','TIMEOUT')", name="ck_attempt_status"),
    )

    op.create_table(
        "events",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True, server_default=sa.text("gen_random_uuid()")),
        sa.Column("tenant_id", sa.BigInteger(), sa.ForeignKey("tenants.id", ondelete="CASCADE"), nullable=False),
        sa.Column("run_id", postgresql.UUID(as_uuid=True), sa.ForeignKey("runs.id", ondelete="SET NULL"), nullable=True),
        sa.Column("seq", sa.BigInteger(), nullable=False, server_default=sa.text("nextval('event_seq')")),
        sa.Column("kind", sa.Text(), nullable=False),
        sa.Column("ts", sa.DateTime(timezone=True), server_default=sa.text("now()")),
        sa.Column("actor", sa.Text(), nullable=False, server_default=sa.text("''::text")),
        sa.Column("payload", postgresql.JSONB(), nullable=False, server_default=sa.text("'{}'::jsonb")),
        sa.Column("idempotency_key", sa.Text(), nullable=True, unique=True),
    )

    op.create_table(
        "claims",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True, server_default=sa.text("gen_random_uuid()")),
        sa.Column("tenant_id", sa.BigInteger(), sa.ForeignKey("tenants.id", ondelete="CASCADE"), nullable=False),
        sa.Column("run_id", postgresql.UUID(as_uuid=True), sa.ForeignKey("runs.id", ondelete="CASCADE"), nullable=True),
        sa.Column("source_event_id", postgresql.UUID(as_uuid=True), sa.ForeignKey("events.id", ondelete="SET NULL"), nullable=True),
        sa.Column("claim_text", sa.Text(), nullable=False),
        sa.Column("claim_type", sa.Text(), nullable=False, server_default=sa.text("'generic'::text")),
        sa.Column("severity_weight", sa.SmallInteger(), nullable=False, server_default="1"),
        sa.Column("status", sa.Text(), nullable=False, server_default=sa.text("'claimed'::text")),
        sa.Column("created_by_role", sa.Text(), nullable=False, server_default=sa.text("''::text")),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()")),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.text("now()")),
        sa.Column("verified_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("evidence_ids", postgresql.ARRAY(postgresql.UUID(as_uuid=True)), nullable=False, server_default=sa.text("'{}'::uuid[]")),
        sa.CheckConstraint("severity_weight IN (0,1,2,3,5)", name="ck_claim_severity_weight"),
        sa.CheckConstraint(
            "status IN ('claimed','pending','verified','supported','inferred','unverified','contradicted')",
            name="ck_claim_status",
        ),
    )

    op.create_table(
        "evidence",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True, server_default=sa.text("gen_random_uuid()")),
        sa.Column("tenant_id", sa.BigInteger(), sa.ForeignKey("tenants.id", ondelete="CASCADE"), nullable=False),
        sa.Column("run_id", postgresql.UUID(as_uuid=True), sa.ForeignKey("runs.id", ondelete="CASCADE"), nullable=True),
        sa.Column("claim_id", postgresql.UUID(as_uuid=True), sa.ForeignKey("claims.id", ondelete="SET NULL"), nullable=True),
        sa.Column("kind", sa.Text(), nullable=False, server_default=sa.text("''::text")),
        sa.Column("validation_status", sa.Text(), nullable=False, server_default=sa.text("'PENDING'::text")),
        sa.Column("source", sa.Text(), nullable=False, server_default=sa.text("''::text")),
        sa.Column("artifact_ref", sa.Text(), nullable=False, server_default=sa.text("''::text")),
        sa.Column("artifact_hash", sa.Text(), nullable=False, server_default=sa.text("''::text")),
        sa.Column("tool_name", sa.Text(), nullable=False, server_default=sa.text("''::text")),
        sa.Column("observed_at", sa.DateTime(timezone=True), server_default=sa.text("now()")),
        sa.Column("idempotency_key", sa.Text(), nullable=True, unique=True),
        sa.CheckConstraint("validation_status IN ('PASS','FAIL','PENDING')", name="ck_evidence_status"),
    )

    op.create_table(
        "b_state",
        sa.Column("id", sa.BigInteger(), autoincrement=True, primary_key=True),
        sa.Column("tenant_id", sa.BigInteger(), sa.ForeignKey("tenants.id", ondelete="CASCADE"), nullable=False),
        sa.Column("run_id", postgresql.UUID(as_uuid=True), sa.ForeignKey("runs.id", ondelete="CASCADE"), nullable=False),
        sa.Column("revision", sa.Integer(), nullable=False),
        sa.Column("pulse", sa.Text(), nullable=False, server_default=sa.text("'pre'::text")),
        sa.Column("payload", postgresql.JSONB(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()")),
        sa.UniqueConstraint("tenant_id", "run_id", "revision", name="uq_bstate_tenant_run_rev"),
    )

    op.create_table(
        "b_jobs",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True, server_default=sa.text("gen_random_uuid()")),
        sa.Column("tenant_id", sa.BigInteger(), sa.ForeignKey("tenants.id", ondelete="CASCADE"), nullable=False),
        sa.Column("run_id", postgresql.UUID(as_uuid=True), sa.ForeignKey("runs.id", ondelete="CASCADE"), nullable=False),
        sa.Column("kind", sa.Text(), nullable=False),
        sa.Column("job_key", sa.Text(), nullable=False),
        sa.Column("payload", postgresql.JSONB(), nullable=False),
        sa.Column("status", sa.Text(), nullable=False, server_default=sa.text("'pending'::text")),
        sa.Column("attempts", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("max_attempts", sa.Integer(), nullable=False, server_default="5"),
        sa.Column("lock_until", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()")),
        sa.Column("processed_at", sa.DateTime(timezone=True), nullable=True),
        sa.CheckConstraint("status IN ('pending','processing','done','failed')", name="ck_bjob_status"),
        sa.UniqueConstraint("job_key", name="uq_bjobs_key"),
    )

    op.create_table(
        "outbox",
        sa.Column("id", sa.BigInteger(), autoincrement=True, primary_key=True),
        sa.Column("tenant_id", sa.BigInteger(), sa.ForeignKey("tenants.id", ondelete="CASCADE"), nullable=False),
        sa.Column("aggregate", sa.Text(), nullable=False),
        sa.Column("event_key", sa.Text(), nullable=False),
        sa.Column("payload", postgresql.JSONB(), nullable=False),
        sa.Column("retries", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("published_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()")),
        sa.UniqueConstraint("event_key", name="uq_outbox_event_key"),
    )

    op.create_table(
        "provider_registry",
        sa.Column("id", sa.BigInteger(), autoincrement=True, primary_key=True),
        sa.Column("alias", sa.Text(), nullable=False, unique=True),
        sa.Column("kind", sa.Text(), nullable=False, server_default=sa.text("'chat_completions'::text")),
        sa.Column("base_url", sa.Text(), nullable=False, server_default=sa.text("''::text")),
        sa.Column("model", sa.Text(), nullable=False),
        sa.Column("capabilities", postgresql.JSONB(), nullable=False, server_default=sa.text("'{}'::jsonb")),
        sa.Column("enabled", sa.Boolean(), nullable=False, server_default=sa.text("true")),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()")),
    )

    op.create_index("ix_events_run", "events", ["run_id"])
    op.create_index("ix_events_tenant_ts", "events", ["tenant_id", "ts"])
    op.create_index("ix_claims_run", "claims", ["run_id"])
    op.create_index("ix_evidence_run", "evidence", ["run_id"])
    op.create_index("ix_bjobs_status", "b_jobs", ["status"])
    op.create_index("ix_outbox_unpublished", "outbox", ["published_at", "id"])
    op.create_index("ix_attempts_run", "provider_attempts", ["run_id"])

    _enable_rls(bind)


def _enable_rls(bind) -> None:
    # Dedicated non-superuser role used by tenant-scoped (RLS) sessions.
    op.execute(
        """
        DO $$
        BEGIN
            IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'dual_lobe_rls') THEN
                CREATE ROLE dual_lobe_rls LOGIN PASSWORD 'dual_lobe_rls';
            END IF;
        END
        $$;
        """
    )
    op.execute("GRANT CONNECT ON DATABASE %s TO dual_lobe_rls" % bind.engine.url.database)
    op.execute("GRANT USAGE ON SCHEMA public TO dual_lobe_rls")
    op.execute("GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO dual_lobe_rls")
    op.execute("GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO dual_lobe_rls")
    op.execute("GRANT SELECT ON provider_registry TO dual_lobe_rls")

    tenant_id_expr = "current_setting('app.tenant_id', true)::bigint"
    # provider_registry is globally readable; the rest of the data plane is RLS-isolated.
    tenant_tables = [
        "tenants",
        "api_keys",
        "runs",
        "provider_attempts",
        "events",
        "claims",
        "evidence",
        "b_state",
        "b_jobs",
        "outbox",
    ]
    for t in tenant_tables:
        col = "id" if t == "tenants" else "tenant_id"
        op.execute(f"ALTER TABLE {t} ENABLE ROW LEVEL SECURITY")
        op.execute(
            f"DROP POLICY IF EXISTS {t}_tenant ON {t}"
        )
        op.execute(
            f"CREATE POLICY {t}_tenant ON {t} "
            f"FOR ALL USING ({col} = {tenant_id_expr}) "
            f"WITH CHECK ({col} = {tenant_id_expr})"
        )


def downgrade() -> None:
    for t in reversed(TABLES):
        op.execute(f"ALTER TABLE {t} DISABLE ROW LEVEL SECURITY")
        op.execute(f"DROP POLICY IF EXISTS {t}_tenant ON {t}")
    op.drop_table("provider_registry")
    op.drop_table("outbox")
    op.drop_table("b_jobs")
    op.drop_table("b_state")
    op.drop_table("evidence")
    op.drop_table("claims")
    op.drop_table("events")
    op.drop_table("provider_attempts")
    op.drop_table("runs")
    op.drop_table("api_keys")
    op.drop_table("tenants")
    op.execute("DROP SEQUENCE IF EXISTS event_seq")
    op.execute("DROP ROLE IF EXISTS dual_lobe_rls")