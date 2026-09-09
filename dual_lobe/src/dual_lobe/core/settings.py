"""Central configuration for the dual-lobe inference proxy.

Reads from environment or a .env file. ``DATABASE_URL`` is the admin/owner
connection (used by migrations, the auth key lookup and the B worker); the
tenant-scoped ``RLS_DATABASE_URL`` is used for request data-plane access under
Postgres Row-Level Security.
"""
from __future__ import annotations

from functools import lru_cache
from typing import Literal

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

RolloutStage = Literal[
    "observation", "context", "integrity-observe", "integrity-intervene", "enforcement"
]
BootstrapMode = Literal["FAST_BOOTSTRAP", "ENRICHED_BOOTSTRAP"]


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=(".env", ".env.local"),
        extra="ignore",
        populate_by_name=True,
    )

    database_url: str = (
        "postgresql+asyncpg://dual_lobe:dual_lobe_dev@localhost:5432/dual_lobe"
    )
    rls_database_url: str | None = None
    redis_url: str = "redis://localhost:6379/0"

    host: str = Field(default="0.0.0.0", validation_alias="DUAL_LOBE_HOST")
    port: int = Field(default=8801, validation_alias="DUAL_LOBE_PORT")
    log_level: str = Field(default="INFO", validation_alias="DUAL_LOBE_LOG_LEVEL")
    otel_enabled: bool = Field(default=False, validation_alias="DUAL_LOBE_OTEL_ENABLED")

    bootstrap_keys: str = Field(default="", validation_alias="DUAL_LOBE_BOOTSTRAP_KEYS")
    seed_tenant_slug: str = Field(default="default", validation_alias="DUAL_LOBE_SEED_TENANT_SLUG")
    seed_tenant_name: str = Field(default="Default Tenant", validation_alias="DUAL_LOBE_SEED_TENANT_NAME")

    a_model: str = Field(default="zai-org/GLM-5.3-Flash", validation_alias="DUAL_LOBE_A_MODEL")
    a_base_url: str = Field(default="https://api.featherless.ai/v1", validation_alias="DUAL_LOBE_A_BASE_URL")
    a_api_key: str = Field(default="", validation_alias="DUAL_LOBE_A_API_KEY")
    a_dialect: str = Field(default="chat_completions", validation_alias="DUAL_LOBE_A_DIALECT")
    b_model: str | None = Field(default=None, validation_alias="DUAL_LOBE_B_MODEL")
    b_base_url: str | None = Field(default=None, validation_alias="DUAL_LOBE_B_BASE_URL")
    b_api_key: str | None = Field(default=None, validation_alias="DUAL_LOBE_B_API_KEY")
    b_dialect: str = Field(default="chat_completions", validation_alias="DUAL_LOBE_B_DIALECT")
    firecrawl_api_key: str | None = Field(default=None, validation_alias="FIRECRAWL_API_KEY")

    rollout_stage: str = Field(default="observation", validation_alias="DUAL_LOBE_ROLLOUT_STAGE")
    bootstrap_mode: str = Field(default="FAST_BOOTSTRAP", validation_alias="DUAL_LOBE_BOOTSTRAP")
    pulse_every: int = Field(default=3, validation_alias="DUAL_LOBE_PULSE_EVERY")
    b_fail_open: bool = Field(default=True, validation_alias="DUAL_LOBE_B_FAIL_OPEN")
    b_rpm_limit: int = Field(default=20, validation_alias="DUAL_LOBE_B_RPM_LIMIT")
    b_spend_units_per_hour: int = Field(default=200, validation_alias="DUAL_LOBE_B_SPEND_UNITS_PER_HOUR")
    max_injection_chars: int = Field(default=7000, validation_alias="DUAL_LOBE_MAX_INJECTION_CHARS")
    max_shadow_input_chars: int = Field(default=30000, validation_alias="DUAL_LOBE_MAX_SHADOW_INPUT_CHARS")

    rpm_limit: int = Field(default=600, validation_alias="DUAL_LOBE_RPM_LIMIT")
    tpm_limit: int = Field(default=120000, validation_alias="DUAL_LOBE_TPM_LIMIT")

    a_retries: int = Field(default=3, validation_alias="DUAL_LOBE_A_RETRIES")
    b_retries: int = Field(default=3, validation_alias="DUAL_LOBE_B_RETRIES")
    gateway_max_request_bytes: int = Field(default=4 * 1024 * 1024, validation_alias="DUAL_LOBE_MAX_REQUEST_BYTES")
    a_timeout: float = Field(default=180.0, validation_alias="DUAL_LOBE_A_TIMEOUT")
    b_timeout: float = Field(default=120.0, validation_alias="DUAL_LOBE_B_TIMEOUT")

    worker_poll_seconds: float = Field(default=1.0, validation_alias="DUAL_LOBE_WORKER_POLL_SECONDS")
    worker_max_concurrency: int = Field(default=8, validation_alias="DUAL_LOBE_WORKER_MAX_CONCURRENCY")
    worker_lock_seconds: int = Field(default=90, validation_alias="DUAL_LOBE_WORKER_LOCK_SECONDS")

    @property
    def rls_url(self) -> str:
        if self.rls_database_url:
            return self.rls_database_url
        return self.database_url

    @property
    def resolved_b_model(self) -> str:
        return self.b_model or self.a_model

    @property
    def resolved_b_base_url(self) -> str:
        return self.b_base_url or self.a_base_url

    @property
    def resolved_b_api_key(self) -> str:
        return self.b_api_key if self.b_api_key is not None else self.a_api_key


@lru_cache
def get_settings() -> Settings:
    return Settings()