from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    database_url: str = "postgresql+asyncpg://demo:demo@postgres:5432/demo"
    redis_url: str = "redis://redis:6379/0"
    stripe_webhook_secret: str = "whsec_test_secret"

    # "docker" starts real containers, "fake" keeps everything in memory.
    provisioner: str = "docker"
    tenant_image: str = "traefik/whoami:latest"
    tenant_network: str | None = None
    tenant_memory_limit: str = "128m"
    tenant_cpu_limit: float = 0.25  # fraction of one CPU

    lock_ttl_seconds: int = 120

    # Local testing only: accept UNSIGNED webhook payloads and enable the /dev endpoints.
    # Never enable this on a public deployment.
    dev_mode: bool = False
    health_timeout_seconds: float = 60.0

    # Billing lifecycle (demo defaults): freeze after a grace period, purge much later.
    freeze_after_days: int = 7
    purge_after_days: int = 365


@lru_cache
def get_settings() -> Settings:
    return Settings()
