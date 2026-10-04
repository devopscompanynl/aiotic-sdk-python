"""Settings for an integration service, read from environment variables (and optionally a .env file).

Environment variables (all prefixed ``AIOTIC_``):

======================  ====================================================================
``AIOTIC_BASE_URL``     Tenant base URL, e.g. ``https://acme.aiotic.ai`` (no trailing slash)
``AIOTIC_API_KEY``      Integration key (orders, ERP send, rejected e-mails, …)
``AIOTIC_SYNC_API_KEY`` Optional sync key for ``/customer``, ``/product``, ``/customer-product``
``AIOTIC_TIMEOUT``      HTTP timeout in seconds (default 30)
``AIOTIC_MAX_RETRIES``  Retries for transient failures (default 3)
``AIOTIC_RATE_LIMIT``   Max requests per second the client will issue (default 10; 0 = unlimited)
``AIOTIC_ERP_RECEIVE_KEY``  The key AIOTIC sends in ``X-API-KEY`` to *your* receive endpoint
``AIOTIC_WEBHOOK_KEY``  The key AIOTIC sends in ``X-API-KEY`` to your processing webhook
``AIOTIC_ERP_EVENTS_KEY``  The key *your ERP* sends in ``X-API-KEY`` to ``POST /erp/events`` (defaults to the receive key)
======================  ====================================================================
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path


def _load_dotenv(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    if not path.exists():
        return values
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        value = value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in "\"'":
            value = value[1:-1]
        values[key.strip()] = value
    return values


@dataclass(slots=True)
class Settings:
    """Runtime configuration for the SDK and the integration service."""

    base_url: str = ""
    api_key: str = ""
    sync_api_key: str | None = None
    timeout: float = 30.0
    max_retries: int = 3
    rate_limit: float = 10.0
    erp_receive_key: str | None = None
    webhook_key: str | None = None
    erp_events_key: str | None = None
    user_agent: str = field(default="aiotic-sdk-python")

    @classmethod
    def from_env(cls, dotenv: str | os.PathLike[str] | None = ".env") -> "Settings":
        """Build settings from ``AIOTIC_*`` environment variables, with an optional ``.env`` fallback."""
        env: dict[str, str] = {}
        if dotenv is not None:
            env.update(_load_dotenv(Path(dotenv)))
        env.update({k: v for k, v in os.environ.items() if k.startswith("AIOTIC_")})

        def get(name: str, default: str | None = None) -> str | None:
            return env.get(f"AIOTIC_{name}", default)

        return cls(
            base_url=(get("BASE_URL", "") or "").rstrip("/"),
            api_key=get("API_KEY", "") or "",
            sync_api_key=get("SYNC_API_KEY") or None,
            timeout=float(get("TIMEOUT", "30") or 30),
            max_retries=int(get("MAX_RETRIES", "3") or 3),
            rate_limit=float(get("RATE_LIMIT", "10") or 10),
            erp_receive_key=get("ERP_RECEIVE_KEY") or None,
            webhook_key=get("WEBHOOK_KEY") or None,
            erp_events_key=get("ERP_EVENTS_KEY") or None,
        )

    def require(self) -> "Settings":
        """Raise a helpful error when the minimum configuration is missing."""
        missing = [n for n, v in (("AIOTIC_BASE_URL", self.base_url), ("AIOTIC_API_KEY", self.api_key)) if not v]
        if missing:
            raise ValueError(f"Missing configuration: {', '.join(missing)} (set env vars or run `aiotic init`)")
        return self
