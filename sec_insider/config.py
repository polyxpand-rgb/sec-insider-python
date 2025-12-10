"""Configuration utilities for the SEC insider project."""
from __future__ import annotations

import os
import importlib
from dataclasses import dataclass
from typing import Optional


def _load_dotenv_optional() -> None:
    """Load .env if python-dotenv is installed."""
    if importlib.util.find_spec("dotenv") is None:  # type: ignore[attr-defined]
        return
    load_dotenv = importlib.import_module("dotenv").load_dotenv  # type: ignore[assignment]
    load_dotenv()


@dataclass
class Config:
    database_url: str
    user_agent: str
    rate_limit_per_second: float = 5.0


def load_config() -> Config:
    """Load configuration from environment variables."""
    _load_dotenv_optional()

    database_url = os.environ.get("DATABASE_URL")
    if not database_url:
        raise RuntimeError("DATABASE_URL is required")

    user_agent = os.environ.get("SEC_USER_AGENT")
    if not user_agent:
        raise RuntimeError("SEC_USER_AGENT is required for SEC API requests")

    rate_limit_raw: Optional[str] = os.environ.get("SEC_RATE_LIMIT_PER_SECOND")
    rate_limit = float(rate_limit_raw) if rate_limit_raw else 5.0

    return Config(
        database_url=database_url,
        user_agent=user_agent,
        rate_limit_per_second=rate_limit,
    )


__all__ = ["Config", "load_config"]