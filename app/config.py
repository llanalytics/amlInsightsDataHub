from __future__ import annotations

import os
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parent.parent


def _resolve_path(raw: str, default_rel: str) -> Path:
    val = raw.strip() if raw else default_rel
    p = Path(val)
    if not p.is_absolute():
        p = BASE_DIR / p
    return p


DATABASE_URL = os.getenv("DATA_HUB_DATABASE_URL", "sqlite:///./data_hub.db")
LANDING_DIR = _resolve_path(os.getenv("DATA_HUB_LANDING_DIR", "./data/landing"), "./data/landing")
PROCESSED_DIR = _resolve_path(os.getenv("DATA_HUB_PROCESSED_DIR", "./data/processed"), "./data/processed")
REJECTED_DIR = _resolve_path(os.getenv("DATA_HUB_REJECTED_DIR", "./data/rejected"), "./data/rejected")
JOB_PAGE_SIZE = int(os.getenv("DATA_HUB_JOB_PAGE_SIZE", "50"))
