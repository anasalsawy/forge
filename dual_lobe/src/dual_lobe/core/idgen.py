from __future__ import annotations

import hashlib
import uuid


def new_id() -> str:
    return str(uuid.uuid4())


def sha256_short(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8", "ignore")).hexdigest()[:32]