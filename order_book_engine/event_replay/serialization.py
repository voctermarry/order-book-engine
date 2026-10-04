"""Deterministic serialization shared by every replay-layer component.

Responses, snapshot documents and the idempotency log all serialize through
:func:`canonical_json`, so equal inputs always produce byte-identical output
no matter which module produced the content.
"""

from __future__ import annotations

import hashlib
import json


def canonical_json(value: object) -> bytes:
    """Serialize JSON content deterministically.

    Object keys are sorted, separators are compact and no ASCII escaping is
    applied. Integers stay as exact JSON integers; only JSON-native content is
    accepted (the module never produces floats or other lossy types).
    """
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")


def _digest(value: object) -> str:
    return hashlib.sha256(canonical_json(value)).hexdigest()
