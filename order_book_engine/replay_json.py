"""Deterministic serialization shared by the replay layer and snapshots.

Equal inputs always produce byte-identical output: object keys are sorted,
separators are compact and no ASCII escaping is applied.
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
