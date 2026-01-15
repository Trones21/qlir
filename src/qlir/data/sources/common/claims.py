from __future__ import annotations

import logging

log = logging.getLogger(__name__)

from datetime import datetime, timezone
import json
import os
from pathlib import Path
import time
from typing import Optional

# ---------------------------
# Configuration
# ---------------------------

DEFAULT_TTL_SEC = 60.0  # override from caller if needed
CLAIMS_DIRNAME = "claims"


# ---------------------------
# Helpers
# ---------------------------

def now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def ensure_claims_dir(base_dir: Path) -> Path:
    """
    Ensure the claims directory exists and return it.
    """
    claims_dir = base_dir / CLAIMS_DIRNAME
    claims_dir.mkdir(parents=True, exist_ok=True)
    return claims_dir


def claim_path(base_dir: Path, slice_id: str) -> Path:
    """
    Path to the claim file for a given slice.
    """
    return ensure_claims_dir(base_dir) / f"{slice_id}.lock"


def _claim_age_sec(path: Path) -> float:
    """
    Age of the claim file in seconds (based on mtime).
    """
    return time.time() - path.stat().st_mtime


# ---------------------------
# Core API
# ---------------------------

def is_claimed(base_dir: Path, slice_id: str) -> bool:
    """
    Return True if a claim exists for this slice.
    """
    return claim_path(base_dir, slice_id).exists()


def is_stale(
    base_dir: Path,
    slice_id: str,
    *,
    ttl_sec: float = DEFAULT_TTL_SEC,
) -> bool:
    """
    Return True if the claim exists and is older than ttl_sec.
    """
    path = claim_path(base_dir, slice_id)
    if not path.exists():
        return False
    try:
        return _claim_age_sec(path) > ttl_sec
    except FileNotFoundError:
        # Raced with delete
        return False


def try_claim(
    base_dir: Path,
    slice_id: str,
    *,
    payload: Optional[dict] = None,
) -> bool:
    """
    Attempt to atomically acquire a claim for this slice.

    Returns:
        True  -> claim acquired
        False -> someone else already owns it
    """
    path = claim_path(base_dir, slice_id)

    flags = os.O_CREAT | os.O_EXCL | os.O_WRONLY
    try:
        fd = os.open(path, flags)
    except FileExistsError:
        return False

    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            data = {
                "slice_id": slice_id,
                "claimed_at": now_utc_iso(),
            }
            if payload:
                data.update(payload)
            json.dump(data, f)
            f.flush()
            os.fsync(f.fileno())

        
        log.debug(
            "Claim acquired | slice_id=%s path=%s",
            slice_id,
            path,
        )
    except Exception:
        # If anything goes wrong, do not leave a half-written claim
        try:
            path.unlink(missing_ok=True)
        finally:
            raise

    return True


def release_claim(base_dir: Path, slice_id: str) -> None:
    """
    Release a claim. Safe to call even if already removed.
    """
    path = claim_path(base_dir, slice_id)
    try:
        path.unlink()
        log.debug(
            "Claim released | slice_id=%s path=%s",
            slice_id,
            path,
        )
    except FileNotFoundError:
        # Already released or never acquired — benign
        log.debug(
            "Claim already absent on release | slice_id=%s path=%s",
            slice_id,
            path,
        )



def reclaim_if_stale(
    base_dir: Path,
    slice_id: str,
    *,
    ttl_sec: float = DEFAULT_TTL_SEC,
) -> bool:
    """
    Attempt to reclaim a stale claim.

    Returns:
        True  -> stale claim reclaimed and caller now owns it
        False -> not stale or someone else won the race
    """
    path = claim_path(base_dir, slice_id)

    if not path.exists():
        return False

    try:
        if _claim_age_sec(path) <= ttl_sec:
            return False
    except FileNotFoundError:
        return False

    # Try to atomically rename the stale claim to a private tombstone
    tombstone = path.with_suffix(f".stale.{os.getpid()}")
    try:
        path.rename(tombstone)
    except FileNotFoundError:
        return False
    except OSError:
        # Another process likely raced us
        return False

    # We successfully stole the claim; replace it with a fresh one
    try:
        return try_claim(
            base_dir,
            slice_id,
            payload={
                "reclaimed_at": now_utc_iso(),
                "reclaimed_by_pid": os.getpid(),
            },
        )
    finally:
        tombstone.unlink(missing_ok=True)


# ---------------------------
# Introspection / Debugging
# ---------------------------

def list_claims(base_dir: Path) -> list[str]:
    """
    Return all active slice_ids with claims.
    Basically ps for our workers
    """
    claims_dir = ensure_claims_dir(base_dir)
    return [
        p.stem
        for p in claims_dir.iterdir()
        if p.is_file() and p.suffix == ".lock"
    ]
