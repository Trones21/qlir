from qlir.data.sources.binance.endpoints.klines.manifest.validation.slice_invariants import canonical_slice_comp_key_from_facts, compute_slice_id_from_facts, extract_facts_from_composite_key, extract_facts_from_manifest, extract_facts_from_requested_url
from qlir.data.sources.binance.endpoints.klines.manifest.validation.violations import ManifestViolation
from qlir.data.sources.binance.endpoints.klines.model import SliceStatus
from qlir.data.sources.binance.intervals import interval_to_ms


def validate_slice_invariants(
    *,
    slice_key: str,
    slice_obj: dict,
    manifest: dict,
) -> list[ManifestViolation]:
    violations: list[ManifestViolation] = []

    raw_status = slice_obj.get("status")
    slice_status = SliceStatus.is_valid(raw_status)
    if not isinstance(slice_status, SliceStatus):
        violations.append(ManifestViolation(
            rule="invalid_slice_status",
            slice_key=slice_key,
            message=f"status must be SliceStatus, got {raw_status!r}",
        ))
        return violations  # nothing else is meaningful

    # ── extract facts ────────────────────────────
    try:
        facts_key = extract_facts_from_composite_key(slice_key)
    except Exception as e:
        violations.append(ManifestViolation(
            rule="slice_key_parse_failed",
            slice_key=slice_key,
            message="Failed to parse composite slice key",
            extra={"error": str(e)},
        ))
        return violations  # cannot proceed further

    try:
        facts_url = extract_facts_from_requested_url(
            slice_obj["requested_url"]
        )
    except Exception as e:
        violations.append(ManifestViolation(
            rule="requested_url_parse_failed",
            slice_key=slice_key,
            message="Failed to parse requested URL",
            extra={"error": str(e)},
        ))
        return violations

    facts_manifest = extract_facts_from_manifest(
        manifest,
        start_time=facts_key.start_time,
    )

    # ── invariant checks ─────────────────────────
    if facts_key != facts_url:
        violations.append(ManifestViolation(
            rule="slice_key_url_mismatch",
            slice_key=slice_key,
            message="Composite key and requested URL disagree",
            extra={
                "from_key": facts_key,
                "from_url": facts_url,
            },
        ))

    if (
        facts_key.symbol != facts_manifest.symbol
        or facts_key.interval != facts_manifest.interval
        or facts_key.limit != facts_manifest.limit
        ):
        violations.append(ManifestViolation(
            rule="slice_manifest_mismatch",
            slice_key=slice_key,
            message="Slice facts disagree with manifest header",
            extra={
                "from_key": facts_key,
                "from_manifest": facts_manifest,
            },
        ))

    interval_ms = interval_to_ms(facts_key.interval)
    if facts_key.start_time % interval_ms != 0:
        violations.append(ManifestViolation(
            rule="interval_misalignment",
            slice_key=slice_key,
            message="Slice start time is not aligned to interval",
            extra={
                "start_time": facts_key.start_time,
                "interval_ms": interval_ms,
            },
        ))

    # ── slice_id invariant ──────────────────────
    slice_id = slice_obj.get("slice_id")
    if slice_id is None:
        violations.append(ManifestViolation(
            rule="slice_id_missing",
            slice_key=slice_key,
            message="slice_id missing from slice object",
            extra={},
        ))
    else:
        expected = compute_slice_id_from_facts(facts_key)
        if slice_id != expected:
            violations.append(ManifestViolation(
                rule="slice_id_hash_mismatch",
                slice_key=slice_key,
                message="slice_id does not match canonical hash derived from slice facts",
                extra={
                    "expected_slice_id": expected,
                    "actual_slice_id": slice_id,
                    "canonical_key": canonical_slice_comp_key_from_facts(facts_key),
                },
            ))

    return violations
