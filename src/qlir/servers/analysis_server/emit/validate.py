import difflib



def validate_trigger_registry(reg):
    for k, v in reg.items():
        assert "type" in v, f"{k}: missing type"
        assert "description" in v, f"{k}: missing description"


def validate_active_triggers(
    active: list[str],
    registry: dict[str, dict],
) -> None:
    registry_keys = list(registry.keys())

    unknown: dict[str, list[str]] = {}
    seen: set[str] = set()
    dupes: set[str] = set()

    for t in active:
        if t in seen:
            dupes.add(t)
        seen.add(t)

        if t not in registry:
            suggestions = difflib.get_close_matches(
                t,
                registry_keys,
                n=2,
                cutoff=0.6,
            )
            unknown[t] = suggestions

    if unknown or dupes:
        lines: list[str] = []

        if unknown:
            lines.append("Unknown ACTIVE_TRIGGERS:")
            for k, sugg in unknown.items():
                hint = f" (did you mean {', '.join(sugg)}?)" if sugg else ""
                lines.append(f"  - {k}{hint}")

        if dupes:
            lines.append("Duplicate ACTIVE_TRIGGERS:")
            for d in sorted(dupes):
                lines.append(f"  - {d}")

        raise ValueError("\n".join(lines))
