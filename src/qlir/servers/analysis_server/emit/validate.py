import difflib
from typing import Mapping, Any

ALLOWED_TYPES = {"df_column", "events"}
ALLOWED_EVENTS_CONDITIONS = {"ALL", "ANY", "N_OF_M"}

def _is_missing(x: Any) -> bool:
    return x is None

def validate_trigger_registry(reg: Mapping[str, Mapping[str, Any]]) -> None:
    for k, v in reg.items():
        assert isinstance(v, Mapping), f"{k}: entry must be a mapping/dict"

        desc = v.get("description")
        assert isinstance(desc, str) and desc.strip(), f"{k}: missing/empty description"

        t = v.get("type")
        assert isinstance(t, str), f"{k}: missing type"
        assert t in ALLOWED_TYPES, f"{k}: invalid type {t!r} (allowed: {sorted(ALLOWED_TYPES)})"

        if t == "df_column":
            df = v.get("df")
            col = v.get("column")
            assert isinstance(df, str) and df.strip(), f"{k}: df_column type requires non-empty 'df'"
            assert isinstance(col, str) and col.strip(), f"{k}: df_column type requires non-empty 'column'"

            # Enforce other mechanism is absent/None
            assert _is_missing(v.get("events")), f"{k}: df_column type forbids 'events'"
            assert _is_missing(v.get("events_condition")), f"{k}: df_column type forbids 'events_condition'"

        elif t == "events":
            events = v.get("events")
            assert isinstance(events, list) and events, f"{k}: events type requires non-empty list 'events'"
            assert all(isinstance(e, str) and e.strip() for e in events), f"{k}: events must be list[str]"

            cond = v.get("events_condition")
            assert cond in ALLOWED_EVENTS_CONDITIONS, (
                f"{k}: events type requires events_condition in {sorted(ALLOWED_EVENTS_CONDITIONS)}"
            )

            # Enforce other mechanism is absent/None
            assert _is_missing(v.get("df")), f"{k}: events type forbids 'df'"
            assert _is_missing(v.get("column")), f"{k}: events type forbids 'column'"


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
