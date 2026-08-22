"""
Notification routing configuration.

Answers two questions, kept deliberately separate (see ALERT_OUTBOXES.md):

  "Which outboxes exist?"   -> analysis_outboxes.json, written by the analysis server
  "Which outboxes do *I* handle, and where do they go?"  -> this config

Config resolution order:
  1. $QLIR_NOTIFICATIONS_CONFIG, if set
  2. ./notifications.toml
  3. no file -> console-only fallback for every declared outbox

The fallback matters: it means a fresh clone can run the notification server,
and the whole pipeline, without creating a single account or bot.

VALIDATION IS SCOPED TO WHAT YOU SELECTED. A sink is only built -- and its
required env vars only demanded -- if some route actually references it. An
unreferenced telegram sink with no token is inert, not an error. A *referenced*
one with no token is a hard startup failure, because you asked for a channel the
server cannot deliver on.
"""

from __future__ import annotations

from dataclasses import dataclass
import json
import logging
import os
from pathlib import Path
from typing import Any, Mapping

from .adapters.base import NotificationAdapter
from .adapters.registry import SinkConfigError, get_sink_type

try:  # py3.11+
    import tomllib
except ModuleNotFoundError:  # py3.10
    import tomli as tomllib  # type: ignore[no-redef]

log = logging.getLogger(__name__)

DEFAULT_CONFIG_FILENAME = "notifications.toml"
CONFIG_ENV_VAR = "QLIR_NOTIFICATIONS_CONFIG"


@dataclass(frozen=True)
class SinkSpec:
    name: str
    type: str
    options: Mapping[str, Any]


@dataclass(frozen=True)
class NotificationConfig:
    sinks: dict[str, SinkSpec]
    routes: dict[str, list[str]]
    source: str  # where this config came from, for logging

    @property
    def referenced_sinks(self) -> set[str]:
        """Sink names actually used by at least one route."""
        return {s for names in self.routes.values() for s in names}


def config_path() -> Path | None:
    """
    Find notifications.toml.

    Order: $QLIR_NOTIFICATIONS_CONFIG, then cwd and each parent directory up to
    the filesystem root, then ~/.qlir/notifications.toml.

    The upward walk is load-bearing, not a nicety. The services are launched from
    wherever is convenient -- start_all_simple.sh runs from src/qlir/servers, tmux
    panes inherit their own cwd -- so a plain `cwd / "notifications.toml"` check
    silently misses a config sitting at the repo root and drops the user into the
    console fallback with no obvious reason why.
    """
    explicit = os.environ.get(CONFIG_ENV_VAR)
    if explicit:
        return Path(explicit).expanduser()

    cwd = Path.cwd().resolve()
    for directory in (cwd, *cwd.parents):
        candidate = directory / DEFAULT_CONFIG_FILENAME
        if candidate.exists():
            return candidate

    home = Path.home() / ".qlir" / DEFAULT_CONFIG_FILENAME
    return home if home.exists() else None


def console_fallback(outbox_names: list[str]) -> NotificationConfig:
    """Route every known outbox to a single console sink."""
    return NotificationConfig(
        sinks={"console": SinkSpec(name="console", type="console", options={})},
        routes={name: ["console"] for name in sorted(outbox_names)},
        source="built-in console fallback (no config file found)",
    )


def parse_config(raw: Mapping[str, Any], *, source: str) -> NotificationConfig:
    sinks_raw = raw.get("sinks")
    if not isinstance(sinks_raw, Mapping) or not sinks_raw:
        raise SinkConfigError(
            f"{source}: config must define at least one sink under [sinks.<name>]"
        )

    sinks: dict[str, SinkSpec] = {}
    for name, body in sinks_raw.items():
        if not isinstance(body, Mapping):
            raise SinkConfigError(f"{source}: [sinks.{name}] must be a table")
        type_name = body.get("type")
        if not type_name:
            raise SinkConfigError(f"{source}: [sinks.{name}] is missing 'type'")
        get_sink_type(type_name)  # fail fast on an unknown type, even if unreferenced
        options = {k: v for k, v in body.items() if k != "type"}
        options["_name"] = name
        sinks[name] = SinkSpec(name=name, type=type_name, options=options)

    routes_raw = raw.get("routes")
    if not isinstance(routes_raw, Mapping) or not routes_raw:
        raise SinkConfigError(
            f"{source}: config must define at least one route under [routes]"
        )

    routes: dict[str, list[str]] = {}
    for outbox, sink_names in routes_raw.items():
        if isinstance(sink_names, str):
            sink_names = [sink_names]
        if not isinstance(sink_names, list) or not sink_names:
            raise SinkConfigError(
                f"{source}: routes.{outbox!r} must be a non-empty list of sink names"
            )
        unknown = [s for s in sink_names if s not in sinks]
        if unknown:
            raise SinkConfigError(
                f"{source}: routes.{outbox!r} references undefined sink(s) "
                f"{unknown}; defined sinks are {sorted(sinks)}"
            )
        routes[outbox] = list(sink_names)

    return NotificationConfig(sinks=sinks, routes=routes, source=source)


def load_config(*, declared_outboxes: list[str] | None = None) -> NotificationConfig:
    path = config_path()

    if path is None:
        names = declared_outboxes or []
        if not names:
            raise SinkConfigError(
                "No notifications config found and no outboxes have been declared yet.\n"
                f"Either start the analysis server first (it writes analysis_outboxes.json), "
                f"or create {DEFAULT_CONFIG_FILENAME} -- see notifications.example.toml."
            )
        cfg = console_fallback(names)
        log.warning(
            "No %s found; falling back to console-only delivery for %d outbox(es). "
            "Copy notifications.example.toml to %s to configure real transports.",
            DEFAULT_CONFIG_FILENAME, len(cfg.routes), DEFAULT_CONFIG_FILENAME,
        )
        return cfg

    if not path.exists():
        raise SinkConfigError(
            f"{CONFIG_ENV_VAR} points at {path}, which does not exist"
        )

    with path.open("rb") as f:
        raw = tomllib.load(f)

    return parse_config(raw, source=str(path))


def build_adapters(cfg: NotificationConfig) -> dict[str, list[NotificationAdapter]]:
    """
    Build adapters for referenced sinks only.

    Errors are collected across all sinks so one run reports everything that is
    misconfigured, rather than making the user rediscover them one restart at a
    time. Each error carries that sink type's setup steps.
    """
    built: dict[str, NotificationAdapter] = {}
    problems: list[str] = []

    for sink_name in sorted(cfg.referenced_sinks):
        spec = cfg.sinks[sink_name]
        sink_type = get_sink_type(spec.type)

        missing = [k for k in sink_type.required if k not in spec.options]
        if missing:
            problems.append(
                _problem(sink_name, spec.type,
                         f"missing required config key(s): {', '.join(missing)}",
                         sink_type.setup_steps)
            )
            continue

        try:
            built[sink_name] = sink_type.build(spec.options)
        except SinkConfigError as e:
            problems.append(_problem(sink_name, spec.type, str(e), sink_type.setup_steps))
        except Exception as e:  # adapter constructor rejected the options
            problems.append(
                _problem(sink_name, spec.type, f"{type(e).__name__}: {e}", sink_type.setup_steps)
            )

    if problems:
        raise SinkConfigError(
            "Some notification sinks you selected are not usable:\n\n"
            + "\n\n".join(problems)
            + "\n\nOnly sinks referenced by a route are validated. Remove the route, or "
              "finish the setup above."
        )

    return {
        outbox: [built[s] for s in sink_names]
        for outbox, sink_names in cfg.routes.items()
    }


def _problem(sink_name: str, type_name: str, message: str, steps: tuple[str, ...]) -> str:
    lines = [f"  [{sink_name}] (type: {type_name}) {message}"]
    if steps:
        lines.append("    setup:")
        lines.extend(f"      {i}. {s}" for i, s in enumerate(steps, 1))
    return "\n".join(lines)


def read_declared_outboxes(alerts_root: Path) -> dict[str, dict]:
    """
    Read the analysis server's outbox declaration, if it has written one yet.

    Absence is not an error: the notification server may legitimately start
    first, and outboxes like qlir-ops come from ops_watcher, which does not
    declare at all.
    """
    path = alerts_root / "analysis_outboxes.json"
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text())
    except (OSError, json.JSONDecodeError) as e:
        log.warning("Could not read %s: %s", path, e)
        return {}
    return payload.get("outboxes", {})
