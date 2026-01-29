from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal

try:
    import tomllib  # py311+
except ModuleNotFoundError:  # pragma: no cover
    import tomli as tomllib  # type: ignore


Severity = Literal["info", "warning", "critical"]


@dataclass(frozen=True)
class ServiceConfig:
    name: str
    interval_seconds: int
    emit_outbox: Path
    state_path: Path


@dataclass(frozen=True)
class SelfCheckConfig:
    enabled: bool
    proc_cmdline_contains: str
    severity: Severity = "warning"


@dataclass(frozen=True)
class ProcessCheckConfig:
    name: str
    proc_cmdline_contains: str
    severity: Severity = "critical"
    note: str | None = None


@dataclass(frozen=True)
class LogGrowthCheckConfig:
    name: str
    path: Path
    min_growth_bytes: int
    window_hours: int
    severity: Severity = "warning"
    note: str | None = None


@dataclass(frozen=True)
class OpsWatcherConfig:
    service: ServiceConfig
    self_check: SelfCheckConfig | None
    process_checks: list[ProcessCheckConfig]
    log_growth_checks: list[LogGrowthCheckConfig]


def _req(d: dict[str, Any], k: str) -> Any:
    if k not in d:
        raise ValueError(f"Missing required config key: {k}")
    return d[k]


def load_config(path: str | Path) -> OpsWatcherConfig:
    path = Path(path)
    raw = tomllib.loads(path.read_text(encoding="utf-8"))

    svc = raw.get("service", {})
    service = ServiceConfig(
        name=str(svc.get("name", "qlir_ops_watcher")),
        interval_seconds=int(_req(svc, "interval_seconds")),
        emit_outbox=Path(_req(svc, "emit_outbox")),
        state_path=Path(svc.get("state_path", "/tmp/qlir_ops_watcher_state.json")),
    )

    self_cfg: SelfCheckConfig | None = None
    sc = raw.get("self_check")
    if sc is not None and bool(sc.get("enabled", False)):
        self_cfg = SelfCheckConfig(
            enabled=True,
            proc_cmdline_contains=str(_req(sc, "proc_cmdline_contains")),
            severity=str(sc.get("severity", "warning")),  # type: ignore
        )

    pcs: list[ProcessCheckConfig] = []
    for item in raw.get("process_checks", []):
        pcs.append(
            ProcessCheckConfig(
                name=str(_req(item, "name")),
                proc_cmdline_contains=str(_req(item, "proc_cmdline_contains")),
                severity=str(item.get("severity", "critical")),  # type: ignore
                note=item.get("note"),
            )
        )

    lgs: list[LogGrowthCheckConfig] = []
    for item in raw.get("log_growth_checks", []):
        lgs.append(
            LogGrowthCheckConfig(
                name=str(_req(item, "name")),
                path=Path(_req(item, "path")),
                min_growth_bytes=int(_req(item, "min_growth_bytes")),
                window_hours=int(_req(item, "window_hours")),
                severity=str(item.get("severity", "warning")),  # type: ignore
                note=item.get("note"),
            )
        )

    return OpsWatcherConfig(
        service=service,
        self_check=self_cfg,
        process_checks=pcs,
        log_growth_checks=lgs,
    )
