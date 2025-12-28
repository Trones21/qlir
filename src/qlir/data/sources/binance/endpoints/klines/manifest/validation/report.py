from dataclasses import dataclass, field


@dataclass
class ManifestValidationReport:
    fatal: list[str] = field(default_factory=list)
    warnings: list[dict] = field(default_factory=list)
    violations: list[dict] = field(default_factory=list)

    def has_fatal(self) -> bool:
        return bool(self.fatal)