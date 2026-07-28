class ManifestValidationReport:
    """
    Minimal report, mirroring the Binance report's public surface (fatal/warnings/
    violations + has_fatal). The Binance validator also runs URL-spacing and slice-invariant
    checks that are meaningless for IBKR (no request URL; equities have legitimate gaps),
    so the IBKR validator is intentionally lighter — see orchestrator.py.
    """

    def __init__(self):
        self.fatal: list[str] = []
        self.warnings: list[dict] = []
        self.violations: list[dict] = []

    def has_fatal(self) -> bool:
        return bool(self.fatal)
