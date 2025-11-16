from __future__ import annotations

from dataclasses import dataclass
from enum import Enum, auto
from typing import Any, Optional, Set


class DataSourceKind(Enum):
    """
    High-level classification of data sources.
    """

    EXECUTION_VENUE = auto()   # Drift, Hyperliquid, etc.
    MARKET_DATA = auto()       # Kaiko, CryptoCompare, etc.
    INDEXER = auto()           # Helius, Triton, etc.
    MOCK = auto()              # Internal synthetic/mock data


class DataTier(Enum):
    """
    Access tier for a data source.

    NOTE: This is about DATA ACCESS, not about execution.
    """

    FREE = auto()      # Public/free tier (may be rate-limited)
    PAID = auto()      # Paid tier (normal commercial)
    ENTERPRISE = auto()  # Enterprise-only / higher SLAs / deep history


@dataclass(frozen=True)
class DataSourceSpec:
    """
    Specification for a data source.

    This captures what the source CAN do, not how you use it in a specific
    script or config.
    """

    name: str
    kind: DataSourceKind

    # Capabilities
    supports_candles: bool
    supports_execution: bool
    requires_symbol_map: bool

    # Access model
    supported_tiers: Set[DataTier]
    requires_api_key: bool

    # Optional extra metadata
    description: str = ""


class DataSource(Enum):
    """
    High-level registry of available data sources.

    Enum values are DataSourceSpec instances.
    """

    DRIFT = DataSourceSpec(
        name="drift",
        kind=DataSourceKind.EXECUTION_VENUE,
        supports_candles=True,
        supports_execution=True,
        requires_symbol_map=True,
        supported_tiers={DataTier.FREE},
        requires_api_key=False,
        description="Drift perpetual futures (execution + candles via public API).",
    )

    KAIKO = DataSourceSpec(
        name="kaiko",
        kind=DataSourceKind.MARKET_DATA,
        supports_candles=True,
        supports_execution=False,
        requires_symbol_map=True,
        supported_tiers={DataTier.PAID, DataTier.ENTERPRISE},
        requires_api_key=True,
        description="Kaiko market data (paid multi-venue historical candles).",
    )

    HELIUS = DataSourceSpec(
        name="helius",
        kind=DataSourceKind.INDEXER,
        supports_candles=True,
        supports_execution=False,
        requires_symbol_map=False,  # typically mirrors venue IDs
        supported_tiers={DataTier.FREE, DataTier.PAID},
        requires_api_key=True,
        description="Helius Solana indexer / data infra (RPC, webhooks, etc.).",
    )

    MOCK = DataSourceSpec(
        name="mock",
        kind=DataSourceKind.MOCK,
        supports_candles=True,
        supports_execution=False,
        requires_symbol_map=False,
        supported_tiers={DataTier.FREE},
        requires_api_key=False,
        description="Internal mock/synthetic data for tests and examples.",
    )

    # ---- Convenience accessors ---------------------------------------------

    @property
    def spec(self) -> DataSourceSpec:
        return self.value

    @property
    def name(self) -> str:  # type: ignore[override]
        return self.value.name

    @property
    def kind(self) -> DataSourceKind:
        return self.value.kind

    @property
    def supports_candles(self) -> bool:
        return self.value.supports_candles

    @property
    def supports_execution(self) -> bool:
        """This doesnt matter for anything inside the QLIR research pipeline. The idea is that if something like SPL wanted to leverage QLIR classes your could write a little helper to ensure the passed datasource supports execution"""
        return self.value.supports_execution

    @property
    def requires_symbol_map(self) -> bool:
        return self.value.requires_symbol_map

    @property
    def supported_tiers(self) -> Set[DataTier]:
        return self.value.supported_tiers

    @property
    def requires_api_key(self) -> bool:
        return self.value.requires_api_key

    @property
    def description(self) -> str:
        return self.value.description
