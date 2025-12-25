"""
Binance data server

This module provides the main entrypoint for running Binance data workers.

"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import TypeAlias, Union, Literal
from .endpoints.klines.worker import run_klines_worker
# from .endpoints.uiklines.worker import run_uiklines_worker

# ---------------------------------------------------------------------------
# Configuration models
# ---------------------------------------------------------------------------

class WorkerType(str, Enum):
    KLINES = "klines"
    UI_KLINES = "ui_klines"


# ---------------------------------------------------------------------------
# Job Config Classes (specify all the data that the job needs to run)
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class KlinesJobConfig:
    """
    Configuration for a single kline ingestion job.

    One job corresponds to a single (symbol, interval, limit) triplet.
    The worker is responsible for:
      - determining the time range
      - slicing it into KlineSliceKey windows
      - fetching missing slices
      - writing raw responses under the data root
    """
    symbol: str           # e.g. "BTCUSDT"
    interval: str         # e.g. "1s" or "1m"
    limit: int = 1000     # fixed for now in our design

@dataclass(frozen=True)
class UIKlinesJobConfig:
    """Only separate from klines job for clarity/readability reasons"""
    symbol: str           # e.g. "BTCUSDT"
    interval: str         # e.g. "1s" or "1m"
    limit: int = 1000     # fixed for now in our design


# ---------------------------------------------------------------------------
# BinanceServerConfig (Using a union typealias to avoid the optional/none/type:ignore pattern we would encounter if BinanceServerConfig was a single class)
# ---------------------------------------------------------------------------

@dataclass
class BaseServerConfig:
    data_root: Path


@dataclass
class KlinesServerConfig(BaseServerConfig):
    worker_type: Literal[WorkerType.KLINES]
    klines: KlinesJobConfig


@dataclass
class UIKlinesServerConfig(BaseServerConfig):
    worker_type: Literal[WorkerType.UI_KLINES]
    ui_klines: UIKlinesJobConfig


BinanceServerConfig: TypeAlias = Union[
    KlinesServerConfig,
    UIKlinesServerConfig,
]

# ---------------------------------------------------------------------------
# Server entrypoint
# ---------------------------------------------------------------------------

def start_data_server(config: BinanceServerConfig) -> None:
    """
    Start the Binance data server.

    Args:
        config: BinanceServerConfig
    """
    if config.worker_type is None:
        raise ValueError("You must pass a WorkerType to BinanceServerConfig (from qlir.data.sources.binance import WorkerType)")

    # All Workers beneath here rely on the data_root, so we ensure it is not None
    if config.data_root is None:
        raise TypeError("data_root is None and the server reached the job area which rely on data_root. Please ensure that you provide a data_root if your job requires a data_root.")

    if config.worker_type == WorkerType.KLINES:
        _start_klines_worker(config.klines, config.data_root)

    elif config.worker_type == WorkerType.UI_KLINES:
        _start_uiklines_worker(config.ui_klines, config.data_root)


# ---------------------------------------------------------------------------
# Config to Worker Param Mapping
# ---------------------------------------------------------------------------

def _start_klines_worker(job_config: KlinesJobConfig, data_root: Path) -> None:
    """Start the klines worker"""
    run_klines_worker(
            symbol=job_config.symbol,
            interval=job_config.interval,
            limit=job_config.limit,
            data_root=data_root,
        )
    
# def _start_uiklines_worker(job_config: UIKlinesJobConfig, data_root: Path) -> None:
#     """Start the klines worker"""
#     # run_uiklines_worker(
#             symbol=job_config.symbol,
#             interval=job_config.interval,
#             limit=job_config.limit,
#             data_root=data_root,
#         )

