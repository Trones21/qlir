"""
Binance data server

This module provides the main entrypoint for running Binance data workers.

"""

from __future__ import annotations
import logging

from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import TypeAlias, Union, Literal
from .endpoints.klines.worker import run_klines_worker
import multiprocessing as mp
import signal
import time
from typing import List
# from .endpoints.uiklines.worker import run_uiklines_worker

from qlir.data.sources.binance.manifest_aggregator import run_manifest_aggregator
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
    endpoint: str = "klines"
    datasource: str = "binance"

@dataclass
class UIKlinesServerConfig(BaseServerConfig):
    worker_type: Literal[WorkerType.UI_KLINES]
    ui_klines: UIKlinesJobConfig
    endpoint: str = "uiklines"
    datasource: str = "binance"

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

    This function acts as a supervisor:
    - starts worker processes
    - starts internal services (delta log / manifest aggregation)
    - manages lifecycle and shutdown

    Args:
        config: BinanceServerConfig
    """
    if config.worker_type is None:
        raise ValueError(
            "You must pass a WorkerType to BinanceServerConfig "
            "(from qlir.data.sources.binance import WorkerType)"
        )

    if config.data_root is None:
        raise TypeError(
            "data_root is None and the server reached the job area which relies "
            "on data_root. Please ensure that you provide a data_root."
        )

    processes: List[mp.Process] = []

    # ----------------------------
    # KLINES
    # ----------------------------
    if config.worker_type == WorkerType.KLINES:
        # Worker (hot path)
        processes.append(
            mp.Process(
                target=_start_klines_worker,
                args=(config.klines, config.data_root),
                name="klines-worker",
            )
        )

        # Delta Log / Manifest Aggregator (cold path)
        processes.append(
            mp.Process(
                target=run_manifest_aggregator,
                args=(config.klines, config.data_root),
                name="klines-manifest-aggregator",
            )
        )

    # ----------------------------
    # UI_KLINES (future)
    # ----------------------------
    # elif config.worker_type == WorkerType.UI_KLINES:
    #     processes.append(...)
    #     processes.append(...)

    if not processes:
        raise RuntimeError(f"No processes were created for worker_type={config.worker_type}")

    # ----------------------------
    # Start processes
    # ----------------------------
    for p in processes:
        p.start()

    # ----------------------------
    # Supervisor loop
    # ----------------------------
    try:
        while True:
            for p in processes:
                if not p.is_alive():
                    raise RuntimeError(
                        f"Child process exited unexpectedly: {p.name} (pid={p.pid})"
                    )
            time.sleep(1)

    except KeyboardInterrupt:
        # Graceful shutdown
        for p in processes:
            if p.is_alive():
                p.terminate()

        for p in processes:
            p.join()


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


