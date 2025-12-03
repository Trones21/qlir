"""
Binance data server

This module provides the main entrypoint for running Binance data workers.

Typical usage:

    from qlir.data.sources.binance.server import start_data_server
    start_data_server()

By default this will start a kline worker for a small default set of
(symbol, interval) pairs. In real deployments you will likely pass an
explicit config instead of relying on defaults.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Iterable, Sequence, Optional
import threading

# Import the kline worker. We'll define run_klines_worker in
# qlir/data/sources/binance/endpoints/klines/worker.py
from .endpoints.klines.worker import run_klines_worker


# ---------------------------------------------------------------------------
# Configuration models
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


@dataclass
class BinanceServerConfig:
    """
    High-level configuration for the Binance data server.

    Attributes:
        klines_jobs:
            Iterable of KlinesJobConfig entries. Each will be processed
            by a kline worker.

        data_root:
            Optional explicit data root for all datasets. If None,
            lower layers (io / path utilities) should fall back to
            get_data_root() which resolves:
                1. user_root (if provided)
                2. QLIR_DATA_ROOT env var
                3. default ~/qlir_data

        use_threads:
            If True, each job is run in its own thread. If False,
            jobs are run sequentially in the current thread.

        daemon_threads:
            If True and use_threads is enabled, worker threads will be
            started as daemon threads.
    """
    klines_jobs: Sequence[KlinesJobConfig] = field(
        default_factory=lambda: (
            # conservative defaults; callers should usually override
            [KlinesJobConfig(symbol="BTCUSDT", interval="1m")]
        )
    )
    data_root: Optional[str] = None
    use_threads: bool = False
    daemon_threads: bool = True


# ---------------------------------------------------------------------------
# Server entrypoint
# ---------------------------------------------------------------------------

def start_data_server(config: Optional[BinanceServerConfig] = None) -> None:
    """
    Start the Binance data server.

    If no config is provided, a default configuration is used, which
    starts a single kline worker for BTCUSDT @ 1m, limit=1000.

    This function does *not* return if use_threads=False and workers are
    implemented as long-running loops. If use_threads=True, the threads
    are started and this function returns immediately.

    Args:
        config:
            Optional BinanceServerConfig. If None, a default config is
            constructed.
    """
    if config is None:
        config = BinanceServerConfig()

    if not config.klines_jobs:
        # Nothing to do; just return.
        return

    if config.use_threads:
        _start_workers_threaded(config)
    else:
        _start_workers_sequential(config)


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _start_workers_sequential(config: BinanceServerConfig) -> None:
    """
    Run all configured jobs sequentially in the current thread.

    This is the simplest mode and is fine when you only have a small
    number of jobs and/or are just running one symbol+interval.
    """
    for job in config.klines_jobs:
        # The worker is responsible for being a long-running loop.
        # If you want this to be finite (e.g., one pass), you can add
        # a mode parameter to run_klines_worker later.
        run_klines_worker(
            symbol=job.symbol,
            interval=job.interval,
            limit=job.limit,
            data_root=config.data_root,
        )


def _start_workers_threaded(config: BinanceServerConfig) -> None:
    """
    Run all configured jobs in separate threads.

    This is useful if you want to ingest multiple symbols/intervals
    in parallel without switching to asyncio.
    """
    threads: list[threading.Thread] = []

    for job in config.klines_jobs:
        t = threading.Thread(
            target=run_klines_worker,
            kwargs={
                "symbol": job.symbol,
                "interval": job.interval,
                "limit": job.limit,
                "data_root": config.data_root,
            },
            daemon=config.daemon_threads,
            name=f"binance-klines-{job.symbol}-{job.interval}",
        )
        threads.append(t)
        t.start()

    # In threaded mode we *optionally* join threads. For a library-style
    # server it's reasonable to just start daemon threads and return,
    # so callers regain control. If you want blocking behavior, you can
    # uncomment the join loop below or add a flag for it.

    # for t in threads:
    #     t.join()
