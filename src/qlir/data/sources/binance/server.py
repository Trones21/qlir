"""
Binance data server

This module provides the main entrypoint for running Binance data workers.

"""

from __future__ import annotations

import multiprocessing as mp
import time
from typing import List

# from .endpoints.uiklines.worker import run_uiklines_worker
from qlir.data.sources.binance.manifest_delta_service import run_manifest_delta_service
from qlir.data.sources.binance.server_config_models import (
    BinanceServerConfig,
    WorkerType,
    start_klines_worker,
)

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
                target=start_klines_worker,
                args=(config, config.data_root),
                name="klines-worker",
            )
        )

        # Delta Log / Manifest Aggregator (cold path)
        processes.append(
            mp.Process(
                target=run_manifest_delta_service,
                args=(config, config.data_root),
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



