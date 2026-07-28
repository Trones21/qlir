"""
Interactive Brokers data server.

Main entrypoint for running IBKR data workers. Mirrors the Binance server: it is a
supervisor that spawns the two decoupled processes (Fetcher + Manifest Builder) and
watches them.
"""

from __future__ import annotations

import multiprocessing as mp
import time
from typing import List

from qlir.data.sources.interactive_brokers.manifest_delta_service import (
    run_manifest_delta_service,
)
from qlir.data.sources.interactive_brokers.server_config_models import (
    IBKRServerConfig,
    WorkerType,
    start_historical_bars_worker,
)


def start_data_server(config: IBKRServerConfig) -> None:
    """
    Start the IBKR data server.

    Supervisor:
    - starts the Fetcher (hot path) and Manifest Builder (cold path) processes
    - manages lifecycle and shutdown
    """
    if config.worker_type is None:
        raise ValueError(
            "You must pass a WorkerType to the server config "
            "(from qlir.data.sources.interactive_brokers.server_config_models import WorkerType)"
        )

    if config.data_root is None:
        raise TypeError(
            "data_root is None and the server reached the job area which relies on data_root."
        )

    processes: List[mp.Process] = []

    if config.worker_type == WorkerType.HISTORICAL_BARS:
        # Fetcher (hot path)
        processes.append(
            mp.Process(
                target=start_historical_bars_worker,
                args=(config, config.data_root),
                name="historical-bars-worker",
            )
        )

        # Manifest Builder / delta-log service (cold path)
        processes.append(
            mp.Process(
                target=run_manifest_delta_service,
                args=(config, config.data_root),
                name="historical-bars-manifest-builder",
            )
        )

    if not processes:
        raise RuntimeError(f"No processes were created for worker_type={config.worker_type}")

    for p in processes:
        p.start()

    try:
        while True:
            for p in processes:
                if not p.is_alive():
                    raise RuntimeError(
                        f"Child process exited unexpectedly: {p.name} (pid={p.pid})"
                    )
            time.sleep(1)

    except KeyboardInterrupt:
        for p in processes:
            if p.is_alive():
                p.terminate()
        for p in processes:
            p.join()
