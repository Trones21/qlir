from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Literal, TypeAlias, Union

from qlir.data.sources.interactive_brokers.endpoints.historical_bars.worker import (
    run_historical_bars_worker,
)
from qlir.data.sources.interactive_brokers.job_config_models import HistoricalBarsJobConfig


# ---------------------------------------------------------------------------
# IBKRServerConfig (union typealias — mirrors the Binance pattern)
# ---------------------------------------------------------------------------
class WorkerType(str, Enum):
    HISTORICAL_BARS = "historical_bars"


@dataclass
class BaseServerConfig:
    data_root: Path


@dataclass
class HistoricalBarsServerConfig(BaseServerConfig):
    worker_type: Literal[WorkerType.HISTORICAL_BARS]
    job_config: HistoricalBarsJobConfig
    endpoint: str = "historical_bars"
    datasource: str = "interactive_brokers"


# ---------------------------------------------------------------------------
# Config -> worker param mapping (the multiprocessing target)
# ---------------------------------------------------------------------------

def start_historical_bars_worker(
    server_config: "HistoricalBarsServerConfig", data_root: Path
) -> None:
    """Start the IBKR historical-bars worker (Fetcher)."""
    jc = server_config.job_config
    run_historical_bars_worker(
        data_root=data_root,
        symbol=jc.symbol,
        interval=jc.interval,
        limit=jc.limit,
        sec_type=jc.sec_type,
        exchange=jc.exchange,
        currency=jc.currency,
        primary_exchange=jc.primary_exchange,
        what_to_show=jc.what_to_show,
        use_rth=jc.use_rth,
        host=jc.host,
        port=jc.port,
        client_id=jc.client_id,
    )


IBKRServerConfig: TypeAlias = Union[HistoricalBarsServerConfig]
