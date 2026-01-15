from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Literal, TypeAlias, Union

from qlir.data.sources.binance.endpoints.klines.worker import run_klines_worker
from qlir.data.sources.binance.job_config_models import KlinesJobConfig, UIKlinesJobConfig


# ---------------------------------------------------------------------------
# BinanceServerConfig (Using a union typealias to avoid the optional/none/type:ignore pattern we would encounter if BinanceServerConfig was a single class)
# ---------------------------------------------------------------------------
class WorkerType(str, Enum):
    KLINES = "klines"
    UI_KLINES = "ui_klines"


@dataclass
class BaseServerConfig:
    data_root: Path


@dataclass
class KlinesServerConfig(BaseServerConfig):
    worker_type: Literal[WorkerType.KLINES]
    job_config: KlinesJobConfig
    endpoint: str = "klines"
    datasource: str = "binance"


# ---------------------------------------------------------------------------
# Config to Worker Param Mapping
# ---------------------------------------------------------------------------

def start_klines_worker(server_config: KlinesServerConfig, data_root: Path) -> None:
    """Start the klines worker"""
    run_klines_worker(
            symbol=server_config.job_config.symbol,
            interval=server_config.job_config.interval,
            limit=server_config.job_config.limit,
            data_root=data_root,
        )
    

@dataclass
class UIKlinesServerConfig(BaseServerConfig):
    worker_type: Literal[WorkerType.UI_KLINES]
    job_config: UIKlinesJobConfig
    endpoint: str = "uiklines"
    datasource: str = "binance"


# def _start_uiklines_worker(job_config: UIKlinesJobConfig, data_root: Path) -> None:
#     """Start the klines worker"""
#     # run_uiklines_worker(
#             symbol=job_config.symbol,
#             interval=job_config.interval,
#             limit=job_config.limit,
#             data_root=data_root,
#         )





BinanceServerConfig: TypeAlias = Union[
    KlinesServerConfig,
    UIKlinesServerConfig,
]
