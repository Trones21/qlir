from __future__ import annotations

from pathlib import Path

from qlir.data.agg.engine import AggConfig, run_agg_daemon
from qlir.data.agg.paths import DatasetPaths


def main() -> None:
    # You can wire argparse later; hardcode for now.
    root = Path("/home/tjr/qlir_data/binance/klines")

    symbol = "BTCUSDT"
    interval = "1m"
    limit = "500"

    raw_root = root / "raw" / symbol / interval / f"limit={limit}"
    agg_root = root / "agg" / symbol / interval / f"limit={limit}"

    paths = DatasetPaths(raw_root=raw_root, agg_root=agg_root)

    dataset_meta = {
        "source": "binance",
        "dataset": "klines",
        "symbol": symbol,
        "interval": interval,
        "limit": int(limit),
    }

    cfg = AggConfig(batch_slices=100)

    run_agg_daemon(paths=paths, dataset_meta=dataset_meta, cfg=cfg)

if __name__ == "__main__":
    main()
