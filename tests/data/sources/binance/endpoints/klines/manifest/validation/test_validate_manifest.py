


from qlir.data.core.paths import get_raw_manifest_path, get_raw_responses_dir_path
from qlir.data.sources.binance.endpoints.klines.manifest.manifest import load_manifest
from qlir.data.sources.binance.endpoints.klines.manifest.validation.orchestrator import validate_manifest_and_fs_integrity
import logging
log = logging.getLogger(__name__)

def test_validate_manifest():
    symbol="SOLUSDT"
    interval="1m"
    limit=1000

    manifest_path = get_raw_manifest_path("binance", endpoint="klines", symbol=symbol,interval=interval, limit=limit)
    response_dir = get_raw_responses_dir_path("binance", endpoint="klines", symbol=symbol,interval=interval, limit=limit)
    manifest = load_manifest(
                manifest_path=manifest_path,
                symbol=symbol,
                interval=interval,
                limit=limit,
            )
    
    validate_manifest_and_fs_integrity(manifest=manifest, response_dir=response_dir)
