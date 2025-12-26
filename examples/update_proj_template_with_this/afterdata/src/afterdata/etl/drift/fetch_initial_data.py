import time
from qlir.data.sources.load import candles_from_disk_or_network, DiskOrNetwork
from qlir.data.core.datasource import DataSource
from qlir.data.core.instruments import CanonicalInstrument
from qlir.time.timefreq import TimeFreq
from qlir.time.timeunit import TimeUnit
from afterdata.logging_setup import setup_logging, LogProfile
from qlir.data.sources.drift.fetch.oracleorfill import OracleOrFill


# See logging_setup.py for logging options (LogProfile enum) 
setup_logging(profile=LogProfile.QLIR_INFO)

def get_data(symbols, oracle_or_fill: OracleOrFill):
    day_res = TimeFreq(count=1, unit=TimeUnit.DAY)
    for symbol in symbols:
        candles_from_disk_or_network(disk_or_network=DiskOrNetwork.NETWORK, file_uri=None, datasource=DataSource.DRIFT, symbol=symbol, base_resolution=day_res, oracle_or_fill=oracle_or_fill)

    minute_res = TimeFreq(count=1, unit=TimeUnit.MINUTE)
    for symbol in symbols:
        candles_from_disk_or_network(disk_or_network=DiskOrNetwork.NETWORK, file_uri=None, datasource=DataSource.DRIFT, symbol=symbol, base_resolution=minute_res, oracle_or_fill=oracle_or_fill)


def main():
    print("Fetching data and storing it to the canonical location")
    print("With the current dataset (SOL, BTC, ETH) (1Day, 1Minute) this takes roughly 20 minutes to run")
    print("Starting in 10 seconds...")
    time.sleep(10)
    sol = CanonicalInstrument.SOL_PERP 
    btc = CanonicalInstrument.BTC_PERP
    eth = CanonicalInstrument.ETH_PERP

    symbols = [sol, btc, eth]
    get_data(symbols=symbols, oracle_or_fill=OracleOrFill.oracle)
    get_data(symbols=symbols, oracle_or_fill=OracleOrFill.fill)


if __name__ == "__main__":
    main()