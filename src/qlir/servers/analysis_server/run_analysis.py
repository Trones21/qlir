



from qlir.servers.analysis_server.df_materialization.registry import DF_REGISTRY
from qlir.servers.analysis_server.server import get_clean_data
from qlir.servers.analysis_server.analyses.macd.macd_initial import macd_entry

def main():
    base_df = get_clean_data()
    df = macd_entry(base_df)