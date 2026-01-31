



from qlir.df.granularity.to_row_per_event.to_row_per_event import to_row_per_event
from qlir.servers.analysis_server.df_materialization.registry import DF_REGISTRY
from qlir.servers.analysis_server.server import get_clean_data
from qlir.servers.analysis_server.analyses.macd.macd_initial import macd_entry

def main():
    base_df = get_clean_data()

    # Skip the registry, import the builder directly
    df = macd_entry(base_df)

    # Counts of rows in strict pyramids vs not in strict pyramids
    df.
    # to_row_per_event(df=df, event_id_col="condition_group_id",metrics=[])