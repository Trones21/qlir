import pandas as pd

from qlir.servers.analysis_server.analyses.path_length import path_length_cols
from qlir.servers.analysis_server.analyses.boll.boll_initial import boll_entry
from qlir.servers.analysis_server.analyses.macd.macd_initial import macd_entry
# 

# examples 
#from qlir.servers.analysis_server.analyses.sma import sma_14_direction
#from qlir.servers.analysis_server.analyses.persistence import sma_survival

# def build_df_sma_14_direction(base_df: pd.DataFrame) -> pd.DataFrame:
#     return sma_14_direction(base_df)

def build_df_path_len_cols(base_df: pd.DataFrame) -> pd.DataFrame:
    return path_length_cols(base_df)

def build_df_boll(base_df: pd.DataFrame) -> pd.DataFrame:
    return boll_entry(base_df)

def build_macd(base_df: pd.DataFrame) -> pd.DataFrame:
    return macd_entry(base_df)