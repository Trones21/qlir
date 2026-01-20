

import pandas as _pd

from qlir.column_bundles.trend_with_legs import sma_plus_directional_leg_persistence
from qlir.servers.analysis_server.analyses.path_length import path_length_analysis
from qlir.servers.analysis_server.analyses.distance_dist import distance_distributions
from qlir.servers.analysis_server.analyses.sma_14.execution_analyses import execution_analysis
from qlir.servers.analysis_server.analyses.sma_14.mfe import mfe
from qlir.servers.analysis_server.analyses.sma_14.globals import mae, net_move_to_path_length
import logging
log = logging.getLogger(__name__)

def conduct_analysis(clean_data: _pd.DataFrame):

   # Row Filter for Quick Dev Runs
   # clean_data = clean_data.iloc[-2000:]
   
   # path_length_analysis(clean_data)
   dir_dfs = sma_plus_directional_leg_persistence(df=clean_data, window=14)
   up_df = dir_dfs.up.df
   # log.info(dir_dfs.up.new_cols.items())
   leg_id_col = dir_dfs.up.new_cols.get_column("grp_ids_up_legs_col")
   mae.mae_dists(df=up_df, leg_id_col=leg_id_col)
   
   # mfe_analysis(with_necessary_cols)
   # distance_distributions(with_necessary_cols)
   # net_move_to_path_length.net_move_to_path_length(with_necessary_cols)
   # execution_analysis(with_necessary_cols)
   
   raise NotImplementedError()

   df , error = sma(clean_data=clean_data, window=14)
   if error is not False:
      raise RuntimeError("Unknown error")
   
   return df