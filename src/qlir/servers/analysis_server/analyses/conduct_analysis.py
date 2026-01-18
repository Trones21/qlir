

import pandas as _pd

from qlir.servers.analysis_server.analyses.distance_dist import distance_distributions
from qlir.servers.analysis_server.analyses.sma import sma
from qlir.servers.analysis_server.analyses.sma_14.execution_analyses import execution_analysis
from qlir.servers.analysis_server.analyses.sma_14.mfe import mfe
from qlir.servers.analysis_server.analyses.sma_14.globals import mae, net_move_to_path_length
def conduct_analysis(clean_data: _pd.DataFrame):

   # Row Filter for Quick Dev Runs
   clean_data = clean_data.iloc[-2000:]
   
   # mfe_analysis(clean_data)
   
   # distance_distributions(clean_data)
   
   mae.mae_dists(clean_data)
   
   #net_move_to_path_length.net_move_to_path_length(clean_data)
   
   # execution_analysis(clean_data)
   
   raise NotImplementedError()

   df , error = sma(clean_data=clean_data, window=14)
   if error is not False:
      raise RuntimeError("Unknown error")
   
   return df