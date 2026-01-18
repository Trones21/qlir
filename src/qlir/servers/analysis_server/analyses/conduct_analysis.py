

import pandas as _pd

from qlir.servers.analysis_server.analyses.distance_dist import distance_distributions
from qlir.servers.analysis_server.analyses.sma import sma
from qlir.servers.analysis_server.analyses.sma_14.execution_analyses import execution_analysis
from qlir.servers.analysis_server.analyses.sma_14.mfe import mfe_analysis

def conduct_analysis(clean_data: _pd.DataFrame):


   # mfe_analysis(clean_data)
   distance_distributions(clean_data)
   # execution_analysis(clean_data)
   raise NotImplementedError()

   df , error = sma(clean_data=clean_data, window=14)
   if error is not False:
      raise RuntimeError("Unknown error")
   
   return df