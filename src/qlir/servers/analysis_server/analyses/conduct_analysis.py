

import pandas as _pd

from qlir.servers.analysis_server.analyses.sma import sma


def conduct_analysis(clean_data: _pd.DataFrame):
   df , error = sma(clean_data=clean_data, window=14)
   if error is not False:
      raise RuntimeError("Unknown error")
   
   return df