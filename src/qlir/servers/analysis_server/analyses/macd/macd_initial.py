


import pandas as pd
from qlir.indicators.macd import with_macd
from qlir.logging.logdf import logdf

def macd_entry(clean_data: pd.DataFrame) -> pd.DataFrame:

    df = with_macd(df=clean_data)
    logdf(df)
    return df