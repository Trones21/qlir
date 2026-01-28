


import pandas as pd
from qlir.features.boll.block import with_boll_feature_block


def boll_entry(clean_data: pd.DataFrame):

    df = with_boll_feature_block(df=clean_data)
    return df 



def boll_signal(df):
    NotImplementedError("This is just a placeholder, its a place where we might emit a boll specific signal")