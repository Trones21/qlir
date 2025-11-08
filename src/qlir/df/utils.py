import pandas as pd


def union_and_sort(dfs: list[pd.DataFrame], sort_by: list[str] | None = None) -> pd.DataFrame:
    """
    Union multiple DataFrames and return a sorted, deduplicated result.
    """
    df = pd.concat(dfs, ignore_index=True)
    
    # Drop duplicates (optional, common for unions)
    df = df.drop_duplicates().reset_index(drop=True)
    
    # Sort if requested
    if sort_by:
        df = df.sort_values(by=sort_by).reset_index(drop=True)
    
    return df