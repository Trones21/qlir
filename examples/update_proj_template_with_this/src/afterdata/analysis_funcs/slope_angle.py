import pandas as pd
from qlir.core.ops import temporal

def slope_angle_analysis(df: pd.DataFrame, sma_col: str):
    '''If you want to understand how "slope" of the legs is changing

        Think about how the slope of the track on the rollercoaster:
        1. As you reach the top of the hill the slope DECREASES 
        2. the slope then reaches 0 (flat)
        3. the slope then INCREASES as you transition to the drop
        4. the slope hits a maximum steepness while going down
        5. the slope then DECREASES as you approach the bottom
        6. the slope then reaches 0 (flat)
        7. the slope then INCREASES as you transition to going up  
        8. the slope hits a maximum steepness while going up 
        9. repeat step 1

    '''
    
    # How much change occured 
    df, sma_delta_col = temporal.with_diff(df, cols=sma_col)
    df[sma_delta_col] = df[sma_delta_col].abs()
    
    # How much change occured (another way to get there (no qlir needed)) 
    df["sma_abs_delta"] = (df[sma_col] - df[sma_col].shift(1)).abs() 
    
    # Now compare than amount of change to the amount of change from the previous row

    df["ratio (abs v prev abs)"] = (df["sma_abs_delta"] / df["sma_abs_delta"].shift(1))
    
    
    return 