# Usage Examples

# VWAP per SMA leg
with_vwap_hlc3_grouped(df, groupby="open_sma_14_up_leg_id")

# VWAP per volatility regime
with_vwap_hlc3_grouped(df, groupby="vol_regime")

# VWAP per custom computed bucket
with_vwap_hlc3_grouped(df, groupby=lambda d: d["arp"].round(1))
