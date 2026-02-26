
# TRIGGER_REGISTRY = {
#     "<trigger_key>": {
#         "name": str,
#         "description": str,

#         # Exactly ONE trigger mechanism (df/column or list[events] & condition/bool logic)

#         "df": str | None,
#         "column": str | None,

#         "events": list[str] | None,
#         "events_condition": "ALL" | "ANY" | "N_OF_M" | None,
#     }
# }

TRIGGER_REGISTRY = {
    "perfect_macd_pyramid_frontside_reversal_point": {
        "type": "df_column", 
        "name": "perfect_macd_pyramid_frontside_reversal_point",
        "description": "When we have a perfect frontside (either direction), and we get the reversal signal. DF granularity determined by the DF",
        "df": "1m_macd_with_pyramids",
        "column": "perfect_frontside_plus_1_light",
    }
}
