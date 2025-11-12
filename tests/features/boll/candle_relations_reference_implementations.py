import numpy as np
import pandas as pd
import logging
log = logging.getLogger(__name__)
from qlir.utils.pdtools import null_if
from qlir.utils.logdf import logdf

def ref_imp_with_candle_line_relations(
    df: pd.DataFrame,
    *,
    high_col="high",
    low_col="low",
    close_col="close",
    lower_col="boll_lower",
    mid_col="boll_mid",
    upper_col="boll_upper",
    touch_bps: float = 0.001,  # 0.1% of band width as tolerance
    abs_eps: float = 1e-9,     # fallback if width is ~0
    prefix: str = "bb"
) -> pd.DataFrame:
    out = df.copy()

    H, L, C = out[high_col].values, out[low_col].values, out[close_col].values
    lo, md, up = out[lower_col].values, out[mid_col].values, out[upper_col].values

    width = np.maximum(up - lo, 0.0)
    eps = np.maximum(touch_bps * width, abs_eps)

    def three_state(B):
        # touch if range straddles B or close is within eps
        touch = ((L <= B) & (B <= H)) | (np.abs(C - B) <= eps)
        above = (L > B) & ~touch
        below = (H < B) & ~touch
        # map to labels
        state = np.where(touch, "touch",
                 np.where(above, "above", "below"))
        return state

    out[f"{prefix}_lower_state"] = three_state(lo)
    out[f"{prefix}_mid_state"]   = three_state(md)
    out[f"{prefix}_upper_state"] = three_state(up)

    out = null_if(out, "boll_valid", [f"{prefix}_{b}_state" for b in ("lower","mid","upper")])

    # Optional: compsssssact numeric flags {-1,0,1} matching below/touch/above
    # mapping = {"below": -1, "touch": 0, "above": 1}
    # for col in [f"{prefix}_lower_state", f"{prefix}_mid_state", f"{prefix}_upper_state"]:
    #     out[col + "_flag"] = pd.Categorical(out[col], categories=["below","touch","above"])
    #     out[col + "_flag"] = out[col].map(mapping).astype("int8")

    return out

def ref_imp_with_candle_relation_mece(
    df: pd.DataFrame,
    *,
    high_col="high",
    low_col="low",
    close_col="close",
    lower_col="boll_lower",
    mid_col="boll_mid",
    upper_col="boll_upper",
    valid_col="boll_valid",
    touch_bps: float = 0.001,  # 0.1% of band width
    abs_eps: float = 0.0,      # fallback epsilon if width==0
    out_col="boll_position"
) -> pd.DataFrame:
    """
    Adds a single-column categorical feature representing the
    **Mutually Exclusive, Collectively Exhaustive (MECE)** set of possible
    spatial relations between a candle and its Bollinger bands.

    This function collapses the per-band line relations (lower/mid/upper)
    into one consolidated label describing the candle's overall position
    within or beyond the Bollinger structure.

    Why "MECE"?
    -----------
    The relation space between a candle and the three Bollinger lines
    is finite and fully partitionable — every candle must fall into
    exactly one of these categories (mutually exclusive, collectively
    exhaustive). Hence the `_mece` suffix in the function name.

    Possible categories
    -------------------
    Each label corresponds to a distinct spatial relationship:
        - "fully_below"        : Candle lies entirely below the lower band.
        - "touch_lower"        : Candle touches or crosses the lower band.
        - "within_lower_rng"   : Candle entirely between lower and mid.
        - "touch_mid"          : Candle touches or crosses the midline.
        - "within_upper_rng"   : Candle entirely between mid and upper.
        - "touch_upper"        : Candle touches or crosses the upper band.
        - "fully_above"        : Candle lies entirely above the upper band.
        - "touch_lower_mid"    : Candle range touches both lower and mid.
        - "touch_mid_upper"    : Candle range touches both mid and upper.
        - "touch_all"          : Candle range spans all three bands (rare).

    The result is a **finite, complete partition** of the candle→band relation space.

    Returns
    -------
    pd.DataFrame
        Input DataFrame with one additional categorical column:
        - `boll_position` (default name, can be overridden via `out_col`)

    Notes
    -----
    - Rows where `boll_valid == False` are set to NA.
    - The output categorical is ordered logically from fully_below → fully_above.
    - Designed to complement `with_candle_line_relations()`, which emits
      per-band 3-state columns (`below/touch/above`).
    """
    out = df.copy()

    H = out[high_col].values
    L = out[low_col].values
    C = out[close_col].values
    lo = out[lower_col].values
    md = out[mid_col].values
    up = out[upper_col].values

    width = np.maximum(up - lo, 0.0)
    eps = np.maximum(touch_bps * width, abs_eps)

    # --- touches (range crosses band OR close is very near the band)
    t_lo = ((L <= lo) & (lo <= H)) | (np.abs(C - lo) <= eps)
    t_md = ((L <= md) & (md <= H)) | (np.abs(C - md) <= eps)
    t_up = ((L <= up) & (up <= H)) | (np.abs(C - up) <= eps)

    # --- outside / inside ranges (no touch implied)
    fully_below = H < (lo - eps)
    fully_above = L > (up + eps)

    within_lower_rng = (L >= (lo + eps)) & (H <= (md - eps))  # entirely between lower and mid
    within_upper_rng = (L >= (md + eps)) & (H <= (up - eps))  # entirely between mid and upper

    # --- multi-touch bitmask: L=1, M=2, U=4
    mask_bits = (t_lo.astype(int) * 1) + (t_md.astype(int) * 2) + (t_up.astype(int) * 4)

    # --- consolidated label precedence:
    # multi-touch first, then single-touch, then outside/inside buckets
    labels = np.full(len(out), "unknown", dtype=object)

    # multi-touch combos
    labels[mask_bits == 7] = "touch_all"          # lower+mid+upper
    labels[mask_bits == 3] = "touch_lower_mid"    # lower+mid
    labels[mask_bits == 6] = "touch_mid_upper"    # mid+upper

    # single-touch
    labels[(mask_bits == 1)] = "touch_lower"
    labels[(mask_bits == 2)] = "touch_mid"
    labels[(mask_bits == 4)] = "touch_upper"

    # ranges (only set if no touch already assigned)
    unset = labels == "unknown"
    labels[unset & within_lower_rng] = "within_lower_rng"
    labels[unset & within_upper_rng] = "within_upper_rng"

    # outside extremes
    unset = labels == "unknown"
    labels[unset & fully_below] = "fully_below"
    labels[unset & fully_above] = "fully_above"

    # anything still unknown (straddle a boundary without eps-qualified touch): fall back by position of close
    unset = labels == "unknown"
    labels[unset & (C < md)] = "within_lower_rng"
    labels[unset & (C >= md)] = "within_upper_rng"

    # write columns
    out["bb_touch_lower"] = t_lo
    out["bb_touch_mid"]   = t_md
    out["bb_touch_upper"] = t_up
    out["bb_within_lower_rng"] = within_lower_rng
    out["bb_within_upper_rng"] = within_upper_rng
    out["bb_fully_below"] = fully_below
    out["bb_fully_above"] = fully_above

    # categorical with stable ordering (nice for plots & groupby)
    cats = [
        "fully_below",
        "touch_lower",
        "within_lower_rng",
        "touch_lower_mid",
        "touch_mid",
        "touch_mid_upper",
        "within_upper_rng",
        "touch_upper",
        "fully_above",
        "touch_all",   # rare, but keep explicit
    ]
    out[out_col] = pd.Categorical(labels, categories=cats, ordered=True)

    # invalidate where bands aren't valid
    if valid_col in out:
        m = ~out[valid_col].astype(bool).values
        out.loc[m, [out_col,
                    "bb_touch_lower","bb_touch_mid","bb_touch_upper",
                    "bb_within_lower_rng","bb_within_upper_rng",
                    "bb_fully_below","bb_fully_above"]] = pd.NA

    return out

