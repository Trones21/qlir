# qlir/data/drift_fill.py
from __future__ import annotations
from typing import Optional, List, Tuple
import pandas as pd

from .drift import old_fetch_drift_candles_all, normalize_drift_resolution_token
from qlir.data.candle_quality import detect_candle_gaps

# map Drift tokens to step seconds
_STEP_SEC = {"1":60,"5":300,"15":900,"60":3600,"240":14400}
def _step_seconds(token: str) -> Optional[int]:
    return _STEP_SEC.get(token)

def _gap_windows(missing_starts: List[pd.Timestamp], token: str) -> List[Tuple[pd.Timestamp, pd.Timestamp]]:
    """
    Collapse individual missing tz_start stamps into continuous windows to fetch.
    For D/W/M we fetch one-by-one (variable length); for minute/hour tokens we batch.
    """
    tok = normalize_drift_resolution_token(token)
    step = _step_seconds(tok)

    if not missing_starts:
        return []

    ms = sorted(pd.to_datetime(missing_starts, utc=True))
    if step is None:
        # D/W/M: variable—fetch each start individually as [start, start] window
        return [(t, t) for t in ms]

    # minute tokens: coalesce consecutive gaps
    out: List[Tuple[pd.Timestamp, pd.Timestamp]] = []
    run_start = ms[0]
    prev = ms[0]
    for t in ms[1:]:
        if int((t - prev).total_seconds()) == step:
            prev = t
            continue
        # commit window [run_start, prev]
        out.append((run_start, prev))
        run_start = t
        prev = t
    out.append((run_start, prev))
    return out

def backfill_gaps_drift(
    df: pd.DataFrame,
    symbol: str,
    token: str,  # Drift token: 1,5,15,60,240,D,W,M
    *,
    session=None,
    chunk_limit: int = 1000,
    include_partial: bool = True,
    sleep_s: float = 0.15,
    timeout: float = 15.0,
) -> pd.DataFrame:
    """
    Detect gaps in df and fetch just those missing ranges from Drift, then merge.
    Does not fabricate data.
    """
    # Ensure sorted & deduped first
    df = df.copy().sort_values("tz_start").drop_duplicates(subset=["tz_start"], keep="last").reset_index(drop=True)

    missing, _ = detect_candle_gaps(df, token=token)
    if not missing:
        print("[candles_fill] ✅ No gaps detected.")
        return df

    print(f"[candles_fill] Found {len(missing)} missing bars — fetching them from Drift…")
    tok = normalize_drift_resolution_token(token)
    windows = _gap_windows(missing, tok)
    pulled: List[pd.DataFrame] = []

    for (wstart, wend) in windows:
        # For variable D/W/M, set end=wstart to fetch that single bar; for minute tokens, use actual window
        start_time = int(wstart.timestamp())
        end_time = int(wend.timestamp())
        # inclusive-safe: add small padding so we cover boundary behaviors
        end_time += 1

        part = fetch_drift_candles_all(
            symbol=symbol,
            resolution=tok,
            start_time=start_time,
            end_time=end_time,
            chunk_limit=chunk_limit,
            session=session,
            timeout=timeout,
            include_partial=include_partial,
            sleep_s=sleep_s,
        )
        if not part.empty:
            pulled.append(part)

    if not pulled:
        print("[candles_fill] ⚠️ No data returned for gaps (API may be missing those bars).")
        return df

    add = pd.concat(pulled, ignore_index=True)
    out = (
        pd.concat([df, add], ignore_index=True)
          .sort_values("tz_start")
          .drop_duplicates(subset=["tz_start"], keep="last")
          .reset_index(drop=True)
    )

    # Re-run gap detection to confirm
    remaining, _ = detect_candle_gaps(out, token=tok)
    if remaining:
        print(f"[candles_fill] ⚠️ {len(remaining)} gaps remain (source truly missing or outside retention).")
    else:
        print("[candles_fill] ✅ All gaps filled from source.")
    return out
