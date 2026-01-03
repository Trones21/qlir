# qlir/df/scalars/units.py

def abs_to_bps(abs_move, ref_price):
    return (abs_move / ref_price) * 10_000


def abs_to_pct(abs_move, ref_price):
    return (abs_move / ref_price) * 100.0
