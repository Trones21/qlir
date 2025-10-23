# QLIR Visualization Layer

This directory provides a modular visualization framework for the QLIR system — allowing you to quickly **validate**, **inspect**, and **visualize** quantitative feature blocks such as VWAP distance and Bollinger regimes.

---

## 🎯 Purpose
The visualization layer serves three main goals:

1. **Quick Validation**  
   Verify that your feature computations (e.g., range detection, crossover flags) match expectations visually and numerically.
   - Example: *Does my code correctly label range-bound periods?*
   - Example: *Does the full-band crossover logic trigger in the right spots?*

2. **Feature Insight**  
   Observe how computed metrics evolve over time — like VWAP distance or Bollinger width — to refine thresholds and mean-reversion logic.
   - Great for tuning tolerances or identifying recurring patterns.

3. **Feature Overlay on Charts**  
   View your features directly on the price chart to eliminate manual markup (like drawing colored boxes for trends or ranges).
   - Adds regime shading and event markers automatically.

---

## 🧩 Folder Structure

```
qlir/viz/
├── core.py         # Generic view primitives (SeriesSpec, Panel, View, etc.)
├── mpl.py          # Matplotlib renderer for defined Views
├── registry.py     # Global registry for views (used to build dashboards)
├── views/
│   ├── vwap.py     # VWAP distance & z-score visualization
│   └── boll.py     # Bollinger regime and validation visualization
└── __init__.py     # Exports render, register, get
```

---

## ⚙️ Core Concepts

### View Primitives (`core.py`)
Each visualization is made of composable primitives:
- **SeriesSpec** → Plot a column (line, scatter, or bar).
- **BandSpec** → Fill a region (e.g., Bollinger bands or zero lines).
- **EventSpec** → Add vertical markers for events.
- **TableSpec** → Display relevant columns as summary or validation tables.
- **Panel** → A single chart or table.
- **View** → A collection of panels.

---

## 🧠 Feature-Specific Views

### 1. VWAP Distance (`vwap_distance_view`)
**Purpose:** Analyze and visualize how far price moves from VWAP and when mean-reversion tendencies emerge.

**Outputs:**
- Price vs VWAP chart.
- VWAP distance + z-score panel (for overextension detection).
- Table summary (useful for numeric validation).

**Typical Use Cases:**
- Measure average VWAP deviation.
- Tune mean-reversion thresholds.
- Identify regime-dependent VWAP behavior.

```python
from qlir.viz.views.vwap import vwap_distance_view
from qlir.viz.mpl import render

view = vwap_distance_view()
render(view, df)
```

---

### 2. Bollinger Validation (`boll_validation_view`)
**Purpose:** Visually validate Bollinger band features and detect full-band crossovers.

**Outputs:**
- Price with Bollinger upper/mid/lower bands.
- Regime shading (range, uptrend, downtrend).
- Optional event table (for detected full-band crossovers).

**Typical Use Cases:**
- Verify range/trend labeling logic.
- Validate event detection algorithms (crossovers, regime changes).
- Combine with VWAP views to test confluence signals.

```python
from qlir.viz.views.boll import boll_validation_view
from qlir.viz.mpl import render

view = boll_validation_view()
render(view, df)
```

---

## 🧾 Registry Usage
The registry system allows you to dynamically register and retrieve views by name.

```python
from qlir.viz.registry import REGISTRY, register, get

@register("custom_view")
def my_view(...):
    ...

view_fn = get("custom_view")
view = view_fn(...)
render(view, df)
```

You can also build **declarative dashboards** by composing view specs in JSON/YAML:

```yaml
title: "Mean Reversion QA"
views:
  - name: vwap_distance
  - name: boll_validation
```

---

## 🧰 Future Extensions
- **Event highlighting:** Add vertical bands for detected signals.
- **Session overlays:** Add trading-session shading (e.g., Asia, EU, US).
- **Hover interaction:** (Plotly/Bokeh layer integration).

---

## ✅ Summary
This visualization layer is designed for:
- **Rapid feedback loops** during feature engineering.
- **Consistent visualization patterns** across all feature blocks.
- **Minimal coupling** between computation and visualization code.

It lets you iterate on new features visually — the fastest way to spot logical or statistical inconsistencies before moving to backtests or automated runs.
