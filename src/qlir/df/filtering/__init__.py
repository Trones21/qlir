"""
Filtering utilities: return *subsets* of a DataFrame.

Submodules:
- date: calendar / intraday filters
- session: trading sessions
- events: event-anchored windows
"""

from . import date, events, session
