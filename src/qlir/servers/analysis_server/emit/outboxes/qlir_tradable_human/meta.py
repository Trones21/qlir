"""Canonical declaration for the `qlir-tradable-human` outbox.

The package directory name (`qlir_tradable_human`) is a Python module name and must use
underscores. The *outbox name* is the cross-process identifier: it names the
directory under $QLIR_ALERTS_DIR, the key in analysis_outboxes.json, and the
key the notification server routes on. Those two are NOT the same string, so
the outbox name is declared explicitly here rather than derived from the
directory -- deriving it is what previously produced underscore directories
that no notification route could ever match.

See ALERT_OUTBOXES.md (declaration contract) and ALERT_LEVELS.md (taxonomy).
"""

OUTBOX = {
    "name": "qlir-tradable-human",
    "alert_level": "tradable",
    "priority": "medium",
}
