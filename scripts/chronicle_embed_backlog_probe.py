#!/usr/bin/env python3
"""Read-only backlog probe for chronicle:daily-embed timeout verification.

Prints per-kind unembedded counts (items present in source tables but missing from
memory_vectors), the total vectors present, and the raw facts-table size. Run BEFORE a
live re-run of chronicle_daily_embed.py to prove the run processed real production volume
(not a drained queue). No writes, no network, no side effects.

Usage:
    python3 scripts/chronicle_embed_backlog_probe.py
"""
import sqlite3
import sys

_HELP_ARGS = {"--help", "-h"}
if set(sys.argv[1:]) & _HELP_ARGS:
    print((__doc__ or "").strip() or "Usage: python3 chronicle_embed_backlog_probe.py")
    sys.exit(0)


DB = "~/.hermes/profiles/indigo/commons/db/chronicle/chronicle.db"

KINDS = [
    ("facts",
     "SELECT count(*) FROM facts f LEFT JOIN memory_vectors mv ON mv.belief_id=f.belief_id AND mv.kind='fact' WHERE mv.belief_id IS NULL AND f.status='active'"),
    ("episodes",
     "SELECT count(*) FROM episodes e LEFT JOIN memory_vectors mv ON mv.belief_id=e.belief_id AND mv.kind='episode' WHERE mv.belief_id IS NULL"),
    ("documents",
     "SELECT count(*) FROM documents d LEFT JOIN memory_vectors mv ON mv.belief_id=d.id AND mv.kind='document' WHERE mv.belief_id IS NULL AND d.abstract IS NOT NULL"),
    ("events",
     "SELECT count(*) FROM events e LEFT JOIN memory_vectors mv ON mv.belief_id=e.event_id AND mv.kind='event' WHERE mv.belief_id IS NULL AND e.type='observed' AND json_extract(e.payload,'$.excerpt') IS NOT NULL"),
    ("notes",
     "SELECT count(*) FROM notes n LEFT JOIN memory_vectors mv ON mv.belief_id=n.belief_id AND mv.kind='note' WHERE mv.belief_id IS NULL AND n.status='active' AND n.body IS NOT NULL AND length(n.body)>10 AND n.salience IN ('pinned','high')"),
]

# Capped passes per run (mirrors chronicle_daily_embed.py constants)
CAPS = {"facts": 8000, "episodes": 8000, "documents": 500, "events": 500, "notes": 8000}


def main():
    c = sqlite3.connect(DB)
    cur = c.cursor()
    print("== unembedded (missing from memory_vectors) ==")
    capped_backlog = False
    for kind, q in KINDS:
        n = cur.execute(q).fetchone()[0]
        cap = CAPS.get(kind)
        flag = ""
        if cap and n > cap:
            capped_backlog = True
            flag = "  <-- capped at {}/run; drains across runs".format(cap)
        print("  {}: {}".format(kind, n) + flag)
    total = cur.execute("SELECT count(*) FROM memory_vectors").fetchone()[0]
    print("== total vectors present: {} ==".format(total))
    print("== raw facts table size: {} ==".format(cur.execute("SELECT count(*) FROM facts").fetchone()[0]))
    c.close()
    print()
    if capped_backlog:
        print("VERDICT: live backlog present in a capped pass -> a re-run will process REAL volume.")
    else:
        print("VERDICT: no capped-pass backlog -> a fast re-run could be a drained-queue false-pass; check facts/episodes/notes=0 vs daily volume.")


if __name__ == "__main__":
    main()
