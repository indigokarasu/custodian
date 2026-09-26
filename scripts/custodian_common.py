#!/usr/bin/env python3
"""custodian_common.py — shared helpers for the ocas-custodian scripts.

Single home for the raw_decode-based JSON stream parser used by the
custodian escalation-loop scripts. Handles both layouts seen in production:
newline-separated JSONL and multiple JSON objects concatenated on one line
(issues.jsonl / journal files are written by several processes and can be
concatenated or carry malformed tails).

Uses C-optimized json.JSONDecoder().raw_decode() instead of a
character-by-character accumulation loop (~9x faster, and correct on strings
that contain brace/bracket characters). On malformed input it skips forward
to the next '{' or '[' and resumes, so one bad record doesn't hide the rest.

Self-check:  python3 custodian_common.py
"""

import json

__all__ = ["parse_issues"]


def parse_issues(text):
    """Parse concatenated / JSONL JSON text into a list of dict records.

    - dict records are returned as-is
    - dicts nested inside top-level JSON lists are flattened into the result
    - malformed segments are skipped up to the next '{' or '[' (never raises
      on bad input; an unparseable tail is ignored)
    """
    if not text:
        return []
    objs = []
    decoder = json.JSONDecoder()
    idx = 0
    length = len(text)
    while idx < length:
        while idx < length and text[idx].isspace():
            idx += 1
        if idx >= length:
            break
        try:
            obj, end = decoder.raw_decode(text, idx)
            if isinstance(obj, dict):
                objs.append(obj)
            elif isinstance(obj, list):
                objs.extend(x for x in obj if isinstance(x, dict))
            elif isinstance(obj, str):
                # Tolerate string-wrapped (double-encoded) records: on
                # 2026-09-25 every issues.jsonl line was string-wrapped and
                # readers silently returned 0 records. Never skip a dict
                # that only got wrapped in a JSON string.
                try:
                    inner = json.loads(obj)
                except Exception:
                    inner = None
                if isinstance(inner, dict):
                    objs.append(inner)
                elif isinstance(inner, list):
                    objs.extend(x for x in inner if isinstance(x, dict))
            idx = end
        except json.JSONDecodeError:
            pos_brace = text.find('{', idx + 1)
            pos_bracket = text.find('[', idx + 1)
            if pos_brace == -1 and pos_bracket == -1:
                break
            if pos_brace == -1:
                idx = pos_bracket
            elif pos_bracket == -1:
                idx = pos_brace
            else:
                idx = min(pos_brace, pos_bracket)
    return objs


if __name__ == "__main__":
    # Self-check: nested braces, braces/brackets inside strings, JSONL,
    # concatenated-on-one-line, top-level list, and recovery from junk.
    assert parse_issues("") == []
    assert parse_issues("   \n  ") == []
    assert parse_issues("no json here") == []
    assert parse_issues('{"a": {"b": {"c": 1}}, "d": 2}') == [
        {"a": {"b": {"c": 1}}, "d": 2}
    ]
    assert parse_issues('{"msg": "got { and } and [ too"}') == [
        {"msg": "got { and } and [ too"}
    ]
    assert parse_issues('{"s": "unbalanced { brace"}') == [
        {"s": "unbalanced { brace"}
    ]
    assert parse_issues('{"a": 1}\n{"b": 2}\n') == [{"a": 1}, {"b": 2}]
    assert parse_issues('{"a": 1}{"b": 2}') == [{"a": 1}, {"b": 2}]
    assert parse_issues('[{"a": 1}, {"b": 2}]') == [{"a": 1}, {"b": 2}]
    assert parse_issues('garbage {"a": 1}') == [{"a": 1}]
    assert parse_issues(json.dumps({"a": 1})) == [{"a": 1}]
    print("custodian_common self-check OK")
