## 2026-07-30 - JSON Parsing Optimization for Concatenated JSONL
**Learning:** Parsing concatenated JSON objects on a single line using a character-by-character string accumulation loop in Python is slow (O(n) string copies) and error-prone with escaped quotes. Using C-optimized `json.JSONDecoder().raw_decode()` streams through multi-object lines in C speed with zero manual string concatenation overhead.
**Action:** Use `json.JSONDecoder().raw_decode()` when parsing streaming or concatenated JSON objects on a single line instead of custom character scanner loops.
