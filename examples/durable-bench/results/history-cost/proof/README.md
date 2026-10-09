# Semantic proof

The Prompt restoration differential uses the pinned upstream Prompt decoder/encoder as its oracle. Five valid corpora cover all four roles and seven part types, empty/shorthand content, provider options, defaults, URL/byte files and optional fields. Ten invalid inputs remain rejected. Native marker/prototype-bearing values and re-encoded outputs are equal.

The digest differential compares the actual baseline and candidate `digestRunHistory` functions through BrowserCrypto, as well as their canonical JSON inputs. Twenty-four cases cover those shapes, a complete JSON-compatible corpus, and arbitrary tool params/results (including nested non-JSON values). Ten cases are accepted with exactly equal JSON and SHA-256; fourteen retain the same rejection classification and public error message. Canonical recovery continues to recompute and compare the digest.

These are task-local differential checks, with source hashes retained in the accompanying JSON. They add no committed product test suite. Existing repository recovery, corruption, compaction, continuation and adapter checks passed through the full gate recorded in `../ready.json`.
