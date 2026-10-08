# Wake deferral evidence

The deployed Cloudflare comparison and deterministic count evidence are retained on the
[measurement branch](https://github.com/yielded-dev/agent/tree/55d9583f47778765ca080fcc0626bd174c034c49/examples/durable-bench/results/wake-defer),
keeping the raw benchmark artifacts outside the product change.

- [Measurement report](https://github.com/yielded-dev/agent/blob/55d9583f47778765ca080fcc0626bd174c034c49/examples/durable-bench/results/wake-defer/report.md): warm and cold results, repeat spreads, CPU coverage, and limitations.
- [Raw archive manifest](https://github.com/yielded-dev/agent/blob/55d9583f47778765ca080fcc0626bd174c034c49/examples/durable-bench/results/wake-defer/raw-manifest.json) and [offline replay instructions](https://github.com/yielded-dev/agent/blob/55d9583f47778765ca080fcc0626bd174c034c49/examples/durable-bench/results/wake-defer/README.md).
- [Verified Cloudflare cleanup](https://github.com/yielded-dev/agent/blob/55d9583f47778765ca080fcc0626bd174c034c49/examples/durable-bench/results/wake-defer/cleanup.json).
- [Validation receipts](https://github.com/yielded-dev/agent/blob/55d9583f47778765ca080fcc0626bd174c034c49/examples/durable-bench/results/wake-defer/validation/summary.json), including the passing `vp run ready` gate.

All 224 measured turns completed; 25 of 28 Objects enter paired summaries after the strict cold-start checks.
The report preserves the excluded observations and all captured failed outcomes.
