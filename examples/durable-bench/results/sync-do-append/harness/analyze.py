"""Summarize this spike's retained counts and Cloudflare invocation receipts.

Usage: python3 harness/analyze.py <task-results-directory>
No timing workloads run here. Quartiles use linear interpolation (type 7).
"""

import collections
import gzip
import json
import math
import pathlib
import statistics
import sys


root = pathlib.Path(sys.argv[1]).resolve()


def read(path):
    opener = gzip.open if path.suffix == ".gz" else open
    with opener(path, "rt") as source:
        return json.load(source)


def quantile(values, probability):
    values = sorted(values)
    index = (len(values) - 1) * probability
    low = math.floor(index)
    high = math.ceil(index)
    return values[low] + (values[high] - values[low]) * (index - low)


def stats(values):
    if not values:
        return {"n": 0}
    return {
        "n": len(values),
        "median": statistics.median(values),
        "q1": quantile(values, 0.25),
        "q3": quantile(values, 0.75),
        "min": min(values),
        "max": max(values),
        "values": values,
    }


counts = []
for role in ("baseline", "candidate"):
    for history in (50, 250):
        report = read(root / "counts" / f"{role}-{history}" / "report.json.gz")
        turns = report["measured"]["stats"]["kom433"]
        first = turns[0]
        append = first["inclusiveStages"]["durable-object-append"]
        counts.append({
            "role": role,
            "history": history,
            "fingerprint": report["seed"]["fingerprint"],
            "reference": report["seed"]["reference"],
            "bundleSha256": report["bundleSha256"],
            "firstTurn": first["total"],
            "firstAppendStage": append,
            "firstPerAppend": {
                key: append[key] / append["calls"]
                for key in ("evaluations", "allocations", "sqlStatements", "syncTransactions")
            },
            "laterAppendStage": {
                key: stats([
                    row["inclusiveStages"]["durable-object-append"][key]
                    for row in turns[1:]
                ])
                for key in ("evaluations", "allocations", "sqlStatements", "syncTransactions")
            },
            "appendSitePartition": first.get("stageSites", {}).get("durable-object-append"),
            "individualAppends": first["appends"],
            "synchronousCore": first["sites"].get("sync-append.append"),
            "unclosedSites": [row["unclosedSites"] for row in turns],
        })

output = {"quartiles": "type 7, linear interpolation", "counts": counts}
deployed = root / "deployed"
sample_path = deployed / "samples.json"
if sample_path.exists():
    samples = read(sample_path)
    by_key = collections.defaultdict(dict)
    for sample in samples:
        key = (sample["seedRecords"], sample["block"], sample["cohort"], sample["operation"]["phase"])
        if sample["role"] in by_key[key]:
            raise ValueError("Duplicate role for a matched operation")
        by_key[key][sample["role"]] = sample
    if len({sample["invocationId"] for sample in samples}) != len(samples):
        raise ValueError("An invocation was counted more than once")
    triples = {key: row for key, row in by_key.items() if len(row) == 3}
    rows = []
    for seed in (10, 1000):
        for name, phases in (
            ("initial", (1, 2, 3, 4, 5)),
            ("warm-fresh", (7, 8, 10)),
            ("warm-compaction", (6, 9)),
        ):
            pairs = [
                row for key, row in sorted(triples.items())
                if key[0] == seed and key[3] in phases
            ]
            if not pairs:
                continue
            summary = {"seedRecords": seed, "phases": phases, "kind": name}
            for role in ("baseline", "candidate", "control"):
                summary[role] = {
                    metric: stats([row[role][metric] for row in pairs])
                    for metric in ("cpuMs", "workerWallMs", "clientElapsedMs", "ingressCpuMs")
                }
                summary[role]["objectWallTimeMs"] = stats([
                    row[role]["operation"]["objectWallTimeMs"] for row in pairs
                ])
            summary["pairedCandidateRatios"] = stats([
                row["candidate"]["cpuMs"] / row["baseline"]["cpuMs"] for row in pairs
            ])
            summary["pairedControlRatios"] = stats([
                row["control"]["cpuMs"] / row["baseline"]["cpuMs"] for row in pairs
            ])
            summary["candidateToMeanControls"] = stats([
                2 * row["candidate"]["cpuMs"] / (row["baseline"]["cpuMs"] + row["control"]["cpuMs"])
                for row in pairs
            ])
            summary["blocks"] = []
            for block in (0, 1, 2):
                group = [
                    row for key, row in sorted(triples.items())
                    if key[0] == seed and key[1] == block and key[3] in phases
                ]
                if not group:
                    continue
                candidate = stats([row["candidate"]["cpuMs"] / row["baseline"]["cpuMs"] for row in group])
                control = stats([row["control"]["cpuMs"] / row["baseline"]["cpuMs"] for row in group])
                spread = max(abs(1 - control["q1"]), abs(1 - control["q3"]))
                summary["blocks"].append({
                    "block": block,
                    "candidate": candidate,
                    "control": control,
                    "controlSpread": spread,
                    "criterionMet": candidate["median"] <= 0.9 and 1 - candidate["median"] > spread,
                })
            summary["criterionMet"] = len(summary["blocks"]) == 3 and all(row["criterionMet"] for row in summary["blocks"])
            rows.append(summary)
    telemetry_outcomes = collections.Counter()
    telemetry_by_method = collections.defaultdict(list)
    telemetry_invocations = {}
    for path in deployed.glob("block-*/*/telemetry-*.json"):
        for event in read(path).get("events", []):
            telemetry_invocations[event["id"]] = event
    for event in telemetry_invocations.values():
        telemetry_outcomes[f'{event.get("executionModel")}/{event.get("outcome")}'] += 1
        if isinstance(event.get("cpuTimeMs"), (int, float)):
            method = event.get("rpcMethod") or event.get("eventType") or "unknown"
            telemetry_by_method[f'{event.get("executionModel")}/{method}/{event.get("outcome")}'].append(event["cpuTimeMs"])
    failures = [
        {"path": str(path.relative_to(root)), **read(path)}
        for path in deployed.glob("block-*/cohort-*/*/*/failure.json")
    ]
    gaps = [gap for path in deployed.glob("block-*/*/telemetry-gaps.json") for gap in read(path)["gaps"]]
    output["cloudflare"] = {
        "validatedSamples": len(samples),
        "matchedTriples": len(triples),
        "unmatchedValidatedSamples": sum(len(row) for row in by_key.values() if len(row) != 3),
        "completedObjectProofs": len(list(deployed.glob("block-*/cohort-*/*/*/evidence.json*"))),
        "telemetryGaps": len(gaps),
        "telemetryGapReasons": dict(collections.Counter(gap["reason"] for gap in gaps)),
        "telemetryGapMatchCounts": dict(collections.Counter(gap["matches"] for gap in gaps)),
        "failures": failures,
        "telemetryOutcomes": dict(telemetry_outcomes),
        "telemetryCpuByMethod": {key: stats(values) for key, values in telemetry_by_method.items()},
        "rows": rows,
        "limits": [
            "Only complete baseline/candidate/control triples enter the paired tables; unmatched samples and failed requests remain in the raw evidence.",
            "CPU comes only from deployed Cloudflare Workers Observability; local counts are a different workload.",
            "End-to-end A/B estimates the CPU change attributable to the conversion, not the entire original append stage's CPU fraction.",
        ],
    }
(root / "analysis.json").write_text(json.dumps(output, indent=2) + "\n")
print(json.dumps({"counts": len(counts), "cloudflare": output.get("cloudflare", {}).get("validatedSamples")}))
