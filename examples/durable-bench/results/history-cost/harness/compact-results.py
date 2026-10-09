import argparse
import collections
import hashlib
import json
import math
import statistics
from pathlib import Path

parser = argparse.ArgumentParser()
parser.add_argument("raw", type=Path)
parser.add_argument("output", type=Path)
args = parser.parse_args()
raw_bytes = args.raw.read_bytes()
run = json.loads(raw_bytes)
samples = run["samples"]

def median(values):
    return statistics.median(values) if values else None

def quantile(values, q):
    values = sorted(values)
    if not values:
        return None
    at = (len(values) - 1) * q
    low = math.floor(at)
    return values[low] + (values[min(low + 1, len(values) - 1)] - values[low]) * (at - low)

def stats(values):
    return {"n": len(values), "median": median(values), "q1": quantile(values, .25), "q3": quantile(values, .75), "min": min(values) if values else None, "max": max(values) if values else None}

def groups(rows, key):
    result = collections.defaultdict(list)
    for row in rows:
        result[key(row)].append(row)
    return result

metrics = ("firstRequestMs", "driverMs", "admissionMs", "gapMs")
ok = [s for s in samples if s["status"] == "ok" and s["state"] != "warmup"]
cells = []
for key, rows in sorted(groups(ok, lambda s: (s["history"], s["ttftMs"], s["state"])).items()):
    history, ttft, state = key
    cell = {"history": history, "ttftMs": ttft, "state": state, "metrics": {}}
    for metric in metrics:
        available = [s for s in rows if metric in s]
        expected_labels = {b["label"] for b in run["builds"]}
        epochs_per_build = 2 if run["options"]["rigorous"] else 1
        repeats_per_epoch = run["options"]["repeats"] if state == "warm" else 1
        values = []
        excluded = []
        for (target, obj), turns in groups(available, lambda s: (s["target"], s["object"])).items():
            builds = groups(turns, lambda s: s["build"])
            complete = set(builds) == expected_labels
            for build_rows in builds.values():
                epochs = groups(build_rows, lambda s: s["epoch"])
                complete &= len(epochs) == epochs_per_build and all(len(es) == repeats_per_epoch and len({e["repeat"] for e in es}) == repeats_per_epoch for es in epochs.values())
            if complete:
                values.extend(turns)
            else:
                excluded.append({"target": target, "object": obj.rsplit("-o", 1)[-1], "availableSamples": len(turns)})
        if not available:
            continue
        outcome = {"builds": {}, "pairedYielded": {}, "coverage": {"availableSamples": len(available), "completeSamples": len(values), "incompleteObjects": excluded}}
        for build, build_rows in sorted(groups(values, lambda s: s["build"]).items()):
            entry = {}
            for target, target_rows in sorted(groups(build_rows, lambda s: s["target"]).items()):
                object_rows = groups(target_rows, lambda s: s["object"])
                object_medians = [median([s[metric] for s in rs]) for rs in object_rows.values()]
                spreads = [max(s[metric] for s in rs)-min(s[metric] for s in rs) for rs in object_rows.values()]
                entry[target] = {"objects": len(object_rows), "samples": len(target_rows), "objectMedians": stats(object_medians), "repeatRange": stats(spreads)}
            if "yielded" in entry and "pi" in entry and entry["pi"]["objectMedians"]["median"] > 0:
                entry["yieldedDivPi"] = entry["yielded"]["objectMedians"]["median"] / entry["pi"]["objectMedians"]["median"]
            outcome["builds"][build] = entry
        paired = []
        drift = []
        deltas = []
        for _, rs in groups([s for s in values if s["target"] == "yielded"], lambda s: s["object"]).items():
            a = [s for s in rs if s["build"] == "baseline"]
            b = [s for s in rs if s["build"] == "candidate"]
            if not a or not b:
                continue
            am, bm = median([s[metric] for s in a]), median([s[metric] for s in b])
            if am > 0:
                paired.append(bm/am)
            deltas.append(bm-am)
            epochs = [median([s[metric] for s in es]) for es in groups(a, lambda s: s["epoch"]).values()]
            if len(epochs) == 2 and min(epochs) > 0:
                drift.append(max(epochs)/min(epochs)-1)
        outcome["pairedYielded"] = {"candidateDivBaseline": stats(paired), "deltaMs": stats(deltas), "baselineEpochDrift": stats(drift)}
        cell["metrics"][metric] = outcome
    cells.append(cell)

per_object = []
for key, rows in sorted(groups(samples, lambda s: (s["target"], s["history"], s["ttftMs"], s["object"], s["epoch"], s["build"])).items()):
    target, history, ttft, obj, epoch, build = key
    item = {"target": target, "history": history, "ttftMs": ttft, "object": obj.rsplit("-o", 1)[-1], "epoch": epoch, "build": build, "states": {}}
    for state, rs in groups(rows, lambda s: s["state"]).items():
        item["states"][state] = [{"repeat": s["repeat"], "status": s["status"], **{f: s[f] for f in (*metrics, "objectBuildVerified", "driverStartedMs", "firstRequestArrivalMs", "firstRequestRawMs", "firstRequestLowerMs", "firstRequestUpperMs", "firstRequestClockValid", "colo", "providerColo", "error") if f in s}} for s in rs]
    per_object.append(item)

cpu = run.get("cpu", [])
cpu_outcomes = [{"target": key[0], "kind": key[1], "outcome": key[2], "count": len(rows), "cpuMs": stats([r["cpuMs"] for r in rows if r["cpuMs"] is not None])} for key, rows in sorted(groups(cpu, lambda r: (r.get("target", "unattributed"), r["kind"], r["outcome"])).items())]
clocks = [s for s in ok if s.get("firstRequestClockValid")]
result = {k: run[k] for k in ("run", "revision", "dirty", "startedAt", "wallMs", "options", "seedBuild", "builds", "fixtures", "complete", "failures", "cleanup", "kept") if k in run}
result.update({"rawSha256": hashlib.sha256(raw_bytes).hexdigest(), "samplesByStatus": dict(collections.Counter(s["status"] for s in samples)), "fingerprintsVerified": sum(s.get("fingerprintVerified", False) for s in samples), "modelRequestsVerified": sum(len(s.get("fingerprints", [])) for s in samples), "clockChecks": {"valid": len(clocks), "rejected": sum(s.get("firstRequestClockValid") is False for s in ok), "intervalWidthMs": stats([s["firstRequestUpperMs"]-s["firstRequestLowerMs"] for s in clocks]), "providerColos": dict(collections.Counter(s.get("providerColo", "unavailable") for s in ok))}, "cpuOutcomes": cpu_outcomes, "unmatchedCpuMarkers": run.get("unmatchedCpuMarkers"), "cells": cells, "objects": per_object})
if any("objectBuildVerified" in s for s in samples):
    result["objectBuildsVerified"] = sum(s.get("objectBuildVerified", False) for s in samples)
args.output.parent.mkdir(parents=True, exist_ok=True)
args.output.write_text(json.dumps(result, indent=2)+"\n")
print(json.dumps({"complete": run["complete"], "sampleCount": len(samples), "failures": run["failures"], "clockChecks": result["clockChecks"]}))
for cell in cells:
    for name, metric in cell["metrics"].items():
        if name not in ("firstRequestMs", "driverMs"):
            continue
        print(json.dumps({"cell": [cell["history"], cell["ttftMs"], cell["state"]], "metric": name, "ratios": {b:v.get("yieldedDivPi") for b,v in metric["builds"].items()}, "paired": metric["pairedYielded"]}))
