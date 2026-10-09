import collections
import json
import statistics
import sys
from pathlib import Path

source, output = map(Path, sys.argv[1:])
run = json.loads(source.read_text())
groups = collections.defaultdict(list)
for obj in run["objects"]:
    if obj["target"] == "yielded":
        groups[(obj["history"], obj["ttftMs"])].append(obj)
result = []
for (history, ttft), objects in sorted(groups.items()):
    repeat_ids = sorted({sample["repeat"] for obj in objects for sample in obj["states"].get("warm", [])})
    if len(repeat_ids) != run["options"]["repeats"]:
        raise ValueError("Unexpected warm repeat indexes")
    for metric in ("firstRequestMs", "driverMs"):
        by_object = collections.defaultdict(lambda: collections.defaultdict(list))
        for obj in objects:
            for sample in obj["states"].get("warm", []):
                if sample["status"] == "ok" and metric in sample:
                    by_object[obj["object"]][(obj["build"], sample["repeat"])].append(sample[metric])
        complete = {o: samples for o, samples in by_object.items() if all(len(samples[(b, r)]) == 2 for b in ("baseline", "candidate") for r in repeat_ids)}
        if not complete:
            continue
        estimates = []
        for repeat in repeat_ids:
            ratios, deltas = [], []
            for samples in complete.values():
                b = statistics.median(samples[("baseline", repeat)])
                c = statistics.median(samples[("candidate", repeat)])
                ratios.append(c / b)
                deltas.append(c - b)
            estimates.append({"repeat": repeat, "pairedMedianRatio": statistics.median(ratios), "pairedMedianDeltaMs": statistics.median(deltas)})
        ratio_range = max(e["pairedMedianRatio"] for e in estimates) - min(e["pairedMedianRatio"] for e in estimates)
        delta_range = max(e["pairedMedianDeltaMs"] for e in estimates) - min(e["pairedMedianDeltaMs"] for e in estimates)
        result.append({"history": history, "ttftMs": ttft, "metric": metric, "objects": len(complete), "repeatEstimates": estimates, "ratioRange": ratio_range, "deltaRangeMs": delta_range})
output.write_text(json.dumps({"run": run["run"], "method": "For each warm repeat index, take each complete Object's median over both epochs of each build; report the median paired candidate/baseline ratio and delta across Objects. These six cohort estimates describe repeat variation; they are not independent trials or confidence intervals. Raw individual-turn ranges and baseline epoch drift remain in the compact run.", "cells": result}, indent=2) + "\n")
for cell in result:
    if cell["metric"] == "firstRequestMs":
        print(json.dumps(cell))
