"""Reduce retained deployed measurements and deterministic counts; no workloads."""
from pathlib import Path
import gzip
import json
import statistics

ROOT = Path(__file__).resolve().parent


def load(relative):
    path = ROOT / relative
    raw = path.read_bytes() if path.exists() else gzip.decompress(Path(str(path) + ".gz").read_bytes())
    return json.loads(raw)


def save(relative, value):
    (ROOT / relative).write_text(json.dumps(value, indent=2) + "\n")


def q(values, fraction):
    ordered = sorted(values)
    if not ordered:
        return None
    position = (len(ordered) - 1) * fraction
    index = int(position)
    return ordered[index] + (ordered[min(index + 1, len(ordered) - 1)] - ordered[index]) * (position - index)


def stats(values):
    return {"n": len(values), "median": q(values, .5), "q1": q(values, .25), "q3": q(values, .75), "values": values}


calibration = load("calibration/summary.json")
race = load("calibration-race/summary.json")
attribution = load("attribution/summary.json")
real = load("real-turn/summary.json")
roles = ["pin", "pin-control"]
models = [row for row in calibration["fits"] if row["basis"] == "two-N-slope" and row["population"] == "composition-only" and row["role"] in roles and row["metric"] != "joint"]
e_rates = [row["eWeight"] for row in models if row["metric"] == "evaluations"]
a_rates = [row["aWeight"] for row in models if row["metric"] == "allocations"]


def project(row):
    return {**row,
            "evaluationModelMs": [row["evaluations"] * value / 1e6 for value in e_rates],
            "allocationModelMs": [row["allocations"] * value / 1e6 for value in a_rates]}


stages = [project({"stage": name, **row}) for name, row in attribution["stages"]["exclusive"].items()]
assert sum(row["evaluations"] for row in stages) == 37431
assert sum(row["allocations"] for row in stages) == 44024
whole = project({"evaluations": 37431, "allocations": 44024})
turns = []
for counter in attribution["coverage"]:
    turn = int(counter["input"]["id"][1:])
    row = project({"turn": turn, "precedingHistory": counter["historicalTurns"], "evaluations": counter["evaluations"], "allocations": counter["allocations"]})
    row["observed"] = [item for item in real["groups"] if item["turn"] == turn]
    turns.append(row)


def paired_overhead(shape, lane=calibration):
    output = []
    for role in roles:
        target = next(row for row in lane["slopeGroups"] if row["role"] == role and row["case"] == shape)
        control = next(row for row in lane["slopeGroups"] if row["role"] == role and row["case"] == "sync")
        values = []
        for row in target["roundValues"]:
            other = next((item for item in control["roundValues"] if item["round"] == row["round"]), None)
            if other:
                values.append(row["nsPerIteration"] - other["nsPerIteration"])
        output.append({"role": role, "nsPerEntry": stats(values)})
    return output


overheads = {name: paired_overhead(name) for name in ["errors-success", "fn-traced", "span", "scope", "semaphore", "interrupt-mask"]}
overheads.update({name: paired_overhead(name, race) for name in ["race-winner-first", "race-loser-first"]})


def scenario(name, entries, rates, caveat):
    values = [entries * rate / 1e6 for rate in rates]
    return {"name": name, "hypotheticalRemovedEntries": entries, "modeledMs": values, "rangeMs": [min(values), max(values)], "caveat": caveat}


candidates = [
    scenario("All success-path OnFailure frames", 4030 / 2, [row["nsPerEntry"]["median"] for row in overheads["errors-success"]], "4030 frames divided by the microcase's two guard frames. Many actual handlers protect asynchronous failures, interruption or schema errors and cannot be removed; no proven removable fraction."),
    scenario("All span entries represented by 280 endSpan calls", 280, [row["nsPerEntry"]["median"] for row in overheads["span"]], "Measures the no-exporter withSpan microshape. End counts include named fn spans; actual costs and removable leaf spans vary. Removing them loses tracing detail."),
    {"name": "KOM-433's actually demonstrated synchronous conversions", **project({"evaluations": 2695, "allocations": 3032}), "rangeMs": [min(2695 * min(e_rates), 3032 * min(a_rates)) / 1e6, max(2695 * max(e_rates), 3032 * max(a_rates)) / 1e6], "caveat": "A conditional extrapolation of the old first-position count delta, not a deployed prototype measurement."},
    scenario("Uncontended permit wrappers", 312, [row["nsPerEntry"]["median"] for row in overheads["semaphore"]], "312 acquire callback entries. Contended permits and resource lifetime remain necessary; genuine Scopes have no supported removal estimate."),
    scenario("The 56 race orchestration entries", 56, [row["nsPerEntry"]["median"] for name in ["race-winner-first", "race-loser-first"] for row in overheads[name]], "The simple synchronous races omit timer registration, asynchronous winners/finalizers and engine payload work. Preserve deadlines, interruption and joined cleanup."),
]
failpoint_ms = []
for role in roles:
    failpoint = next(row for row in calibration["slopeGroups"] if row["role"] == role and row["case"] == "failpoint")
    sync = next(row for row in calibration["slopeGroups"] if row["role"] == role and row["case"] == "sync")
    failpoint_ms.append((135 * failpoint["extraOverPlainNsPerIteration"]["median"] + 60 * sync["nsPerIteration"]["median"]) / 1e6)
candidates.append({"name": "Disabled failpoint calls and wrappers", "modeledMs": failpoint_ms, "rangeMs": [min(failpoint_ms), max(failpoint_ms)], "caveat": "135 no-op calls have zero E/A; 60 storage wrappers originate one E/A each. Removing fault-injection boundaries would sacrifice recovery evidence for tens of microseconds."})
candidates.sort(key=lambda row: sum(row["rangeMs"]) / 2, reverse=True)

projection = {"warning": "These separate count-only models fail held-out and whole-turn validation. Estimates are illustrative composition scenarios, not measured stage CPU, promises, upper bounds, or additive savings. Never add the evaluation and allocation predictions or different attribution views.",
              "models": models, "evaluationRatesNs": e_rates, "allocationRatesNs": a_rates,
              "wholeAt50": whole, "stages": stages, "turns": turns,
              "exclusiveSites": [project(row) for row in attribution["firstTurn"]["exclusiveSites"]],
              "originSites": [project(row) for row in attribution["firstTurn"]["originSites"]],
              "originModules": [project(row) for row in attribution["firstTurn"]["originModules"]],
              "outsideStageSites": [project(row) for row in attribution["firstTurn"]["outsideStageSites"]],
              "shapeOverheads": overheads, "candidateScenarios": candidates,
              "kom433": {"exactFirstPosition": project({"evaluations": 2695, "allocations": 3032}), "rounded3000EvaluationsMs": [3000 * rate / 1e6 for rate in e_rates], "historicalNodeControlRatio": {"median": 1.005341, "q1": .929344, "q3": 1.173600}, "source": "kom433-source/report.md", "warning": "Cloudflare calibration does not establish Node unit costs. Original Node workload used fresh threads and four tool calls, a different fixture."}}
save("projection.json", projection)


def interval(values, digits=2):
    return f"{min(values):.{digits}f}–{max(values):.{digits}f}"


def stat(value, digits=1):
    if value is None or not value["n"]:
        return "—"
    return f"{value['median']:.{digits}f} [{value['q1']:.{digits}f}–{value['q3']:.{digits}f}] (n={value['n']})"


lines = ["# Arithmetic projections", "", projection["warning"], "", "| Exclusive stage | Evaluations | Allocations | E-only model ms | A-only model ms |", "|---|---:|---:|---:|---:|"]
for row in sorted(stages, key=lambda row: row["evaluations"], reverse=True):
    lines.append(f"| {row['stage']} | {row['evaluations']:,} | {row['allocations']:,} | {interval(row['evaluationModelMs'])} | {interval(row['allocationModelMs'])} |")
lines.append(f"| **Whole turn** | **37,431** | **44,024** | **{interval(whole['evaluationModelMs'])}** | **{interval(whole['allocationModelMs'])}** |")
for title, key in [("Dynamic execution sites", "exclusiveSites"), ("Construction-origin modules", "originModules"), ("Sites outside the eight stage boundaries", "outsideStageSites")]:
    lines += ["", "## " + title, "", "| Site / module | E | A | E-only model ms | A-only model ms |", "|---|---:|---:|---:|---:|"]
    for row in projection[key][:25]:
        lines.append(f"| `{row['site']}` | {row['evaluations']:,} | {row['allocations']:,} | {interval(row['evaluationModelMs'])} | {interval(row['allocationModelMs'])} |")
lines += ["", "## Comparison with deployed turns", "", "| Position | History | E | A | E-only ms | A-only ms | Observed pin CPU ms [Q1–Q3] | Observed control CPU ms |", "|---|---:|---:|---:|---:|---:|---|---|"]
for row in turns:
    lines.append(f"| m{row['turn']} | {row['precedingHistory']} | {row['evaluations']:,} | {row['allocations']:,} | {interval(row['evaluationModelMs'])} | {interval(row['allocationModelMs'])} | {stat(row['observed'][0]['cpuMs'])} | {stat(row['observed'][1]['cpuMs'])} |")
lines += ["", "## Conditional removal scenarios", "", "| Scenario | Modeled ms | Limitation |", "|---|---:|---|"]
for row in candidates:
    lines.append(f"| {row['name']} | {interval(row['rangeMs'], 3)} | {row['caveat']} |")
(ROOT / "projection-tables.md").write_text("\n".join(lines) + "\n")

for lane_name, data in [("calibration", calibration), ("calibration-race", race)]:
    lines = ["# Complete per-build calibration", "", "Medians [Q1–Q3] use observed complete subtraction tuples; n is shown. E/A rates divide baseline-subtracted CPU by marginal constructor/interpreter counts. These are workload ratios, not causal prices. All raw CPU, comparator CPU and walls remain in summary.json."]
    for role in ["pin", "pin-control", "base", "head", "base-control"]:
        lines += ["", "## " + role, "", "| Shape | N | CPU ms | Comparator CPU ms | ns/E | ns/A | Extra ns/iteration |", "|---|---:|---|---|---|---|---|"]
        for row in data["groups"]:
            if row["role"] != role or row["case"] == "empty":
                continue
            lines.append(f"| {row['case']} | {row['iterations']:,} | {stat(row['effectCpuMs'])} | {stat(row['plainCpuMs'])} | {stat(row['nsPerEvaluation'])} | {stat(row['nsPerAllocation'])} | {stat(row['extraOverPlainNsPerIteration'])} |")
    lines += ["", "## Largest-N paired comparisons", "", "The primary flag requires all seven matched rounds and a median gain beyond the descriptive identical-code envelope. It is not a significance test.", "", "| Shape | N | Head/base | Identical base-control/base | Head/control | Identical pin-control/pin | Beyond primary envelope |", "|---|---:|---|---|---|---|---|"]
    for row in data["comparisons"]:
        if row["case"] == "empty" or any(other["case"] == row["case"] and other["iterations"] > row["iterations"] for other in data["comparisons"]):
            continue
        lines.append(f"| {row['case']} | {row['iterations']:,} | {stat(row['headOverBase'],3)} | {stat(row['controlOverBase'],3)} | {stat(row['headOverControl'],3)} | {stat(row['pinControlOverPin'],3)} | {row['exceedsControlEnvelope']} |")
    (ROOT / lane_name / "per-build-tables.md").write_text("\n".join(lines) + "\n")

print(json.dumps({"wholeAt50": whole, "candidateScenarios": [{"name": row["name"], "rangeMs": row["rangeMs"]} for row in candidates]}, indent=2))
