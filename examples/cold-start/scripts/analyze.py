"""Validate first-use evidence and report paired, deployment-stratified 95% CIs."""
import argparse
import hashlib
import json
import math
from pathlib import Path
import random
import statistics

if not __debug__:
    raise RuntimeError("Qualification requires Python assertions; disable optimization")

ARMS = ["yielded-single", "pi-single", "yielded-split", "pi-split", "yielded-empty", "pi-empty"]


def load(path):
    return json.loads(path.read_text())


def read_run(root, seen, bundles):
    plan, uploads = load(root / "plan.json"), load(root / "uploads.json")
    confirmed = load(root / "confirmed-versions.json")
    for uploaded in uploads.values():
        bundle, digest = uploaded["bundle"], uploaded["bundleSha256"]
        assert bundles.setdefault(bundle, digest) == digest, "Mixed source bundles"
    blocks = {}
    driver_instance = None
    for cell in plan["cells"]:
        result = load(root / f"capture-b{cell['block']}-{cell['arm']}.json")
        assert "error" not in result, result
        # Readiness precedes uploads and propagation. Require one incarnation during
        # collection; eviction during untimed setup does not touch a target.
        if driver_instance is None:
            driver_instance = result["instance"]
        assert result["instance"] == driver_instance, "Driver incarnation changed during collection"
        rows = result["frames"]
        assert not any(r["frame"]["type"] == "error" for r in rows), rows
        assert all(math.isfinite(r["at"]) and r["at"] >= 0 for r in rows)
        assert [r["at"] for r in rows] == sorted(r["at"] for r in rows), "Arrival order"
        assert [r["frame"]["type"] for r in rows[:2]] == ["worker", "object"]
        markers = {}
        for kind, name in [("worker", cell["front"]), ("object", cell["host"])]:
            matches = [r for r in rows if r["frame"]["type"] == kind]
            assert len(matches) == 1, matches
            marker = matches[0]
            value = marker["frame"]["value"]
            assert value["calls"] == 1 and value["alarms"] == 0, value
            assert value["constructions"] == (1 if kind == "object" else 0), value
            assert value["version"] == uploads[name]["version"] == confirmed[name], value
            assert value["moduleId"] not in seen, "Reused module instance"
            seen.add(value["moduleId"])
            assert hashlib.sha256((root / (name + ".mjs")).read_bytes()).hexdigest() == uploads[name]["sha256"]
            markers[kind] = marker["at"]
        values = {"worker": markers["worker"], "object": markers["object"] - markers["worker"]}
        if not cell["arm"].endswith("empty"):
            texts = [r for r in rows if r["frame"]["type"] == "text" and r["frame"]["value"]["text"]]
            done = [r for r in rows if r["frame"]["type"] == "done"]
            assert "".join(r["frame"]["value"]["text"] for r in texts) == "hello", texts
            assert rows[-1]["frame"]["type"] == "done"
            assert len(done) == 1 and done[0]["frame"]["value"] == {"outcome": "completed", "calls": 1}, done
            values["firstText"] = texts[0]["at"]
            values["afterObject"] = texts[0]["at"] - markers["object"]
        else:
            assert len(rows) == 2, "Empty Object performed a turn"
        blocks.setdefault(cell["block"], {})[cell["arm"]] = values
    assert len(blocks) == plan["blocks"]
    assert all(set(block) == set(ARMS) for block in blocks.values()), "Incomplete pair block"
    assert load(root / "cleanup.json")["verified"], "Teardown unverified"
    return list(blocks.values())


def estimates(cohorts, pick):
    values = [[pick(block) for block in cohort] for cohort in cohorts]
    rng = random.Random(204)
    boot = sorted(statistics.mean(rng.choice(group) for group in values for _ in group)
                  for _ in range(20000))
    return {"mean": statistics.mean(v for group in values for v in group),
            "low": boot[500], "high": boot[19499]}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("runs", nargs="+", type=Path)
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args()
    seen = set()
    bundles = {}
    cohorts = [read_run(root, seen, bundles) for root in args.runs]
    assert len(cohorts) >= 2 and sum(map(len, cohorts)) >= 12, "Need >=12 pairs in >=2 deployments"
    result = {"pairs": sum(map(len, cohorts)), "deployments": len(cohorts), "arms": {}, "contrasts": {}}
    for arm in ARMS:
        result["arms"][arm] = {metric: estimates(cohorts, lambda b: b[arm][metric])
                               for metric in cohorts[0][0][arm]}
    for shape in ["single", "split", "empty"]:
        result["contrasts"][shape] = {metric: estimates(cohorts, lambda b:
            b["yielded-" + shape][metric] - b["pi-" + shape][metric])
            for metric in cohorts[0][0]["yielded-" + shape]}
    for framework in ["yielded", "pi"]:
        result["contrasts"][framework + "-split-minus-single"] = estimates(cohorts, lambda b:
            b[framework + "-split"]["firstText"] - b[framework + "-single"]["firstText"])
    args.out.write_text(json.dumps(result, indent=2) + "\n")
    def fmt(v):
        return f"{v['mean']:.0f} [{v['low']:.0f}, {v['high']:.0f}]"
    print("| Layout | Yielded first text (ms) | pi first text (ms) | Paired gap (ms) |")
    print("| --- | ---: | ---: | ---: |")
    for shape in ["single", "split"]:
        print(f"| {shape} | {fmt(result['arms']['yielded-' + shape]['firstText'])} | "
              f"{fmt(result['arms']['pi-' + shape]['firstText'])} | "
              f"{fmt(result['contrasts'][shape]['firstText'])} |")
    print("Empty Object window gap:", fmt(result["contrasts"]["empty"]["object"]))
    print("After Object marker gap:", fmt(result["contrasts"]["split"]["afterObject"]))


if __name__ == "__main__":
    main()
