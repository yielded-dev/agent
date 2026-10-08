import json
import os
from pathlib import Path
import signal
import statistics
import subprocess
import sys
import time

args = sys.argv[1:]
if args and args[0] == "--":
    args.pop(0)
if len(args) != 3:
    raise SystemExit("cpu-cohorts <base-cpu-stage> <prototype-cpu-stage> <new-output-dir>")
base, prototype, out = map(lambda value: Path(value).resolve(), args)
if out.exists():
    raise SystemExit(f"Refusing existing output {out}")
identities = {role: json.loads((stage / "identity.json").read_text()) for role, stage in [("base", base), ("prototype", prototype)]}
if any(identity["mode"] != "cpu" for identity in identities.values()):
    raise SystemExit("Stages must use the CPU-only fixture adapter")
if identities["base"]["fixtureFiles"] != identities["prototype"]["fixtureFiles"]:
    raise SystemExit("Shared fixture bytes differ")
out.mkdir(parents=True)
orders = [
    ["base", "prototype", "control"], ["control", "prototype", "base"],
    ["prototype", "control", "base"], ["base", "control", "prototype"],
    ["control", "base", "prototype"], ["prototype", "base", "control"],
    ["base", "prototype", "control"],
]
report = {"measurement": "kom433-process-cpu-v1", "identities": identities, "control": "same baseline stage, separate process and fresh database", "orders": orders, "cohorts": [], "complete": False, "failure": None}
child = None

def persist():
    (out / "cohorts.json").write_text(json.dumps(report, indent=2) + "\n")

def stop_child():
    if child is not None and child.poll() is None:
        os.killpg(child.pid, signal.SIGTERM)
        try:
            child.wait(timeout=5)
        except subprocess.TimeoutExpired:
            os.killpg(child.pid, signal.SIGKILL)
            child.wait()

def interrupted(_signal, _frame):
    raise KeyboardInterrupt("cohort controller interrupted")

signal.signal(signal.SIGTERM, interrupted)
persist()
try:
    for index, order in enumerate(orders):
        cohort = {"index": index, "order": order, "runs": {}}
        report["cohorts"].append(cohort)
        for role in order:
            name = f"{index:02d}-{role}"
            stage = prototype if role == "prototype" else base
            options = {"cold": False, "profile": "pr", "mode": "steady-state-profile", "cpuProfile": str(out / f"{name}.cpu.json"), "cases": ["sqlite-tool-rounds-4"], "warmups": 0, "samples": 1, "output": str(out / f"{name}.worker.json")}
            options_file = out / f"{name}.options.json"
            options_file.write_text(json.dumps(options) + "\n")
            (out / f"{name}.host-before.txt").write_text(subprocess.check_output(["ps", "-Ao", "pid,ppid,pcpu,comm"], text=True))
            run = {"stage": str(stage), "started": time.time(), "loadBefore": os.getloadavg(), "exitCode": None}
            cohort["runs"][role] = run
            persist()
            print(f"cohort {index + 1}/7: {role}", flush=True)
            with (out / f"{name}.log").open("w") as log:
                child = subprocess.Popen(["vp", "run", "cpu-worker", "--", str(stage), str(options_file)], cwd=Path(__file__).resolve().parent, stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
                try:
                    run["exitCode"] = child.wait(timeout=900)
                finally:
                    stop_child()
                    child = None
            run["finished"] = time.time()
            run["loadAfter"] = os.getloadavg()
            persist()
            if run["exitCode"] != 0:
                raise RuntimeError(f"{name} failed; retained its raw worker report and log")
            worker = json.loads((out / f"{name}.worker.json").read_text())
            cpu = json.loads((out / f"{name}.cpu.json").read_text())
            result = worker["steadyState"]
            if worker["failure"] is not None or worker["active"] is not None or not cpu["success"] or cpu["measurement"] != report["measurement"]:
                raise RuntimeError(f"{name} incomplete")
            for key, value in {"warmupOperations": 500, "operations": 1000, "modelCalls": 5000, "modelFinalizers": 5000, "toolCalls": 4000, "toolFinalizers": 4000, "canonicalCompletions": 1000}.items():
                if result[key] != value:
                    raise RuntimeError(f"{name}: {key}={result[key]}, expected {value}")
            if "operationSamplesMs" in result or result["samplingIntervalMicros"] is not None:
                raise RuntimeError(f"{name} used clocks or Inspector instrumentation")
            run["cpu"] = cpu
            persist()
        b = cohort["runs"]["base"]["cpu"]["cpu"]["totalMicros"]
        cohort["prototypeOverBase"] = cohort["runs"]["prototype"]["cpu"]["cpu"]["totalMicros"] / b
        cohort["controlOverBase"] = cohort["runs"]["control"]["cpu"]["cpu"]["totalMicros"] / b
        persist()
    runtimes = {run["cpu"]["node"] for cohort in report["cohorts"] for run in cohort["runs"].values()}
    if runtimes != {"v24.21.0"}:
        raise RuntimeError(f"Runtime changed: {runtimes}")
    report["ratios"] = {}
    for key in ["prototypeOverBase", "controlOverBase"]:
        values = [cohort[key] for cohort in report["cohorts"]]
        q = statistics.quantiles(values, n=4, method="inclusive")
        report["ratios"][key] = {"raw": values, "median": statistics.median(values), "min": min(values), "q1": q[0], "q3": q[2], "max": max(values)}
    report["complete"] = True
except BaseException as error:
    stop_child()
    report["failure"] = repr(error)
    raise
finally:
    persist()
