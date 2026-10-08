"""Standalone figures from retained Cloudflare CPU measurements."""
from pathlib import Path
import gzip
import json
import platform

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np

ROOT = Path(__file__).resolve().parent
OUT = ROOT / "figures"
OUT.mkdir(exist_ok=True)


def load(relative):
    path = ROOT / relative
    raw = path.read_bytes() if path.exists() else gzip.decompress(Path(str(path) + ".gz").read_bytes())
    return json.loads(raw)


data = load("calibration/summary.json")
projection = load("projection.json")
real = load("real-turn/summary.json")
plt.rcParams.update({"font.family": "DejaVu Sans", "font.size": 10, "axes.spines.top": False,
                     "axes.spines.right": False, "axes.labelcolor": "#253347", "text.color": "#172438",
                     "axes.edgecolor": "#a8b1bf", "savefig.facecolor": "white", "svg.fonttype": "none"})
blue, amber, gray = "#2365a8", "#ba6a18", "#778394"
fig, axes = plt.subplots(1, 2, figsize=(15, 7.8), gridspec_kw={"width_ratios": [1.5, 1]})
ax = axes[0]
names = ["flatMap8", "map8", "service8", "semaphore", "stream8", "interrupt-mask", "fn-untraced", "sync-reused", "gen8", "scope", "sync", "errors-success", "fn-traced", "span", "sql"]
labels = {"flatMap8": "8 flatMaps", "map8": "8 maps", "service8": "8 service lookups", "semaphore": "Uncontended permit", "stream8": "8-item stream", "interrupt-mask": "Interruption mask", "fn-untraced": "fnUntraced", "sync-reused": "Reused sync", "gen8": "8 yielded helpers", "scope": "Scoped acquire/release", "sync": "New sync", "errors-success": "Success-path error guards", "fn-traced": "Named fn", "span": "withSpan", "sql": "SQL wrapper + native SQL"}
for role, color, shift, label in [("pin", blue, -.14, "Pinned Worker"), ("pin-control", amber, .14, "Identical-code control")]:
    for index, name in enumerate(names):
        group = max((row for row in data["groups"] if row["role"] == role and row["case"] == name), key=lambda row: row["iterations"])
        value = group["nsPerEvaluation"]
        ax.errorbar(value["median"] / 1000, index + shift,
                    xerr=np.array([[(value["median"] - value["q1"]) / 1000], [(value["q3"] - value["median"]) / 1000]]),
                    fmt="o", markersize=4.7, color=color, elinewidth=2, capsize=2,
                    label=label if index == 0 else None)
ax.set_yticks(range(len(names)), [labels[name] for name in names])
ax.invert_yaxis()
ax.set_xscale("log")
ax.set_xticks([.05, .1, .2, .5, 1, 2, 5, 10, 20], [".05", ".1", ".2", ".5", "1", "2", "5", "10", "20"])
ax.set_xlim(.035, 30)
ax.grid(axis="x", alpha=.2)
ax.set_xlabel("Baseline-subtracted CPU / evaluation (µs; log scale)")
ax.set_title("One count has very different CPU costs", loc="left", weight="bold", pad=16)
ax.legend(loc="upper right", frameon=False, fontsize=9)

ax = axes[1]
turn = projection["turns"][1]
e, a = turn["evaluationModelMs"], turn["allocationModelMs"]
observed = [next(row for row in real["groups"] if row["target"] == role and row["turn"] == 1)["cpuMs"] for role in ["pin", "pin-control"]]
values = [np.mean(e), np.mean(a), observed[0]["median"], observed[1]["median"]]
low = [min(e), min(a), observed[0]["q1"], observed[1]["q1"]]
high = [max(e), max(a), observed[0]["q3"], observed[1]["q3"]]
colors = [gray, gray, blue, amber]
ax.bar(range(4), values, color=colors, width=.62, alpha=.93)
ax.errorbar(range(4), values, yerr=[np.array(values)-low, np.array(high)-values], fmt="none", ecolor="#28374c", capsize=5, linewidth=1.5)
for i, value in enumerate(values):
    ax.text(i, high[i] + 10, f"{value:.1f}" if i < 2 else f"{value:.0f}", ha="center", weight="bold")
ax.set_xticks(range(4), ["Eval\nmodel", "Allocation\nmodel", "Actual\nWorker", "Actual\ncontrol"])
ax.set_ylim(0, 410)
ax.set_ylabel("CPU per warm turn (ms)")
ax.grid(axis="y", alpha=.2)
ax.set_axisbelow(True)
ax.set_title("Count models miss most whole-turn CPU", loc="left", weight="bold", pad=16)
ax.text(.02, .92, "m1 · 51 prior turns\n38,338 evaluations · 45,194 primitives", transform=ax.transAxes, fontsize=10, va="top")
fig.suptitle("Effect evaluation cost on deployed Cloudflare", x=.025, ha="left", fontsize=19, weight="bold", y=.98)
fig.text(.025, .91, "Patched Effect 4.0.0 · Workers Observability cpuTimeMs · same wnam location hint", fontsize=11, color="#566273")
fig.text(.025, .035, "Left: largest-N medians with Q1–Q3; 5–7 observed baseline pairs. Ratios include each shape's payload work.\nRight: separate composition-only model ranges across calibration copies; actual bars show median and Q1–Q3.\nSchema and no-op yields can consume CPU with zero marginal interpreter evaluations. Missing telemetry is never imputed.", fontsize=9, color="#566273", linespacing=1.5)
fig.subplots_adjust(left=.20, right=.98, bottom=.18, top=.84, wspace=.32)
fig.savefig(OUT / "cost-and-turn.png", dpi=180)
fig.savefig(OUT / "cost-and-turn.svg")
plt.close(fig)
(OUT / "environment.json").write_text(json.dumps({"python": platform.python_version(), "matplotlib": matplotlib.__version__, "numpy": np.__version__, "inputs": ["calibration/summary.json", "real-turn/summary.json", "projection.json"]}, indent=2) + "\n")
print(OUT / "cost-and-turn.png")
