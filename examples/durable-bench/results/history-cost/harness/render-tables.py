import json
import sys
from pathlib import Path

source, output = map(Path, sys.argv[1:])
run = json.loads(source.read_text())
lines = [f"# {run['run']}", "", f"Complete: `{run['complete']}`. Revisions: " + ", ".join(f"{b['label']} `{b['revision']}`" for b in run['builds']) + ".", ""]
for metric, title in (("firstRequestMs", "Submission to first provider arrival"), ("driverMs", "Complete turn")):
    lines.extend([f"## {title}", "", "Medians of complete Object medians. All times are milliseconds. B = baseline; C = candidate. Paired ratios and deltas use each Yielded Object; drift is the median absolute relative difference between its two baseline epochs.", "", "| Turns | Provider ms | State | Yielded B → C ms | pi B → C ms | Y ÷ pi B → C | Paired C ÷ B | Paired Δ ms | Baseline epoch drift |", "|---:|---:|---|---:|---:|---:|---:|---:|---:|"])
    for cell in run["cells"]:
        result = cell["metrics"].get(metric)
        if not result or not all(b in result["builds"] for b in ("baseline", "candidate")):
            continue
        b, c = (result["builds"][label] for label in ("baseline", "candidate"))
        if not all(t in build for build in (b, c) for t in ("yielded", "pi")):
            continue
        pair = result["pairedYielded"]
        time = lambda build, target: build[target]["objectMedians"]["median"]
        lines.append(f"| {cell['history']:,} | {cell['ttftMs']} | {cell['state']} | {time(b, 'yielded'):.1f} → {time(c, 'yielded'):.1f} | {time(b, 'pi'):.1f} → {time(c, 'pi'):.1f} | {b['yieldedDivPi']:.3f}× → {c['yieldedDivPi']:.3f}× | {pair['candidateDivBaseline']['median']:.3f}× | {pair['deltaMs']['median']:+.1f} | {pair['baselineEpochDrift']['median']:.1%} |")
    lines.append("")
lines.extend(["The compact JSON retains Object IQRs, repeat ranges, complete-pair coverage, clock uncertainty and all observed non-ok outcomes. A ratio alone is not an improvement claim.", ""])
output.write_text("\n".join(lines))
