import csv
import hashlib
import json
import sys
from pathlib import Path

source, output = map(Path, sys.argv[1:])
run = json.loads(source.read_text())
objects = run.pop("objects")
keys = ("target", "history", "ttftMs", "object", "epoch", "build")
rows = [{**{key: obj[key] for key in keys}, "state": state, **sample}
        for obj in objects for state, samples in obj["states"].items() for sample in samples]
columns = [*keys, "state", *sorted({key for row in rows for key in row} - {*keys, "state"})]
table = output.with_suffix(".samples.csv")
with table.open("w", newline="") as stream:
    writer = csv.DictWriter(stream, fieldnames=columns, lineterminator="\n")
    writer.writeheader()
    writer.writerows(rows)
run["sampleTable"] = {"file": table.name, "rows": len(rows), "sha256": hashlib.sha256(table.read_bytes()).hexdigest(), "note": "Numeric samples including warmups; blank cells mean absent. Object numbers are local to target/history/TTFT cohorts. Detailed provider probes and raw telemetry remain private; their source result is bound by rawSha256."}
output.write_text(json.dumps(run, indent=2) + "\n")
print(json.dumps({"summaryBytes": output.stat().st_size, "sampleTableBytes": table.stat().st_size, "rows": len(rows)}))
