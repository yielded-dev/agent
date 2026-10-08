"""Read retained invocation logs for this spike, including after resource cleanup.

Usage: python3 harness/export-telemetry.py <closed-run-directory>
Credentials are read from the environment and never written or printed.
"""

import concurrent.futures
import json
import os
import pathlib
import sys
import urllib.request


root = pathlib.Path(sys.argv[1]).resolve()
endpoint = (
    "https://api.cloudflare.com/client/v4/accounts/"
    + os.environ["CLOUDFLARE_ACCOUNT_ID"]
    + "/workers/observability/telemetry/query"
)
headers = {
    "Authorization": "Bearer " + os.environ["CLOUDFLARE_API_TOKEN"],
    "Content-Type": "application/json",
}


def export(path):
    deployment = json.loads(path.read_text())
    name = deployment["name"]
    if not name.startswith("sync-do-append-"):
        raise ValueError("Refusing a worker outside this spike")
    directory = path.parent
    target = directory / "telemetry-after-cleanup.json"
    if target.exists():
        raise ValueError("Preserve the existing export")
    receipts = [
        json.loads(receipt.read_text())
        for receipt in directory.parent.glob(f"cohort-*/{deployment['role']}/*/*.json")
    ]
    receipts = [row for row in receipts if "startedAtMillis" in row]
    query = {
        "queryId": name + "-after-cleanup",
        "dry": True,
        "view": "events",
        "limit": 2000,
        "timeframe": {
            "from": min(row["startedAtMillis"] for row in receipts) - 60_000,
            "to": max(row["endedAtMillis"] for row in receipts) + 60_000,
        },
        "parameters": {
            "filterCombination": "and",
            "filters": [{"key": "$workers.scriptName", "operation": "eq", "type": "string", "value": name}],
        },
    }
    request = urllib.request.Request(endpoint, data=json.dumps(query).encode(), headers=headers)
    with urllib.request.urlopen(request, timeout=45) as response:
        result = json.load(response)["result"]
    rows = []
    for event in result["events"]["events"]:
        worker = event.get("$workers", {})
        metadata = event.get("$metadata", {})
        rpc = worker.get("event", {})
        methods = list(dict.fromkeys(rpc.get("rpcMethods", [])))
        rows.append({
            "timestamp": event.get("timestamp"),
            "id": metadata.get("id"),
            "metadataType": metadata.get("type"),
            "traceId": metadata.get("traceId"),
            **{key: worker.get(key) for key in (
                "cpuTimeMs", "wallTimeMs", "executionModel", "eventType", "durableObjectId",
                "scriptName", "requestId", "outcome", "scriptVersion", "truncated",
            )},
            "rpcMethod": rpc.get("rpcMethod") or (methods[0] if len(methods) == 1 else None),
            "rpcMethods": rpc.get("rpcMethods", []),
            "rpcCallCount": rpc.get("rpcCallCount"),
        })
    count = result["events"]["count"]
    if count != len(rows) or count >= 2000:
        raise ValueError("Telemetry result is truncated")
    statistics = {
        key: value for key, value in result.get("statistics", {}).items()
        if isinstance(value, (int, float, bool))
    }
    target.write_text(json.dumps({"query": query, "statistics": statistics, "count": count, "events": rows}, indent=2) + "\n")
    return {"worker": name, "events": count, "statistics": statistics}


with concurrent.futures.ThreadPoolExecutor(max_workers=3) as pool:
    for result in pool.map(export, sorted(root.glob("block-*/*/deployment.json"))):
        print(json.dumps(result))
