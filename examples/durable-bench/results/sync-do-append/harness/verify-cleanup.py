"""Verify this spike's closed Alchemy runs against the Cloudflare API.

Usage: python3 harness/verify-cleanup.py <task-results-directory>
Read-only: it does not destroy resources or expose credentials.
"""

import datetime
import json
import os
import pathlib
import sys
import urllib.error
import urllib.request


root = pathlib.Path(sys.argv[1]).resolve()
prefix = "sync-do-append-"
base = "https://api.cloudflare.com/client/v4/accounts/" + os.environ["CLOUDFLARE_ACCOUNT_ID"] + "/"
headers = {"Authorization": "Bearer " + os.environ["CLOUDFLARE_API_TOKEN"]}


def request(route, absent_ok=False):
    try:
        with urllib.request.urlopen(urllib.request.Request(base + route, headers=headers), timeout=30) as response:
            data = json.load(response)
            if not data.get("success"):
                raise RuntimeError("Cloudflare API did not report success")
            return response.status, data
    except urllib.error.HTTPError as error:
        if absent_ok and error.code == 404:
            return 404, None
        raise RuntimeError(f"Cloudflare API returned HTTP {error.code}") from None
    except urllib.error.URLError:
        raise RuntimeError("Cloudflare API could not be reached") from None


runs = []
workers = []
for name in ("deployed-attempt-1", "deployed"):
    directory = root / name
    cleanup = json.loads((directory / "cleanup.json").read_text())
    resources = json.loads((directory / "resources.json").read_text())
    private_removed = not pathlib.Path(resources["privateDirectory"]).exists()
    if not (cleanup["complete"] and cleanup["secretRemoved"] and private_removed):
        raise RuntimeError(f"Run {name} is not closed")
    runs.append({"directory": name, "run": resources["run"], "cleanupReceipt": f"{name}/cleanup.json", "privateDirectoryRemoved": private_removed})
    for target in resources["targets"]:
        if not target["cleanupComplete"]:
            raise RuntimeError("An Alchemy target is still open")
        worker = prefix + target["stage"]
        status, _ = request("workers/scripts/" + worker, absent_ok=True)
        if status != 404:
            raise RuntimeError("A task Worker still exists")
        workers.append({"worker": worker, "status": status})

_, scripts = request("workers/scripts")
worker_matches = [row["id"] for row in scripts["result"] if row.get("id", "").startswith(prefix)]
namespace_matches = []
for page in range(1, 101):
    _, namespaces = request(f"workers/durable_objects/namespaces?page={page}&per_page=100")
    namespace_matches.extend(
        {key: row.get(key) for key in ("id", "name", "script")}
        for row in namespaces["result"]
        if (row.get("script") or "").startswith(prefix) or (row.get("name") or "").startswith(prefix)
    )
    if len(namespaces["result"]) < 100:
        break
else:
    raise RuntimeError("Namespace listing exceeded its bound")
if worker_matches or namespace_matches:
    raise RuntimeError("Task-prefixed Cloudflare resources remain")

result = {
    "complete": True,
    "verifiedAtUtc": datetime.datetime.now(datetime.timezone.utc).isoformat(),
    "method": "Alchemy destroy receipts, individual Worker HTTP 404 checks, and account-wide Worker/DO namespace prefix scans",
    "resourcePrefix": prefix,
    "runs": runs,
    "workersVerifiedAbsent": workers,
    "remainingWorkers": worker_matches,
    "remainingDurableObjectNamespaces": namespace_matches,
    "privateStateAndAuthRemoved": all(row["privateDirectoryRemoved"] for row in runs),
}
(root / "cleanup.json").write_text(json.dumps(result, indent=2) + "\n")
print(json.dumps({"cleanupVerified": True, "workersAbsent": len(workers), "matchingNamespaces": 0, "privateStateAndAuthRemoved": True}))
