"""Deploy fresh paired samples, capture in Cloudflare, and remove only this run.

The controller is distilled from the benchmark's deployment workflow. Python's
standard library is sufficient; all timed events happen in the driver Worker.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import signal
import shutil
import subprocess
import time
import urllib.error
import urllib.request
import uuid

ROOT = Path(__file__).resolve().parents[1]
ARMS = ["yielded-single", "pi-single", "yielded-split", "pi-split",
        "yielded-empty", "pi-empty"]


def save(path, value):
    path.write_text(json.dumps(value, indent=2) + "\n")


class Run:
    def __init__(self, out, existing=False):
        self.account = os.environ["CLOUDFLARE_ACCOUNT_ID"]
        self.api_token = os.environ["CLOUDFLARE_API_TOKEN"]
        self.token = uuid.uuid4().hex + uuid.uuid4().hex
        self.domain = ""
        self.out = out
        self.names = []
        self.uploads = {}
        self.last_api = 0
        if existing:
            state = json.loads((out / "state.json").read_text())
            self.prefix, self.names = state["prefix"], state["names"]
        else:
            self.prefix = "press-cs-" + uuid.uuid4().hex[:10]
            out.mkdir(parents=True, exist_ok=False)
            self.state()
            # Snapshot executable inputs, excluding installed packages and generated caches.
            for folder in ["src", "scripts", "third-party/src"]:
                for source in (ROOT / folder).glob("*.*"):
                    if source.is_file():
                        dest = out / "source" / source.relative_to(ROOT)
                        dest.parent.mkdir(parents=True, exist_ok=True)
                        shutil.copyfile(source, dest)
            for source in (ROOT / "dist").glob("*.map"):
                shutil.copyfile(source, out / source.name)
        if not self.prefix.startswith("press-cs-") or any(
                not n.startswith(self.prefix + "-") for n in self.names):
            raise ValueError("Refusing cleanup outside the exact run prefix")

    def state(self):
        save(self.out / "state.json", {"prefix": self.prefix, "names": self.names})

    def redact(self, text):
        for value in [self.account, self.api_token, self.token, self.domain]:
            if value:
                text = text.replace(value, "[redacted]")
        return text

    def api(self, path, method="GET", data=None, content_type="application/json", absent=False, attempt=0):
        time.sleep(max(0, 1.0 - (time.monotonic() - self.last_api)))
        self.last_api = time.monotonic()
        if data is not None and not isinstance(data, bytes):
            data = json.dumps(data).encode()
        request = urllib.request.Request(
            f"https://api.cloudflare.com/client/v4/accounts/{self.account}/{path}",
            data=data, method=method, headers={"Authorization": "Bearer " + self.api_token,
                                              "Content-Type": content_type})
        try:
            with urllib.request.urlopen(request, timeout=90) as response:
                result = json.load(response)
        except urllib.error.HTTPError as error:
            if absent and error.code == 404:
                return None
            if method == "GET" and error.code in [408, 429, 500, 502, 503, 504] and attempt < 6:
                delay = error.headers.get("Retry-After", "")
                time.sleep(max(2 ** attempt, float(delay) if delay.isdigit() else 0))
                return self.api(path, method, data, content_type, absent, attempt + 1)
            raise RuntimeError(self.redact(f"API {method} {path}: HTTP {error.code} " +
                                          error.read().decode())[:1600]) from None
        if not result.get("success"):
            raise RuntimeError(self.redact(json.dumps(result)))
        return result["result"]

    def namespaces(self):
        rows = []
        for page in range(1, 101):
            batch = self.api(f"workers/durable_objects/namespaces?per_page=1000&page={page}")
            rows.extend(batch)
            if len(batch) < 1000:
                return list({r["id"]: r for r in rows}.values())
        raise RuntimeError("Namespace pagination did not finish")

    def capacity(self, additional):
        rows = self.namespaces()
        owned = [r for r in rows if r.get("script", "").startswith("press-cs-") or
                 r.get("name", "").startswith("press-cs-")]
        # Conservative documented paid-plan default; never assume a raised limit.
        check = {"total": len(rows), "owned": len(owned), "additional": additional,
                 "spareAfter": 500 - len(rows) - additional}
        with (self.out / "capacity.jsonl").open("a") as output:
            output.write(json.dumps(check) + "\n")
        if check["spareAfter"] < 100 or len(owned) + additional > 40:
            raise RuntimeError("Need >=100 spare namespaces and <=40 owned by press-cs-")

    def version(self, name):
        result = self.api(f"workers/scripts/{name}/deployments", absent=True)
        deployments = result.get("deployments", []) if result else []
        if deployments and len(deployments[0]["versions"]) == 1:
            return deployments[0]["versions"][0]["version_id"]
        return None

    def upload(self, suffix, bundle, klass=None, host=None, observable=False):
        name = self.prefix + "-" + suffix
        additional = int(klass is not None and host is None and name not in self.names)
        self.capacity(additional)
        bindings = [{"type": "secret_text", "name": "TOKEN", "text": self.token},
                    {"type": "version_metadata", "name": "VERSION"},
                    {"type": "plain_text", "name": "LOCATION_HINT", "text": "wnam"},
                    {"type": "plain_text", "name": "PREFIX", "text": self.prefix},
                    {"type": "plain_text", "name": "SUBDOMAIN", "text": self.domain},
                    {"type": "plain_text", "name": "PROVIDER_URL",
                     "text": f"https://{self.prefix}-provider.{self.domain}.workers.dev/v1"}]
        if klass:
            binding = {"type": "durable_object_namespace",
                       "name": "DRIVER" if klass == "Driver" else "THREAD", "class_name": klass}
            if host:
                binding["script_name"] = host
            bindings.append(binding)
        metadata = {"main_module": "worker.mjs", "compatibility_date": "2026-08-18",
                    "compatibility_flags": ["nodejs_compat", "global_fetch_strictly_public"],
                    "bindings": bindings, "observability": {"enabled": observable,
                    "head_sampling_rate": 1 if observable else 0}}
        if additional:
            metadata["migrations"] = {"new_tag": "v1", "steps": [{"new_sqlite_classes": [klass]}]}
        # Inert but unique bytes for every uploaded sample; no target health request.
        source = (ROOT / "dist" / (bundle + ".mjs")).read_bytes()
        base_hash = hashlib.sha256(source).hexdigest()
        source += f"\n// sample {name} {uuid.uuid4().hex}\n".encode()
        boundary = "----coldstart" + uuid.uuid4().hex
        parts = [("metadata", "metadata.json", "application/json", json.dumps(metadata).encode()),
                 ("worker.mjs", "worker.mjs", "application/javascript+module", source),
                 (bundle + ".mjs.map", bundle + ".mjs.map", "application/source-map",
                  (ROOT / "dist" / (bundle + ".mjs.map")).read_bytes())]
        body = b"".join((f'--{boundary}\r\nContent-Disposition: form-data; name="{field}"; '
                         f'filename="{filename}"\r\nContent-Type: {kind}\r\n\r\n').encode() +
                        value + b"\r\n" for field, filename, kind, value in parts)
        if name not in self.names:
            self.names.append(name)
            self.state()  # Record intent before mutation, including ambiguous failures.
        result = self.api("workers/scripts/" + name, "PUT", body + f"--{boundary}--\r\n".encode(),
                          "multipart/form-data; boundary=" + boundary)
        version = result.get("version_id")
        if not version:
            for _ in range(20):
                version = self.version(name)
                if version:
                    break
                time.sleep(2)
        if not version:
            raise RuntimeError("Uploaded version is not visible")
        self.uploads[name] = {"version": version, "sha256": hashlib.sha256(source).hexdigest(),
                              "startupMs": result.get("startup_time_ms"), "bundle": bundle,
                              "bundleSha256": base_hash,
                              "uploadedAt": time.time(), "host": host}
        (self.out / (name + ".mjs")).write_bytes(source)
        save(self.out / "uploads.json", self.uploads)
        self.api(f"workers/scripts/{name}/subdomain", "POST", {"enabled": True, "previews_enabled": False})
        print("Uploaded", suffix, flush=True)
        return name

    def invoke(self, name, path, body=None, failure_path=None):
        request = urllib.request.Request(
            f"https://{name}.{self.domain}.workers.dev{path}",
            data=None if body is None else json.dumps(body).encode(),
            headers={"Authorization": "Bearer " + self.token, "Content-Type": "application/json",
                     "User-Agent": "durable-bench/1.0"})
        try:
            with urllib.request.urlopen(request, timeout=90) as response:
                data = response.read()
                return json.loads(data) if "application/json" in response.headers.get("Content-Type", "") else data.decode()
        except urllib.error.HTTPError as error:
            detail = self.redact(error.read().decode(errors="replace"))
            if failure_path is not None:
                # Preserve partial Driver frames before teardown deletes its stored copy.
                save(failure_path, {"error": f"Target HTTP {error.code}", "responseBody": detail})
            raise RuntimeError(f"Target HTTP {error.code}: " + detail[:2000]) from None

    def ready(self, name):
        for attempt in range(40):
            try:
                return self.invoke(name, "/health")
            except (RuntimeError, urllib.error.URLError):
                if attempt == 39:
                    raise
                time.sleep(3)

    def cleanup(self):
        errors = []
        pending = list(reversed(self.names))
        for attempt in range(10):
            for name in pending:
                try:
                    self.api(f"workers/scripts/{name}?force=true", "DELETE", absent=True)
                except Exception as error:
                    errors.append({"attempt": attempt, "name": name, "error": self.redact(str(error))})
            try:
                remaining = [r["id"] for r in self.api("workers/scripts") if r["id"] in self.names]
                namespaces = [r for r in self.namespaces() if r.get("script") in self.names]
                versions = []
                for name in self.names:
                    result = self.api(f"workers/scripts/{name}/versions", absent=True)
                    if result and result.get("items"):
                        versions.append(name)
            except Exception as error:
                errors.append({"attempt": attempt, "inventoryError": self.redact(str(error))})
                save(self.out / "cleanup.json", {"verified": False, "deleteHistory": errors})
                time.sleep(3)
                continue
            result = {"workers": remaining, "namespaces": [r["id"] for r in namespaces],
                      "versions": versions, "deleteHistory": errors,
                      "verified": not (remaining or namespaces or versions)}
            save(self.out / "cleanup.json", result)
            if result["verified"]:
                print("Teardown verified: zero Workers, namespaces, and uploaded versions", flush=True)
                return
            pending = list(set(remaining + versions + [r["script"] for r in namespaces]))
            time.sleep(3)
        raise RuntimeError("Teardown incomplete; rerun cleanup with the same output directory")

    def measure(self, blocks):
        self.domain = self.api("workers/subdomain")["subdomain"]
        self.upload("provider", "provider")
        driver = self.upload("driver", "driver", "Driver")
        self.ready(self.prefix + "-provider")
        health = self.ready(driver)
        cells = []
        for block in range(blocks):
            order = ARMS[block // 2 % len(ARMS):] + ARMS[:block // 2 % len(ARMS)]
            if block % 2:
                order = list(reversed(order))
            for arm in order:
                framework, shape = arm.split("-")
                suffix = f"b{block}-{arm}"
                klass = "EmptyObject" if shape == "empty" else "Thread"
                host = self.upload(suffix, framework, klass)
                front = host if shape == "single" else self.upload(suffix + "-front", "front", klass, host)
                cells.append({"block": block, "arm": arm, "host": host, "front": front})
        save(self.out / "plan.json", {"cells": cells, "driver": health, "blocks": blocks,
             "state": "brand-new Thread", "locationHint": "wnam", "propagationSeconds": 180,
             "commit": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip()})
        print("All targets untouched; waiting 180 seconds after final upload", flush=True)
        time.sleep(180)
        confirmations = {}
        for name, uploaded in self.uploads.items():
            version = self.version(name)
            confirmations[name] = version
            if version != uploaded["version"]:
                raise RuntimeError("Deployed version does not match upload")
        save(self.out / "confirmed-versions.json", confirmations)
        for cell in cells:
            path = self.out / f"capture-b{cell['block']}-{cell['arm']}.json"
            result = self.invoke(driver, "/capture", {"name": cell["front"], "sample": "first"},
                                 failure_path=path)
            save(path, result)
            print("Captured", cell["block"], cell["arm"], flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=["run", "cleanup"])
    parser.add_argument("--out", type=Path, required=True, help="New private output directory; cleanup reuses it")
    parser.add_argument("--blocks", type=int, default=6, help="Even number, 2..6; run two deployments for 12 pairs")
    args = parser.parse_args()
    if args.blocks not in [2, 4, 6]:
        parser.error("--blocks must be 2, 4, or 6")
    run = Run(args.out.resolve(), existing=args.command == "cleanup")
    if args.command == "cleanup":
        run.cleanup()
        return
    def interrupted(_signal, _frame):
        raise KeyboardInterrupt()
    signal.signal(signal.SIGTERM, interrupted)
    try:
        run.measure(args.blocks)
    except BaseException as error:
        save(run.out / "failure.json", {"error": run.redact(str(error))})
        raise
    finally:
        run.cleanup()


if __name__ == "__main__":
    main()
