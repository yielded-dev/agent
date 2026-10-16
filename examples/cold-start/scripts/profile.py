"""Try on-demand profiling before module loading and during the first turn.

This is a separate run: attaching or touching an Object contaminates a cold
latency sample. No second/warm turn is dispatched.
"""
import argparse
from concurrent.futures import ThreadPoolExecutor
import json
import gzip
from pathlib import Path
import time
import urllib.error
import urllib.request

from measure import Run, save

if not __debug__:
    raise RuntimeError("Qualification requires Python assertions; disable optimization")


def capture(run, name, label, namespace=None, actor=None):
    version = run.uploads[name]["version"]
    body = {"duration_ms": 5000, "profile_type": "cpu"}
    if namespace is not None:
        body.update(namespace_id=namespace, actor_id=actor)
    request = urllib.request.Request(
        f"https://api.cloudflare.com/client/v4/accounts/{run.account}/workers/workers/{name}/versions/{version}/profile",
        data=json.dumps(body).encode(), headers={"Authorization": "Bearer " + run.api_token,
                                               "Content-Type": "application/json"})
    result = {"label": label, "version": version, "startedAt": time.time()}
    try:
        with urllib.request.urlopen(request, timeout=65) as response:
            data = response.read()
            (run.out / (label + ".pprof.gz")).write_bytes(data)
            result.update(status=response.status, bytes=len(data), gzipValid=bool(gzip.decompress(data)))
    except urllib.error.HTTPError as error:
        result.update(status=error.code, body=run.redact(error.read().decode()),
                      retryAfter=error.headers.get("Retry-After"))
    except Exception as error:
        result.update(error=run.redact(str(error)))
    result["finishedAt"] = time.time()
    save(run.out / (label + ".json"), result)
    print(label, result.get("status", "transport failure"), flush=True)
    if result.get("status") == 429:
        print("Waiting after profiler rate limit", flush=True)
        delay = result.get("retryAfter") or "300"
        time.sleep(float(delay) if delay.isdigit() else 300)
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", required=True, type=Path)
    args = parser.parse_args()
    run = Run(args.out.resolve())
    results = []
    try:
        run.domain = run.api("workers/subdomain")["subdomain"]
        provider = run.upload("provider", "provider")
        run.ready(provider)
        targets = [run.upload(arm, arm, "Thread", observable=True) for arm in ["yielded", "pi"]]
        print("Waiting 180 seconds without target traffic", flush=True)
        time.sleep(180)
        for name in targets:
            arm = name.rsplit("-", 1)[-1]
            assert run.version(name) == run.uploads[name]["version"]
            results.append(capture(run, name, arm + "-before-module-load"))
            actor = run.invoke(name, "/id")["actorId"]
            settings = run.api(f"workers/scripts/{name}/settings")
            namespace = next(b["namespace_id"] for b in settings["bindings"] if b["name"] == "THREAD")
            results.append(capture(run, name, arm + "-before-object-load", namespace, actor))
            touches = []
            for _ in range(8):
                touches.append(run.invoke(name, "/touch"))
                time.sleep(1)
            assert all(t["version"] == run.uploads[name]["version"] for t in touches)
            assert len({t["moduleId"] for t in touches}) == 1
            save(run.out / (arm + "-touches.json"), touches)
            # The API has no attachment acknowledgement. Preserve timestamps; this
            # race must not be described as proof that recording covered the turn.
            with ThreadPoolExecutor(max_workers=1) as pool:
                recording = pool.submit(capture, run, name, arm + "-first-turn", namespace, actor)
                time.sleep(0.3)
                start = time.time()
                try:
                    raw = run.invoke(name, "/capture?sample=first")
                    save(run.out / (arm + "-turn.json"), {"startedAt": start, "finishedAt": time.time(), "raw": raw})
                except Exception as error:
                    save(run.out / (arm + "-turn.json"), {"startedAt": start, "finishedAt": time.time(), "error": run.redact(str(error))})
                    raise
                finally:
                    results.append(recording.result())
                    save(run.out / "summary.json", results)
                frames = [json.loads(line) for line in raw.splitlines() if line]
                assert not any(f["type"] == "error" for f in frames), frames
                assert "".join(f["value"]["text"] for f in frames if f["type"] == "text") == "hello"
                assert frames[-1] == {"type": "done", "value": {"outcome": "completed", "calls": 1}}
                objects = [f["value"] for f in frames if f["type"] == "object"]
                assert len(objects) == 1
                assert objects[0]["version"] == run.uploads[name]["version"]
                assert objects[0]["moduleId"] == touches[-1]["moduleId"]
                assert objects[0]["calls"] == touches[-1]["calls"] + 1
            # The on-demand capture endpoint is rate limited independently of deploys.
            if name != targets[-1]:
                print("Spacing framework profile attempts by five minutes", flush=True)
                time.sleep(300)
        save(run.out / "summary.json", results)
    finally:
        run.cleanup()


if __name__ == "__main__":
    main()
