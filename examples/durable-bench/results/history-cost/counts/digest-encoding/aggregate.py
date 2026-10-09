"""Summarize the retained deterministic captures; never open fixture databases."""
import argparse
import hashlib
import json
from pathlib import Path


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


def without_digest_scope(counts):
    return {key: value for key, value in counts.items() if not key.startswith("historyDigest.")}


def delta(baseline, candidate):
    return {
        key: {
            "baseline": baseline.get(key, 0),
            "candidate": candidate.get(key, 0),
            "change": candidate.get(key, 0) - baseline.get(key, 0),
            "percent": round(100 * (candidate.get(key, 0) / baseline[key] - 1), 6)
            if baseline.get(key)
            else None,
        }
        for key in sorted(baseline.keys() | candidate.keys())
        if baseline.get(key, 0) != candidate.get(key, 0)
    }


def sql_summary(capture):
    def total(predicate):
        return {
            metric: sum(value[metric] for key, value in capture["sql"].items() if predicate(key))
            for metric in ("calls", "rowsRead", "rowsWritten")
        }

    return {
        "all": total(lambda _: True),
        "readPrompt": total(lambda key: key.startswith("readPrompt|")),
        "planning": total(lambda key: key.startswith("readPrompt|SELECT sequence,")),
        "hydration": total(lambda key: key.startswith("readPrompt|SELECT thread_id,")),
        "queryGroups": len(capture["sql"]),
        "fullSqlSnapshotSha256": hashlib.sha256(canonical(capture["sql"]).encode()).hexdigest(),
    }


def view(capture):
    return {
        "firstCallback": capture["modelSnapshots"][0],
        "historyDigest": {
            key.removeprefix("historyDigest."): value
            for key, value in capture["modelSnapshots"][0].items()
            if key.startswith("historyDigest.")
        },
        "fullTurn": capture["counts"],
        "modelCallbacks": [
            {key: value for key, value in snapshot.items() if key in (
                "effectEvaluations", "schemaNodeCalls", "schemaRootDecoderCalls", "modelCalls",
                "journalProjections", "readPromptCalls", "fullEnvelopeDecodes",
                "historyDigest.effectEvaluations", "historyDigest.schemaNodeCalls",
                "historyDigest.schemaRootDecoderCalls",
            )}
            for snapshot in capture["modelSnapshots"]
        ],
        "visits": capture["visits"],
        "sql": sql_summary(capture),
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("captures", type=Path)
    parser.add_argument("--prior-baselines", type=Path)
    parser.add_argument("--expected-summary", type=Path)
    parser.add_argument("--candidate-revision")
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args()
    root = args.captures.resolve()
    allowed_changes = {
        "effectEvaluations", "schemaNodeCalls", "schemaRootDecoderCalls",
        "historyDigest.effectEvaluations", "historyDigest.schemaNodeCalls",
        "historyDigest.schemaRootDecoderCalls",
    }
    rows = []
    raw = {}
    prior = {}
    for size in (50, 250, 1000, 3500):
        baseline_path = root / f"baseline/yielded-baseline-{size}.json"
        candidate_path = root / f"candidate/yielded-digest-encoding-{size}.json"
        b = json.loads(baseline_path.read_text())
        c = json.loads(candidate_path.read_text())
        for field in ("baseline", "candidateSourceSha256", "baselineRecordsSha256", "archiveSha256",
                      "metadataSha256", "meta", "normalized", "visits", "sql", "fingerprint",
                      "seedFingerprint", "messages", "supplementalDigestScope"):
            assert b[field] == c[field], (size, "mismatch", field)
        assert b["supplementalDigestScope"] is True
        assert b["seedFingerprint"] == b["normalized"]["fingerprint"] == b["meta"]["fingerprint"]
        # Canonical transfer retains history and settlements, not historical execution leases.
        expected_tables = {**b["meta"]["tables"], "effect_agent_attempts": 0}
        assert b["normalized"]["tables"] == expected_tables, (size, "normalized table counts")
        assert len(b["modelSnapshots"]) == len(c["modelSnapshots"]) == 9
        assert b["counts"]["modelCalls"] == c["counts"]["modelCalls"] == 9
        assert b["counts"]["fullEnvelopeDecodes"] == c["counts"]["fullEnvelopeDecodes"] == 4
        assert set(delta(b["counts"], c["counts"])) <= allowed_changes
        for before, after in zip(b["modelSnapshots"], c["modelSnapshots"]):
            assert set(delta(before, after)) <= allowed_changes
            for key, value in before.items():
                if key.startswith("historyDigest."):
                    assert value == b["counts"][key]
                    assert after[key] == c["counts"][key]
        if args.prior_baselines:
            old_path = args.prior_baselines / f"yielded-baseline-{size}.json"
            old = json.loads(old_path.read_text())
            for field in ("normalized", "visits", "sql", "fingerprint", "seedFingerprint", "messages"):
                assert b[field] == old[field], (size, "prior baseline mismatch", field)
            assert without_digest_scope(b["counts"]) == without_digest_scope(old["counts"])
            assert [without_digest_scope(v) for v in b["modelSnapshots"]] == [
                without_digest_scope(v) for v in old["modelSnapshots"]
            ]
            prior[str(size)] = {"file": str(old_path), "sha256": sha(old_path)}
        raw[str(size)] = {
            variant: {"file": str(path.relative_to(root)), "sha256": sha(path)}
            for variant, path in (("baseline", baseline_path), ("candidate", candidate_path))
        }
        rows.append({
            "turns": size,
            "seedFingerprint": b["seedFingerprint"],
            "measuredFingerprint": b["fingerprint"],
            "messages": b["messages"],
            "normalizedTables": b["normalized"]["tables"],
            "baseline": view(b),
            "candidate": view(c),
            "delta": {
                "firstCallback": delta(b["modelSnapshots"][0], c["modelSnapshots"][0]),
                "historyDigest": delta(view(b)["historyDigest"], view(c)["historyDigest"]),
                "fullTurn": delta(b["counts"], c["counts"]),
            },
            "unchanged": [
                "seed and measured transcript fingerprints", "normalized canonical table counts",
                "all SQL query/call/row snapshots", "all record visit histograms",
                "all byte, record, projection, readPrompt and promptDecode counts",
                "all nine model callbacks and full-turn proof work",
            ],
        })
    result = {
        "baseline": b["baseline"],
        "candidateRevision": args.candidate_revision,
        "candidateSourceSha256": b["candidateSourceSha256"],
        "baselineRecordsSha256": b["baselineRecordsSha256"],
        "measurement": "Deterministic operation counts only; no elapsed-time or deployed-latency claim",
        "scope": {
            "firstCallback": "counter start through entry to the first scripted model callback",
            "historyDigest": "identical supplemental enter/finally-leave hooks around initial history digest",
            "fullTurn": "counter start through canonical settlement; proof work retained",
            "sql": "full-turn cursor totals; no added SQL snapshot query at callback entry",
        },
        "rows": rows,
        "rawCaptureSha256": raw,
        "priorBaselineEquivalence": prior,
    }
    if args.expected_summary:
        expected = json.loads(args.expected_summary.read_text())
        for field in ("baseline", "candidateRevision", "candidateSourceSha256", "baselineRecordsSha256", "rows"):
            assert result[field] == expected[field], ("published summary mismatch", field)
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(result, indent=2) + "\n")
    for row in rows:
        first = row["delta"]["firstCallback"]
        digest = row["delta"]["historyDigest"]
        print(json.dumps({"turns": row["turns"], "firstCallback": first, "historyDigest": digest}))


if __name__ == "__main__":
    main()
