import { createHash } from "node:crypto";
import { existsSync, readFileSync, writeFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { NodeRuntime } from "@effect/platform-node";
import { Effect, Schema } from "effect";

// Read-only API recheck after Alchemy destruction, using recorded namespace IDs
// as well as script names so an orphan cannot disappear from a name-only filter.
const CleanupVerificationError = Schema.TaggedError()("CleanupVerificationError", { message: Schema.String });
const here = dirname(fileURLToPath(import.meta.url));
const load = (path) => JSON.parse(readFileSync(join(here, path), "utf8"));
const save = (path, value) => writeFileSync(join(here, path), JSON.stringify(value, null, 2) + "\n");

NodeRuntime.runMain(Effect.tryPromise({
  try: async () => {
    const account = process.env.CLOUDFLARE_ACCOUNT_ID;
    const token = process.env.CLOUDFLARE_API_TOKEN;
    if (!account || !token) throw new Error("Cloudflare credentials are required in the environment");
    const captures = ["calibration/hosted", "calibration-race/hosted", "real-turn/hosted", "calibration/failed-startup"].map((lane) => ({ lane, resources: load(`${lane}/resources.json`) }));
    const accountSha256 = createHash("sha256").update(account).digest("hex");
    if (captures.some(({ resources }) => resources.accountSha256 !== accountSha256)) throw new Error("Account ownership mismatch");
    const targets = captures.flatMap((capture) => capture.resources.targets);
    const names = new Set(targets.map((target) => target.name));
    const ids = new Set(targets.flatMap((target) => (target.namespaces ?? []).map((namespace) => namespace.id)));
    if (names.size !== 13 || ids.size !== 12 || [...names].some((name) => !name.startsWith("effect-eval-cost-"))) throw new Error("Unexpected closed-capture inventory");
    const api = (route) => fetch(`https://api.cloudflare.com/client/v4/accounts/${account}/${route}`, {
      headers: { authorization: `Bearer ${token}` }, signal: AbortSignal.timeout(60_000),
    });
    const namespaces = [];
    let completeListing = false;
    let pages = 0;
    for (let page = 1; page <= 100; page++) {
      const response = await api(`workers/durable_objects/namespaces?page=${page}&per_page=100`);
      if (!response.ok) throw new Error(`Namespace API status ${response.status}`);
      const data = await response.json();
      if (!data.success || !Array.isArray(data.result)) throw new Error("Invalid namespace API response");
      namespaces.push(...data.result); pages++;
      if (data.result.length < 100) { completeListing = true; break; }
    }
    if (!completeListing) throw new Error("Namespace API listing exceeded its bound");
    const remaining = namespaces.filter((item) => ids.has(item.id) || names.has(item.script) || [...names].some((name) => item.name?.startsWith(name + "_"))).map(({ id, name, script }) => ({ id, name, script }));
    const workers = [];
    for (const name of names) workers.push({ name, status: (await api(`workers/scripts/${name}`)).status });
    const privateState = captures.map(({ lane, resources }) => ({ lane, directoryAbsent: !existsSync(resources.privateDirectory) }));
    const complete = remaining.length === 0 && workers.every((item) => item.status === 404) && privateState.every((item) => item.directoryAbsent);
    const result = { complete, checkedAt: new Date().toISOString(), accountSha256, workerNamesVerified: names.size, recordedNamespaceIdsVerified: ids.size,
      recordedNamespaceIds: [...ids].sort(), completeListing, pages, remainingNamespaces: remaining, workers, privateState };
    save("cleanup-api-recheck.json", result);
    if (!complete) throw new Error("Cleanup verification failed; inspect cleanup-api-recheck.json");
    save("cleanup.json", { ...load("cleanup.json"), finalApiRecheck: { receipt: "cleanup-api-recheck.json", checkedAt: result.checkedAt, complete,
      workerNamesVerified: names.size, recordedNamespaceIdsVerified: ids.size, privateStateAbsent: true } });
    console.log(JSON.stringify({ complete, workerNamesVerified: names.size, recordedNamespaceIdsVerified: ids.size, privateStateAbsent: true }));
  },
  catch: (cause) => new CleanupVerificationError({ message: cause instanceof Error ? cause.message : "Cleanup verification failed" }),
}));
