// Fixed ABBA BAAB reduction. Pure offline data processing; no I/O or clocks.
const ROUNDS = [false, true, true, false, true, false, false, true];
const ROLES = ["primary", "control"];
const finite = Number.isFinite;
const nonempty = (value) => typeof value === "string" && value.length > 0;
const mean = (values) => values.reduce((total, value) => total + value, 0) / values.length;
const stats = (values) => {
  const sorted = values.filter(finite).sort((a, b) => a - b);
  const q = (p) => {
    if (!sorted.length) return null;
    const at = (sorted.length - 1) * p;
    const lo = Math.floor(at);
    return sorted[lo] + (sorted[Math.ceil(at)] - sorted[lo]) * (at - lo);
  };
  return { n: sorted.length, median: q(.5), q1: q(.25), q3: q(.75), min: sorted[0] ?? null, max: sorted.at(-1) ?? null };
};
const group = (items, key) => {
  const groups = new Map();
  for (const item of items) {
    const k = key(item);
    if (!groups.has(k)) groups.set(k, []);
    groups.get(k).push(item);
  }
  return groups;
};
const firstEntry = (entry) => entry?.firstHarnessRequest === true && entry.priorAlarmStarts === 0 && Array.isArray(entry.activeAlarmIds) && entry.activeAlarmIds.length === 0;
const roundOf = (value) => Number.isInteger(value) ? value : typeof value === "string" && /^[0-7]$/.test(value) ? Number(value) : null;
const flagOf = (value) => value === true || value === "true" ? true : value === false || value === "false" ? false : null;
const rowKey = (row) => `${row.role}/${row.framework ?? "yielded"}/${row.object}`;
const cohortKey = (cohort) => `${cohort.role}/${cohort.framework}/${cohort.object}`;
const deploymentKey = (row) => `${row.role}/${row.round}`;
const digest = (deployment) => deployment.bundleSha256 ?? deployment.hash;

/**
 * records: parent reducer rows, with flat metricNames fields, eligible,
 * coldVerified and round-specific reset evidence already evaluated by the parent.
 * minificationRound/minified accept their exact URL strings or decoded values.
 * Completion firstEntry is the returned lifetime-counter object, not a boolean.
 */
export function analyzeMinification(records, plan, metricNames) {
  const metrics = [...new Set(metricNames)];
  const rows = records.filter((row) => row.phase === "minification");
  const cohorts = plan?.cohorts ?? [];
  const deployments = plan?.deployments ?? [];
  const completed = plan?.completed ?? [];
  const issues = [];
  if (JSON.stringify(plan?.rounds) !== JSON.stringify(ROUNDS)) issues.push("round_schedule_mismatch");
  if (plan?.complete !== true) issues.push("plan_incomplete_estimates_withheld");
  if (plan?.builds) {
    const { plain, minified } = plan.builds;
    for (const field of ["repositoryCommit", "inputsSha256", "fixtureSha256", "esbuildVersion", "effectVersion", "target"]) {
      if (!nonempty(plain?.[field]) || plain[field] !== minified?.[field]) issues.push(`build_mismatch:${field}`);
    }
    if (plain?.minify !== false || minified?.minify !== true) issues.push("build_flag_mismatch");
  }
  const estimatesEnabled = issues.length === 0;
  const byDeployment = group(deployments, deploymentKey);
  const byVersion = group(deployments, (d) => `${d.role}/${d.version}`);
  const byCompletion = group(completed, (c) => `${c.key}/${c.round}`);
  const byCohort = group(cohorts, cohortKey);
  const byObject = group(rows, rowKey);
  const deploymentChecks = [];
  const validDeployment = new Map();
  for (const role of ROLES) for (let round = 0; round < ROUNDS.length; round++) {
    const matches = byDeployment.get(`${role}/${round}`) ?? [];
    const reasons = [];
    const d = matches[0];
    const expectedMinified = role === "primary" && ROUNDS[round];
    if (matches.length !== 1) reasons.push("expected_one_deployment");
    if (d) {
      if (d.minify !== expectedMinified) reasons.push("deployment_flag_mismatch");
      if (!nonempty(d.version) || byVersion.get(`${role}/${d.version}`)?.length !== 1) reasons.push("missing_or_reused_version");
      const expectedBuild = plan?.builds?.[expectedMinified ? "minified" : "plain"];
      if (!nonempty(digest(d)) || (expectedBuild && digest(d) !== expectedBuild.bundleSha256)) reasons.push("deployment_hash_mismatch");
      // All plain uploads, including both Workers, must share executable bytes.
      const hashes = new Set(deployments.filter((x) => x.minify === expectedMinified).map(digest));
      if (hashes.size !== 1) reasons.push("flavor_hashes_differ");
      if (d.startupEvidence?.version !== d.version) reasons.push("startup_version_mismatch");
    }
    const check = { role, round, minified: expectedMinified, version: d?.version ?? null, reasons };
    deploymentChecks.push(check);
    if (!reasons.length) validDeployment.set(`${role}/${round}`, d);
  }
  const unexpectedDeployments = deployments.filter((d) => !ROLES.includes(d.role) || !Number.isInteger(d.round) || d.round < 0 || d.round > 7);
  const unexpectedRows = rows.filter((row) => !byCohort.has(rowKey(row))).map((row) => ({ key: rowKey(row), sample: row.sample, round: row.minificationRound, reason: "unplanned_object" }));
  const unexpectedCompletions = completed.filter((c) => !byCohort.has(c.key) || !Number.isInteger(c.round) || c.round < 0 || c.round > 7).map((c) => ({ key: c.key, round: c.round, reason: "unplanned_completion" }));
  const admitted = [];
  const exclusions = [];
  for (const [key, planned] of byCohort) {
    const cohort = planned[0];
    const own = byObject.get(key) ?? [];
    const reasons = [];
    const rowIssues = [];
    if (planned.length !== 1) reasons.push("duplicate_planned_object");
    if (!ROLES.includes(cohort.role) || cohort.framework !== "yielded" || cohort.history !== 50 || cohort.ttftMs !== 0) reasons.push("cohort_shape_mismatch");
    if (own.length !== 8) reasons.push("expected_eight_rows");
    if (own.some((r) => roundOf(r.minificationRound) === null || roundOf(r.minificationRound) < 0 || roundOf(r.minificationRound) > 7)) reasons.push("unexpected_round");
    const ordered = [];
    for (let round = 0; round < ROUNDS.length; round++) {
      const matches = own.filter((r) => roundOf(r.minificationRound) === round);
      const row = matches[0];
      const cs = byCompletion.get(`${key}/${round}`) ?? [];
      const completion = cs[0];
      const d = validDeployment.get(`${cohort.role}/${round}`);
      const errors = [];
      if (matches.length !== 1) errors.push("expected_one_row");
      if (cs.length !== 1) errors.push("expected_one_completion");
      if (!d) errors.push("invalid_deployment");
      if (row) {
        if (row.eligible !== true) errors.push("parent_ineligible");
        if (row.coldVerified !== true) errors.push("cold_not_verified");
        if (row.history !== 50 || row.ttftMs !== 0 || row.sample !== `m${8 + round}`) errors.push("workload_mismatch");
        if (flagOf(row.minified) !== (cohort.role === "primary" && ROUNDS[round])) errors.push("row_flag_mismatch");
        if (!nonempty(row.version) || row.version !== d?.version) errors.push("row_version_mismatch");
        if (!nonempty(row.incarnation) || row.incarnation !== completion?.incarnation || row.version !== completion?.version || (completion?.sample !== undefined && completion.sample !== row.sample)) errors.push("completion_identity_mismatch");
        if (!firstEntry(completion?.firstEntry)) errors.push("completion_not_first_entry");
        ordered.push(row);
      }
      if (errors.length) rowIssues.push({ round, reasons: errors });
    }
    if (rowIssues.length) reasons.push("invalid_rounds");
    if (new Set(own.map((row) => row.incarnation)).size !== 8) reasons.push("expected_eight_fresh_incarnations");
    const objectIds = own.map((row) => row.objectId).filter(nonempty);
    if (objectIds.length && (objectIds.length !== 8 || new Set(objectIds).size !== 1)) reasons.push("object_identity_changed_or_missing");
    if (!estimatesEnabled) reasons.push("plan_not_admitted");
    if (reasons.length) {
      exclusions.push({ key, role: cohort.role, object: cohort.object, observedRows: own.length, reasons, rounds: rowIssues });
      continue;
    }
    const values = {};
    for (const metric of metrics) {
      const missingRounds = ordered.flatMap((row, round) => finite(row[metric]) ? [] : [round]);
      const plainPositionMean = missingRounds.length ? null : mean(ordered.filter((_, round) => !ROUNDS[round]).map((row) => row[metric]));
      const minPositionMean = missingRounds.length ? null : mean(ordered.filter((_, round) => ROUNDS[round]).map((row) => row[metric]));
      values[metric] = { plainPositionMean, minPositionMean, contrast: missingRounds.length ? null : plainPositionMean - minPositionMean, missingRounds };
    }
    admitted.push({ key, role: cohort.role, object: cohort.object, values, rows: ordered });
  }
  const perObject = admitted.map(({ rows: own, ...unit }) => ({ ...unit, rounds: own.map((row, round) => ({ round, sample: row.sample, version: row.version, incarnation: row.incarnation, minified: flagOf(row.minified) })) }));
  const roundSummaries = ROLES.flatMap((role) => ROUNDS.map((minified, round) => {
    const own = admitted.filter((unit) => unit.role === role).map((unit) => unit.rows[round]);
    return { role, round, minified: role === "primary" && minified, objects: own.length, metrics: Object.fromEntries(metrics.map((metric) => [metric, stats(own.map((row) => row[metric]))])) };
  }));
  const contrasts = ROLES.map((role) => {
    const own = admitted.filter((unit) => unit.role === role);
    return { role, objects: own.length, metrics: Object.fromEntries(metrics.map((metric) => [metric, stats(own.map((unit) => unit.values[metric].contrast))])) };
  });
  const adjustedUnits = [];
  const unpaired = [];
  const names = new Set(cohorts.map((cohort) => cohort.object));
  for (const object of names) {
    const primary = admitted.find((unit) => unit.role === "primary" && unit.object === object);
    const control = admitted.find((unit) => unit.role === "control" && unit.object === object);
    if (!primary || !control) { unpaired.push({ object, missingRoles: ROLES.filter((role) => !admitted.some((unit) => unit.role === role && unit.object === object)) }); continue; }
    adjustedUnits.push({ object, metrics: Object.fromEntries(metrics.map((metric) => {
      const p = primary.values[metric].contrast;
      const c = control.values[metric].contrast;
      return [metric, finite(p) && finite(c) ? p - c : null];
    })) });
  }
  const startup = ROLES.flatMap((role) => [false, true].filter((minified) => role === "primary" || !minified).map((minified) => {
    const rounds = ROUNDS.flatMap((flag, round) => (role === "primary" ? flag : false) === minified ? [round] : []);
    const uploads = rounds.map((round) => {
      const d = validDeployment.get(`${role}/${round}`);
      const value = d?.startupTimeMs;
      const valid = estimatesEnabled && d !== undefined && finite(value) && value >= 0 && d.startupEvidence?.startupTimeMs === value;
      return { round, version: d?.version ?? null, bundleSha256: d ? digest(d) : null, bundleBytes: d?.bundleBytes ?? null, gzipBytes: d?.gzipBytes ?? null, startupTimeMs: finite(value) ? value : null, admitted: valid, reason: valid ? null : !estimatesEnabled ? "plan_not_admitted" : !d ? "invalid_deployment" : "missing_or_inconsistent_startup" };
    });
    return { role, minified, expectedUploads: rounds.length, stats: stats(uploads.filter((u) => u.admitted).map((u) => u.startupTimeMs)), uploads };
  }));
  return {
    complete: plan?.complete === true, estimatesEnabled,
    unit: "Eight valid cold requests per Object; mean of four plain positions minus mean of four minified positions, then median [Q1–Q3] across Objects. Control uses the same positions while all its uploads remain plain.",
    positiveMeans: "Lower values in primary minified positions; adjusted contrast subtracts the paired control position contrast.",
    schedule: ROUNDS, issues,
    coverage: { expectedObjectsPerRole: 7, expectedRequests: 112, observedRequests: rows.length, parentEligible: rows.filter((r) => r.eligible === true).length, coldVerified: rows.filter((r) => r.coldVerified === true).length, admittedRequests: admitted.length * 8, roles: ROLES.map((role) => ({ role, plannedObjects: cohorts.filter((c) => c.role === role).length, admittedObjects: admitted.filter((c) => c.role === role).length, excludedObjects: exclusions.filter((c) => c.role === role).length })) },
    perObject, roundSummaries, contrasts,
    adjusted: { pairs: adjustedUnits.length, units: adjustedUnits, metrics: Object.fromEntries(metrics.map((metric) => [metric, stats(adjustedUnits.map((unit) => unit.metrics[metric]))])), unpaired },
    exclusions, unexpectedRows, unexpectedCompletions, deploymentChecks, unexpectedDeployments,
    startup: { unit: "One unique Worker-version upload, never one turn or Object", groups: startup },
    limits: [
      "Quartiles and ranges are descriptive, not confidence intervals. Seven Objects share each upload; there are only four primary uploads per flavor and eight control uploads.",
      "The single ABBA BAAB sequence balances linear progression; it does not remove Worker-specific nonlinear drift, carryover, routing or placement differences.",
      "Do not claim a causal improvement below the observed identical-code control spread; an effect exceeding that spread is not by itself causal proof.",
      "Objects retain growing canonical history (58–65 completed turns before m8–m15). Cold means a verified fresh Object incarnation, not a guaranteed fresh isolate or JIT.",
      "Missing metric values exclude the whole eight-round Object contrast for that metric; adjusted contrasts require both Objects. Available round metrics and upload startup have their own n.",
      "Upload startup and client/DO invocation times measure different scopes and must not be added or subtracted as a latency partition.",
    ],
  };
}
