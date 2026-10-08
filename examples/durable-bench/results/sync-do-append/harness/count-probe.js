globalThis.__kom433 = (() => {
  const key = "~effect/Fiber/currentFiber";
  let stacks = new WeakMap();
  let outside = [];
  let active;
  const sqlSpans = new WeakMap();
  const rows = [];
  const empty = () => ({ evaluations: 0, inlineSuccesses: 0, allocations: 0, calls: 0, sqlStatements: 0, syncTransactions: 0 });
  const bucket = (table, name) => table[name] ??= empty();
  const stack = (fiber = globalThis[key]) => fiber ? (stacks.get(fiber) ?? []) : outside;
  const bump = (kind, name, fiber) => {
    if (!active) return;
    active.total[kind]++;
    if (kind === "evaluations") active.operations[name] = (active.operations[name] ?? 0) + 1;
    if (kind === "allocations") active.allocationSites[name] = (active.allocationSites[name] ?? 0) + 1;
    const entries = stack(fiber).filter((token) => token.open);
    const stages = [...new Set(entries.map((token) => token.stage).filter(globalThis.Boolean))];
    bucket(active.exclusiveStages, stages.at(-1) ?? "unattributed")[kind]++;
    for (const stage of stages) {
      bucket(active.inclusiveStages, stage)[kind]++;
      bucket(active.stageSites[stage] ??= {}, entries.at(-1)?.site ?? "unattributed")[kind]++;
    }
    for (const site of new Set(entries.map((token) => token.site))) bucket(active.sites, site)[kind]++;
    bucket(active.exclusiveSites, entries.at(-1)?.site ?? "unattributed")[kind]++;
    for (const token of entries) token.counts[kind]++;
  };
  return {
    inherit(child, parent) { stacks.set(child, [...stack(parent)]); },
    evaluation(op, fiber) { bump("evaluations", op ?? "unknown", fiber); },
    inline(fiber) { bump("inlineSuccesses", "succeedWith", fiber); },
    allocation(site) { bump("allocations", site); },
    sqlSpan(span) { sqlSpans.set(span, this.enter("effect/sql/Statement.useSpan", null)); },
    closeSqlSpan(span) { this.leave(sqlSpans.get(span)); sqlSpans.delete(span); },
    transactionSync(storage, ...args) { bump("syncTransactions", "transactionSync"); return storage.transactionSync(...args); },
    exec(sql, ...args) {
      bump("sqlStatements", "exec");
      if (active) {
        const statement = args[0].replace(/\s+/g, " ").trim();
        const row = active.sql[statement] ??= { calls: 0, stages: {}, sites: {} };
        row.calls++;
        const entries = stack().filter((token) => token.open);
        const stage = entries.findLast((token) => token.stage)?.stage ?? "unattributed";
        row.stages[stage] = (row.stages[stage] ?? 0) + 1;
        const site = entries.at(-1)?.site ?? "unattributed";
        row.sites[site] = (row.sites[site] ?? 0) + 1;
      }
      return sql.exec(...args);
    },
    enter(site, stage) {
      if (!active) return undefined;
      const fiber = globalThis[key];
      let entries = stack(fiber);
      if (fiber && !stacks.has(fiber)) stacks.set(fiber, entries);
      const token = { site, stage, open: true, entries, counts: empty(), outerAppend: stage === "durable-object-append" && !entries.some((entry) => entry.open && entry.stage === stage) };
      const parent = entries.findLast((entry) => entry.open)?.site ?? "unattributed";
      const edge = `${parent} -> ${site}`;
      active.nesting[edge] = (active.nesting[edge] ?? 0) + 1;
      entries.push(token);
      active.open.add(token);
      active.total.calls++;
      bucket(active.sites, site).calls++;
      if (stage && !entries.slice(0, -1).some((entry) => entry.open && entry.stage === stage)) {
        bucket(active.inclusiveStages, stage).calls++;
        bucket(active.exclusiveStages, stage).calls++;
      }
      return token;
    },
    leave(token) {
      if (!token) return;
      token.open = false;
      if (token.outerAppend) active?.appends.push({ site: token.site, ...token.counts });
      const at = token.entries.indexOf(token);
      if (at >= 0) token.entries.splice(at, 1);
      active?.open.delete(token);
    },
    begin(input) {
      if (active) throw new Error("Overlapping counted turns");
      stacks = new WeakMap();
      outside = [];
      active = { input, total: empty(), operations: {}, allocationSites: {}, exclusiveStages: {}, inclusiveStages: {}, stageSites: {}, sites: {}, exclusiveSites: {}, sql: {}, appends: [], nesting: {}, open: new Set() };
    },
    end() {
      const { open, ...row } = active;
      row.unclosedSites = [...open].map((token) => token.site);
      for (const token of open) token.open = false;
      rows.push(row);
      active = undefined;
    },
    rows() { return rows; },
  };
})();
