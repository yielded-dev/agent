globalThis.__kom433 = (() => {
  const key = "~effect/Fiber/currentFiber";
  let stacks = new WeakMap();
  let outside = [];
  let active;
  const rows = [];
  const empty = () => ({ evaluations: 0, inlineSuccesses: 0, allocations: 0, calls: 0 });
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
    for (const stage of stages) bucket(active.inclusiveStages, stage)[kind]++;
    for (const site of new Set(entries.map((token) => token.site))) bucket(active.sites, site)[kind]++;
  };
  return {
    inherit(child, parent) { stacks.set(child, [...stack(parent)]); },
    evaluation(op, fiber) { bump("evaluations", op ?? "unknown", fiber); },
    inline(fiber) { bump("inlineSuccesses", "succeedWith", fiber); },
    allocation(site) { bump("allocations", site); },
    enter(site, stage) {
      if (!active) return undefined;
      const fiber = globalThis[key];
      let entries = stack(fiber);
      if (fiber && !stacks.has(fiber)) stacks.set(fiber, entries);
      const token = { site, stage, open: true, entries };
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
      const at = token.entries.indexOf(token);
      if (at >= 0) token.entries.splice(at, 1);
      active?.open.delete(token);
    },
    begin(input) {
      if (active) throw new Error("Overlapping counted turns");
      stacks = new WeakMap();
      outside = [];
      active = { input, total: empty(), operations: {}, allocationSites: {}, exclusiveStages: {}, inclusiveStages: {}, sites: {}, nesting: {}, open: new Set() };
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
