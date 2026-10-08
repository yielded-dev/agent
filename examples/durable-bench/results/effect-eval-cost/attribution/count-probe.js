globalThis.__kom433 = (() => {
  const key = "~effect/Fiber/currentFiber";
  let stacks = new WeakMap();
  let origins = new WeakMap();
  const iteratorTokens = new WeakMap();
  let currentIterator;
  const siteModules = new Map();
  const moduleOf = (site) => siteModules.get(site) ?? (site.startsWith("runtime-constructor:ExitPrimitive") || site.startsWith("runtime-constructor:PrimitiveImpl") ? "effect/dist/internal/core.js" : site.startsWith("runtime-constructor:") ? "effect/dist/internal/effect.js" : site);
  let outside = [];
  let active;
  const rows = [];
  const empty = () => ({ evaluations: 0, inlineSuccesses: 0, iteratorSuccesses: 0, allocations: 0, calls: 0 });
  const bucket = (table, name) => table[name] ??= empty();
  const stack = (fiber = globalThis[key]) => fiber ? (stacks.get(fiber) ?? []) : outside;
  const entries = (fiber) => stack(fiber).filter((token) => token.open);
  const bump = (kind, name, fiber, origin) => {
    if (!active) return;
    active.total[kind]++;
    if (kind === "evaluations") active.operations[name] = (active.operations[name] ?? 0) + 1;
    if (kind === "allocations") active.allocationKinds[name] = (active.allocationKinds[name] ?? 0) + 1;
    const open = entries(fiber);
    const stageEntries = open.map((token) => token.stage).filter((stage) => stage !== null && stage !== undefined);
    const stages = [...new Set(stageEntries)];
    bucket(active.exclusiveStages, stageEntries.at(-1) ?? "outside-eight-stages")[kind]++;
    for (const stage of stages) bucket(active.inclusiveStages, stage)[kind]++;
    const sites = [...new Set(open.map((token) => token.site))];
    bucket(active.exclusiveSites, open.at(-1)?.site ?? "unattributed")[kind]++;
    bucket(active.exclusiveModules, moduleOf(open.at(-1)?.site ?? "unattributed"))[kind]++;
    for (const module of new Set(sites.map(moduleOf))) bucket(active.inclusiveModules, module)[kind]++;
    if (stages.length === 0) bucket(active.outsideStageSites, open.at(-1)?.site ?? "unattributed")[kind]++;
    for (const site of sites) bucket(active.inclusiveSites, site)[kind]++;
    // Origin is the innermost selected function alive when this object was allocated.
    // Reused objects retain their original site; never charge an evaluation as an allocation.
    if (kind !== "inlineSuccesses") {
      bucket(active.originSites, origin ?? `unassigned:${name}`)[kind]++;
      bucket(active.originModules, moduleOf(origin ?? `unassigned:${name}`))[kind]++;
    }
  };
  const leave = (token) => {
    if (!token || !token.open) return;
    token.open = false;
    const at = token.entries.indexOf(token);
    if (at >= 0) token.entries.splice(at, 1);
    active?.open.delete(token);
  };
  const abandon = (iterator) => {
    for (const token of iteratorTokens.get(iterator) ?? []) {
      if (!token.open) continue;
      if (active) active.abandonedSites[token.site] = (active.abandonedSites[token.site] ?? 0) + 1;
      leave(token);
    }
    iteratorTokens.delete(iterator);
  };
  return {
    next(iterator, value) {
      const previous = currentIterator;
      currentIterator = iterator;
      try { return iterator.next(value); } finally { currentIterator = previous; }
    },
    abandon,
    finishFiber(fiber) {
      for (const token of [...stack(fiber)]) if (token.fiber === fiber) {
        if (token.iterator) abandon(token.iterator);
        else leave(token);
      }
    },
    inherit(child, parent) { stacks.set(child, [...stack(parent)]); },
    evaluation(current, fiber) { bump("evaluations", current["~effect/Effect/identifier"] ?? "unknown", fiber, origins.get(current)); },
    inline(fiber) { bump("inlineSuccesses", "succeedWith", fiber); },
    iteratorSuccess(value, fiber) { bump("iteratorSuccesses", "iteratorSuccess", fiber, origins.get(value)); },
    allocation(site, value) {
      const origin = entries().at(-1)?.site;
      origins.set(value, origin ?? `runtime-constructor:${site}`);
      bump("allocations", site, undefined, origin ?? `runtime-constructor:${site}`);
    },
    enter(site, stage, generator = false, module = site) {
      siteModules.set(site, module);
      const fiber = globalThis[key];
      const open = stack(fiber);
      if (fiber && !stacks.has(fiber)) stacks.set(fiber, open);
      const token = { site, stage, open: true, entries: open, fiber, iterator: generator ? currentIterator : undefined };
      open.push(token);
      if (token.iterator) {
        const tokens = iteratorTokens.get(token.iterator) ?? [];
        tokens.push(token);
        iteratorTokens.set(token.iterator, tokens);
      }
      if (!active) return token;
      active.open.add(token);
      active.total.calls++;
      bucket(active.inclusiveSites, site).calls++;
      bucket(active.exclusiveSites, site).calls++;
      if (stage) {
        bucket(active.exclusiveStages, stage).calls++;
        if (!open.slice(0, -1).some((entry) => entry.open && entry.stage === stage)) bucket(active.inclusiveStages, stage).calls++;
      }
      return token;
    },
    leave,
    begin(input) {
      if (active) throw new Error("Overlapping counted turns");
      // Seed is executed normally. Only the ten explicit counted turns open windows.
      if (!input.id.startsWith("m")) return;
      stacks = new WeakMap();
      outside = [];
      active = { input, total: empty(), operations: {}, allocationKinds: {}, exclusiveStages: {}, inclusiveStages: {}, exclusiveSites: {}, inclusiveSites: {}, originSites: {}, exclusiveModules: {}, inclusiveModules: {}, originModules: {}, outsideStageSites: {}, abandonedSites: {}, open: new Set() };
    },
    end() {
      if (!active) return;
      const { open, ...row } = active;
      row.unclosedSites = [...open].map((token) => token.site);
      for (const token of open) token.open = false;
      rows.push(row);
      active = undefined;
    },
    rows() { return rows; },
  };
})();
