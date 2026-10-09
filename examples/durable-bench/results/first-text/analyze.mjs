import { readFileSync, writeFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const here = dirname(fileURLToPath(import.meta.url));
const rows = readFileSync(join(here, "turns.jsonl"), "utf8").trim().split("\n").filter(Boolean).map(JSON.parse);
const quantile = (values, p) => {
  const sorted = [...values].sort((a, b) => a - b);
  if (!sorted.length) return null;
  const x = (sorted.length - 1) * p, a = Math.floor(x), b = Math.ceil(x);
  return sorted[a] + (sorted[b] - sorted[a]) * (x - a);
};
const median = values => quantile(values, 0.5);
const stats = values => ({ n: values.length, median: median(values), q1: quantile(values, 0.25), q3: quantile(values, 0.75) });
const group = (items, key) => Map.groupBy(items, key);

const bounds = (row, call, endpoint) => {
  const probes = row.clocks.filter(probe => probe.provider.colo === call.provider.colo);
  if (!probes.length) return null;
  // provider_time = driver_time + offset. Echo receipt lies between send and receive.
  // Different isolates in one colo need not have one constant offset. Keep the
  // full observed offset envelope instead of asserting a false intersection.
  const low = Math.min(...probes.map(probe => probe.provider.arrivalMs - probe.afterMs)) - 1;
  const high = Math.max(...probes.map(probe => probe.provider.arrivalMs - probe.beforeMs)) + 1;
  const base = endpoint === "first" ? call.visible.firstTextMs - call.provider.firstTextMs
    : endpoint === "text" ? call.visible.completeMs - call.provider.lastTextMs
    : endpoint === "finalized" ? (call.visible.finalizedMs ?? call.visible.completeMs) - call.provider.lastTokenMs
    : call.visible.completeMs - call.provider.lastTokenMs;
  return { low: base + low, high: base + high, midpoint: base + (low + high) / 2, precisionMs: high - low, probes: probes.length };
};

const samples = rows.map(row => ({ ...row, calls: row.calls.map(call => ({ ...call,
  visibility: bounds(row, call, "first"), completion: bounds(row, call, "last"),
  textCompletion: bounds(row, call, "text"), finalization: bounds(row, call, "finalized"),
})) }));
const summary = [];
const perCall = [];
const repeatRanges = [];
const phases = [...new Set(samples.map(row => row.phase))];
for (const phase of phases) for (const [cell, turns] of group(samples.filter(row => row.phase === phase && row.state !== "settling"),
  row => `${row.framework}/${row.history}/${row.state}`)) {
  const objects = [...group(turns, row => row.objectIndex)].map(([object, values]) => ({
    object, firstVisibleMs: median(values.map(row => row.firstVisibleMs)), turnMs: median(values.map(row => row.turnMs)),
    visibilityMs: values.every(row => row.calls.every(call => call.visibility))
      ? median(values.map(row => median(row.calls.map(call => call.visibility.midpoint)))) : null,
    completionMs: values.every(row => row.calls.every(call => call.completion))
      ? median(values.map(row => median(row.calls.map(call => call.completion.midpoint)))) : null,
  }));
  summary.push({phase,cell,objects:objects.length,turns:turns.length,
    firstVisibleMs:stats(objects.map(row=>row.firstVisibleMs)),turnMs:stats(objects.map(row=>row.turnMs)),
    visibilityMs:stats(objects.flatMap(row=>row.visibilityMs===null?[]:[row.visibilityMs])),
    completionMs:stats(objects.flatMap(row=>row.completionMs===null?[]:[row.completionMs])),
    missingClockBounds:turns.flatMap(row=>row.calls).filter(call=>!call.visibility).length,
    maxClockIntervalMs:Math.max(...turns.flatMap(row=>row.calls.flatMap(call=>call.visibility?[call.visibility.precisionMs]:[]))),
  });
  for(let call=0;call<9;call++) {
    const values=[...group(turns,row=>row.objectIndex)].map(([,rows])=>rows.map(row=>row.calls[call]));
    perCall.push({phase,cell,call,
      visibilityMs:stats(values.filter(calls=>calls.every(call=>call.visibility)).map(calls=>median(calls.map(call=>call.visibility.midpoint)))),
      completionMs:stats(values.filter(calls=>calls.every(call=>call.completion)).map(calls=>median(calls.map(call=>call.completion.midpoint)))),
      textCompletionMs:stats(values.filter(calls=>calls.every(call=>call.textCompletion)).map(calls=>median(calls.map(call=>call.textCompletion.midpoint)))),
      finalizationMs:stats(values.filter(calls=>calls.every(call=>call.finalization)).map(calls=>median(calls.map(call=>call.finalization.midpoint)))),
    });
  }
  if(turns[0].state === "warm") for(const [object,values] of group(turns,row=>row.objectIndex))
    repeatRanges.push({phase,cell,object,n:values.length,
      firstVisibleRangeMs:Math.max(...values.map(row=>row.firstVisibleMs))-Math.min(...values.map(row=>row.firstVisibleMs)),
      turnRangeMs:Math.max(...values.map(row=>row.turnMs))-Math.min(...values.map(row=>row.turnMs)),
    });
}
const display = value => value.n ? `${Math.round(value.median)} [${Math.round(value.q1)}–${Math.round(value.q3)}]` : "unavailable";
const table = ["| Phase | Target / seed / state | Objects | First visible (ms) | Visibility lag (ms) | Last token → complete text (ms) | Settlement (ms) |",
  "| --- | --- | ---: | ---: | ---: | ---: | ---: |",
  ...summary.filter(row=>!row.phase.startsWith("pilot")).map(row=>`| ${row.phase} | ${row.cell} | ${row.objects} | ${display(row.firstVisibleMs)} | ${display(row.visibilityMs)} | ${display(row.completionMs)} | ${display(row.turnMs)} |`)].join("\n")+"\n";
writeFileSync(join(here,"tables.md"),table);
writeFileSync(join(here,"summary.json"),JSON.stringify({summary,perCall,repeatRanges,clockAssumption:"Provider/driver offsets during a turn stay within the full same-colo echo envelope (plus 1 ms quantization), including two Object-routed probes after the primary timer. This is a conditional calibration, not a globally synchronized clock guarantee. Midpoints are estimates; signed bounds retained, no unavailable values replaced by zero."},null,2)+"\n");
const interval=value=>value ? [value.low,value.high] : null;
writeFileSync(join(here,"visibility-bounds.json"),"[\n"+samples.map(row=>JSON.stringify({phase:row.phase,framework:row.framework,history:row.history,object:row.objectIndex,sample:row.sample,
  calls:row.calls.map(call=>({call:call.call,visibility:interval(call.visibility),completion:interval(call.completion),textCompletion:interval(call.textCompletion),finalization:interval(call.finalization)}))})).join(",\n")+"\n]\n");
console.log(table);
