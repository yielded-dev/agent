# Recompute the evidence summary; no cloud calls or dependencies.
# Run: vp exec python3 examples/durable-bench/results/rpc-overhead/analyze.py
import gzip
import json
import math
import random
import statistics
from pathlib import Path

ROOT = Path(__file__).resolve().parent
VARIANTS = ['native', 'fetch', 'schema-sync', 'schema-runtime', 'effect-json', 'effect-ndjson', 'effect-ws']
THREAD = ['thread-native', 'thread-status', 'thread-progress', 'thread-submit']
ROUNDS = ['warm-unary-a', 'warm-unary-b']

def read(name):
    p = ROOT / (name + '.json')
    if p.exists():
        return json.loads(p.read_text())
    zipped = p.with_suffix('.json.gz')
    return json.loads(gzip.decompress(zipped.read_bytes())) if zipped.exists() else None

def stats(xs):
    xs = sorted(x for x in xs if x is not None)
    if not xs:
        return None
    return dict(n=len(xs), mean=statistics.mean(xs), median=statistics.median(xs),
                p90=xs[math.ceil(.9 * len(xs)) - 1], minimum=xs[0], maximum=xs[-1])

def interpolate(xs, p):
    i = (len(xs) - 1) * p
    lo, hi = math.floor(i), math.ceil(i)
    return xs[lo] * (hi - i) + xs[hi] * (i - lo) if hi != lo else xs[lo]

def bootstrap(ds):
    rng = random.Random(829)
    bs = sorted(statistics.median([ds[rng.randrange(len(ds))] for _ in ds]) for _ in range(100000))
    return [interpolate(bs, .025), interpolate(bs, .975)]

def cpu_rows(round_name, variant, size, role, warm=True):
    data = read(round_name + '-cpu')
    if data is None:
        return []
    return [r for r in data['rows'] if r['marker'] and
            r['marker']['round'] == round_name and r['marker']['variant'] == variant and
            r['marker']['size'] == size and r['marker']['role'] == role and
            (not warm or r['marker']['phase'] == 'measure')]

def cell(round_names, variant, size):
    rows = [r for name in round_names for r in (read(name) or {'rows': []})['rows']
            if r['batch']['variant'] == variant and r['batch']['size'] == size]
    driver = [r for name in round_names for r in cpu_rows(name, variant, size, 'driver')]
    obj = [r for name in round_names for r in cpu_rows(name, variant, size, 'object')]
    return dict(latencyMs=stats([x for r in rows for x in r['latencyMs']]),
                setupMs=stats([r['setupMs'] for r in rows]),
                driverInvocationCpuMs=stats([r['cpuMs'] for r in driver]),
                driverAmortizedCpuMs=stats([r['cpuMs'] / (r['marker']['calls'] + r['marker']['warmup'])
                                           for r in driver if r['cpuMs'] is not None]),
                objectInvocationCpuMs=stats([r['cpuMs'] for r in obj]),
                expectedDriver=len(rows), expectedObject=sum(r['batch']['calls'] for r in rows),
                sameInstance=all(r['sameInstance'] for r in rows))

for required in ROUNDS + [name + '-cpu' for name in ROUNDS]:
    if read(required) is None:
        raise FileNotFoundError(required + '.json or .json.gz')

unary = []
for v in VARIANTS:
    for size in [200, 20000]:
        item = dict(variant=v, size=size, pooled=cell(ROUNDS, v, size),
                    rounds={name: cell([name], v, size) for name in ROUNDS if read(name)})
        unary.append(item)

paired = []
for v, reference in [(v, 'native') for v in VARIANTS if v != 'native'] + [('schema-runtime', 'schema-sync')]:
    for size in [200, 20000]:
        per_round = {}
        for name in ROUNDS:
            data = read(name)
            if data is None:
                continue
            lookup = {(r['batch']['object'], r['batch']['variant']): statistics.median(r['latencyMs'])
                      for r in data['rows'] if r['batch']['size'] == size}
            ds = {o: lookup[o, v] - lookup[o, reference] for o in range(data['options']['objects'])
                  if (o, v) in lookup and (o, reference) in lookup}
            per_round[name] = ds
        objects = sorted(set.intersection(*(set(ds) for ds in per_round.values())))
        combined = [statistics.mean([ds[o] for ds in per_round.values()]) for o in objects]
        paired.append(dict(variant=v, reference=reference, size=size,
                           perRound={name: dict(perObjectDeltas=ds, median=statistics.median(ds.values()))
                                     for name, ds in per_round.items()},
                           perObjectMeanRoundDelta=combined, median=statistics.median(combined),
                           objectBootstrap95=bootstrap(combined)))

thread = []
for v in THREAD:
    row = cell(['warm-thread-a'], v, 200)
    row['singleCallDriverCpuMs'] = stats([r['cpuMs'] for r in cpu_rows('warm-thread-cpu', v, 200, 'driver')])
    row['singleCallObjectCpuMs'] = stats([r['cpuMs'] for r in cpu_rows('warm-thread-cpu', v, 200, 'object')])
    thread.append(dict(variant=v, **row))

push = {}
for name in ['warm-push', 'warm-push-controlled']:
    data = read(name)
    if data is None:
        continue
    rows = [r for r in data['rows'] if r['ok']]
    item = dict(attempted=data['options']['objects'], successes=len(rows), failures=data['failures'],
                failedRows=[r for r in data['rows'] if not r['ok']],
                naturalRecreations=sum(r['websocket']['recreationObserved'] for r in rows),
                sameNativeInstance=sum(r['native']['sameInstance'] for r in rows),
                nativeCleanupRestarts=sum(bool(r['native']['release']['resetRequired']) for r in rows))
    for kind in ['native', 'websocket']:
        item[kind] = dict(setupMs=stats([r[kind]['setupMs'] for r in rows]),
                          writeAndFrameMs=stats([x for r in rows for b in r[kind]['bursts'] for x in b['publishToReceiveMs']]),
                          precommittedFrameMs=stats([x for r in rows if 'precommittedBurst' in r[kind]
                                                    for x in r[kind]['precommittedBurst']['publishToReceiveMs']]))
    item['nativeFreshStreamAndFrameMs'] = stats([r['native']['freshStreamSetupAndFirstFrameMs'] for r in rows])
    item['wakeAndReplayMs'] = stats([p['wakeAndFirstReplayMs'] for r in rows for p in r['websocket']['probes']])
    item['reconnectSetupMs'] = stats([r['reconnect']['setupMs'] for r in rows])
    item['reconnectAndFrameMs'] = stats([r['reconnect']['setupAndFirstFrameMs'] for r in rows])
    push[name] = item

telemetry = {}
for name in ROUNDS + ['warm-thread-a', 'warm-thread-cpu', 'warm-push', 'warm-push-controlled']:
    data = read(name + '-cpu')
    if data is None:
        raise FileNotFoundError(name + '-cpu.json or .json.gz')
    telemetry[name] = dict(retrievedEvents=data['retrievedEvents'],
                           unmatchedMarkers=data['unmatchedMarkers'],
                           transientReadFailures=data.get('transientReadFailures'),
                           coverage=data['coverage'])

out = dict(units='milliseconds', median='arithmetic mean of middle two for even n',
           p90='nearest rank: sorted[ceil(.9*n)-1]',
           pairing='mean of round-specific median deltas per Object, then median over Objects',
           bootstrap='100000 Object-cluster resamples, random.Random(829), linear 2.5/97.5 percentiles; pointwise',
           unary=unary, paired=paired, thread=thread, push=push, telemetry=telemetry,
           cleanup=read('cleanup'))
(ROOT / 'summary.json').write_text(json.dumps(out, indent=2) + '\n')
for row in unary:
    p = row['pooled']
    lat, dc, oc = p['latencyMs'], p['driverAmortizedCpuMs'], p['objectInvocationCpuMs']
    print(row['variant'], row['size'], 'RTT', lat['median'], lat['p90'],
          'driver avg/inv', round(dc['mean'], 3), round(p['driverInvocationCpuMs']['mean'], 2),
          'object mean/p50/p90', round(oc['mean'], 3), oc['median'], oc['p90'],
          'coverage', oc['n'], p['expectedObject'], p['driverInvocationCpuMs']['n'], p['expectedDriver'])
for row in paired:
    print('paired', row['variant'], row['reference'], row['size'], row['median'], row['objectBootstrap95'])
print('push', json.dumps(push, indent=2))
