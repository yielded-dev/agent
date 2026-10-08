# cf-bench-8914 statistics

Status: **incomplete**. Counts are observed/expected. Values marked incomplete describe available data only.

Latency is primary: clientWallMs and doInvocationWallMs lead cold and warm reporting for every role/size/build. CPU, DO wall minus CPU, and client wall minus DO wall follow as breakdown diagnostics. Effect improvement claims require latency evidence; CPU changes alone do not establish a latency improvement.

Primary clientWallMs comes from valid raw samples.json receipts, independently of measured.json or CF logs. normalizedClientWallRatio is computed from that raw clientWallMs divided by the raw Sample.reference.clientWallMs when finite and positive. Both client medians require all nine successful warm receipts; the normalized median additionally requires nine valid HTTP references. A telemetry failure or dropped log does not erase a valid client receipt.

Object-level cohort/control robustness summary (stored as primaryWarm): predeclared minimum of seven out of nine observed successful finite warm turns for DO/CPU metrics, normalized DO/CPU metrics, and telemetry-dependent residuals/reference timings. Below seven yields null. Client/HTTP-reference and receipt-only metrics require all nine. Exact observed n, eligibility, and the threshold are stored per Object/metric. An eligible seven- or eight-turn median retains incomplete telemetry coverage.

Quantiles use linear interpolation. The README warm-turn summary pools 27 warm turns and reports median [p25, p75]. Object medians summarize independent cohorts; paired Object-median ratios provide the robustness comparison. Ratios are numerator/denominator; undefined ratios remain missing.

The design targets three distinct Object clusters per role/size. Inference has n=3 only when all three Objects (and, for comparisons, matching control pairs) are eligible for that metric; otherwise use the reported smaller eligible count. A Tardie Thread plus its Actor belongs to one cluster. Up to 27 pooled warm turns are repeated observations within at most three clusters, a modest independent sample. No formal confidence intervals are reported.

Per measurement steering, raw differences below roughly 30% across separate Workers require supporting normalized control evidence to resolve, but normalization is not sufficient by itself. Control interpretation must account for median bias as well as spread: use a conservative envelope around one including the full control/base range and its reciprocals, not IQR width alone. This report supplies descriptive values and control spread without automatically classifying an effect; three Object clusters remain a modest basis for inference.

Client normalization uses raw HTTP receipts and references, even when CF telemetry is missing. The collector supplies normalizedDoWallRatio = doInvocationWallMs / referenceDoWallMs, normalizedCpuRatio = main CPU / referenceCpuTimeMs, and normalizedDoTotalCpuRatio = sum(each DO CPU / that DO reference CPU). Tardie's Actor reference is a separate earlier HTTP RPC; the Thread reference immediately precedes every turn with the same single-loop HTTP path for every role. For cold, the reference precedes final ctx.abort. Fixed JavaScript work is identical on Threads and Actors. Object medians and pooled summaries operate on per-turn ratios. Reference receipts, referenceInvocations/objectReferences evidence, and all three reference timings are retained. Missing references leave normalized values unavailable without discarding raw rows.

Normalization is a diagnostic ratio, not a guaranteed correction for machine cost. Identical fixed JavaScript work does not establish that reference cost scales with workload cost across roles, builds, or turns. Differences in reference timing can change normalized rankings; raw latency, normalized latency, and matched controls must remain visible together. JIT tier and module placement are possible hypotheses, not established explanations. Control agreement alone does not validate a universal calibration.

doWallMinusCpuMs = doInvocationWallMs - cpuTimeMs; clientWallMinusDoWallMs = clientWallMs - doInvocationWallMs. These are collector-reported timing differences, with signs preserved, not causal allocations to a particular queue, network, or runtime component.

Every raw and normalized comparison carries control/base ratios from the same available size/sample/phase slots. Control spread reports median, p25/p75, min/max, and coverage. Missing control evidence marks the comparison incomplete without discarding the target ratio. A control/base row shows its own spread, not a separate replication.

Accepted sample IDs: size 50 = 1,2,3; sizes 250,1000 = 0,1,2. Pilot 50-0 and attempts/ are excluded. Primary client timings and HTTP normalization use raw receipts, independently of CF telemetry availability. Completed workload status is separate from incomplete telemetry coverage.

Size 3500 was not provisioned and remains unmeasured; it is excluded from planned coverage and tables.

Three cold turns per role/size. Readiness and the fixed JavaScript reference prime the module before the final Thread (and Tardie Actor) ctx.abort. Cold requires a fresh DO runtimeId and a constructor log on the cold RPC trace. Module/isolate survival across the abort is not guaranteed, so module freshness is not guaranteed. Matched rows carry collector validation; receipt-only fallback retains timings with unavailable telemetry evidence explicitly marked.

Each Object's verified deployment resolves its role manifest by both deployed bench.mjs and worker.mjs SHA-256, using current builds/<role>.json and archived builds/wrapper-v*/<role>/manifest.json (also accepting wrapper-v*/<role>.json). Identical manifest copies share one identity; conflicting selected metadata for the same module hashes remains unresolved. Receipt version/generation must match the saved deployment. Historical matchesLocal flags are retained as recorded, never recomputed against the latest wrapper. Unresolved provenance is explicit and does not discard timing observations. Group build columns show the resolved deployed set, which can contain multiple wrapper hashes. Pi uses its targetPackage version (pi-durable 1.0.4) and no Effect runtime; its manifest Effect version is build-CLI provenance only. Other roles display their resolved manifest Effect version. summary.json retains selected manifest fields and each Object's deployment generation, version and module identities; full fixture/reference hashes remain in the original current or archived manifests.

Build columns show the resolved deployed set with abbreviated bench/wrapper hashes. Per-Object deployment modules, matched manifest paths, selected identity fields and provenance status are retained in summary.json. Full manifests, including fixture and reference hashes, remain in the run directory's sibling builds/ and wrapper-v*/ archives.

## RPC clientWallMs (ms)

Object-median eligibility: at least 9 of 9 warm observations. Per-cluster counts below retain the sample ID and mark ineligible medians.

| Size | Role | Deployed build set | Cold median [p25, p75] (clusters/3) | README warm median [p25, p75] (turns/27) | Object-median median [p25, p75] (eligible clusters/3) | Warm n per cluster | Warm-turn p95 | Coverage |
|---:|---|---|---:|---:|---:|---|---:|---|
| 50 | base | base@4.0.1 (deabe970/289122cd); 3/3 deployments matched | 1224 [1090.5, 1255] (3/3) | 724 [692, 795] (27/27) | 709 [696.5, 759.5] (3/3) | 1:9/9; 2:9/9; 3:9/9 | 856.8 | complete |
| 50 | head | head@4.0.1 (fc50f45a/289122cd); 3/3 deployments matched | 935 [905, 1164] (3/3) | 709 [643.5, 812.5] (27/27) | 708 [675.5, 766] (3/3) | 1:9/9; 2:9/9; 3:9/9 | 895.9 | complete |
| 50 | control | control@4.0.1 (deabe970/289122cd); 3/3 deployments matched | 1177 [1049, 1220] (3/3) | 705 [672, 823.5] (27/27) | 694 [673, 796.5] (3/3) | 1:9/9; 2:9/9; 3:9/9 | 948.6 | complete |
| 50 | pinned | pinned@4.0.0 (c823d174/289122cd); 3/3 deployments matched | 1114 [854.5, 1385] (3/3) | 741 [483, 796.5] (27/27) | 765 [580.5, 780.5] (3/3) | 1:9/9; 2:9/9; 3:9/9 | 873.8 | complete |
| 50 | pi | pi 1.0.4 / no Effect (5526e585/866f1f7f); 3/3 deployments matched | 657 [654.5, 669] (3/3) | 462 [444.5, 481] (27/27) | 462 [438.5, 476.5] (3/3) | 1:9/9; 2:9/9; 3:9/9 | 574.7 | complete |
| 50 | tardie | tardie@4.0.0-rc.115 (c8fbbb72/8a0cb11b); 3/3 deployments matched | 2462 [2446.5, 2685] (3/3) | 2241 [2113, 2444] (27/27) | 2154 [2132.5, 2362.5] (3/3) | 1:9/9; 2:9/9; 3:9/9 | 2915.6 | complete |
| 250 | base | base@4.0.1 (deabe970/289122cd); base@4.0.1 (deabe970/6b2bef15); 3/3 deployments matched | 1422 [1207.5, 1439.5] (3/3) | 892 [771.5, 1019] (27/27) | 899 [807, 969] (3/3) | 0:9/9; 1:9/9; 2:9/9 | 1183.5 | complete |
| 250 | head | head@4.0.1 (fc50f45a/289122cd); head@4.0.1 (fc50f45a/6b2bef15); 3/3 deployments matched | 1596 [1490, 1609.5] (3/3) | 938 [915, 1037] (27/27) | 940 [935.5, 984] (3/3) | 0:9/9; 1:9/9; 2:9/9 | 1264.4 | complete |
| 250 | control | control@4.0.1 (deabe970/289122cd); control@4.0.1 (deabe970/6b2bef15); 3/3 deployments matched | 1079 [987, 1310] (3/3) | 707 [670.5, 761.5] (27/27) | 690 [681, 737.5] (3/3) | 0:9/9; 1:9/9; 2:9/9 | 870.2 | complete |
| 250 | pinned | pinned@4.0.0 (c823d174/289122cd); pinned@4.0.0 (c823d174/6b2bef15); 3/3 deployments matched | 1525 [1421, 1568.5] (3/3) | 941 [870.5, 1057] (27/27) | 937 [904.5, 995.5] (3/3) | 0:9/9; 1:9/9; 2:9/9 | 1229.1 | complete |
| 250 | pi | pi 1.0.4 / no Effect (5526e585/866f1f7f); pi 1.0.4 / no Effect (5526e585/f1947b8f); 3/3 deployments matched | 891 [800.5, 1189] (3/3) | 567 [527.5, 600.5] (27/27) | 553 [540, 570.5] (3/3) | 0:9/9; 1:9/9; 2:9/9 | 666.4 | complete |
| 250 | tardie | tardie@4.0.0-rc.115 (c8fbbb72/8a0cb11b); tardie@4.0.0-rc.115 (c8fbbb72/9f22f5b1); 3/3 deployments matched | 2496 [1997.5, 2780] (3/3) | 1769 [1343, 2307] (27/27) | 1769 [1551, 2110] (3/3) | 0:9/9; 1:9/9; 2:9/9 | 2771.7 | complete |
| 1000 | base | base@4.0.1 (deabe970/6b2bef15); 3/3 deployments matched | 1411 [1378.5, 1416.5] (3/3) | 884 [782.5, 988.5] (27/27) | 865 [818.5, 909.5] (3/3) | 0:9/9; 1:9/9; 2:9/9 | 1095 | complete |
| 1000 | head | head@4.0.1 (fc50f45a/6b2bef15); 3/3 deployments matched | 1363 [1249.5, 1410.5] (3/3) | 899 [852.5, 995.5] (27/27) | 953 [866.5, 958] (3/3) | 0:9/9; 1:9/9; 2:9/9 | 1252.3 | complete |
| 1000 | control | control@4.0.1 (deabe970/6b2bef15); 3/3 deployments matched | 1520 [1463, 1559] (3/3) | 920 [829, 1206.5] (27/27) | 920 [860, 1072.5] (3/3) | 0:9/9; 1:9/9; 2:9/9 | 1425.8 | complete |
| 1000 | pinned | pinned@4.0.0 (c823d174/6b2bef15); 3/3 deployments matched | 1936 [1873, 2014] (3/3) | 1378 [1228, 1531] (27/27) | 1262 [1259.5, 1395] (3/3) | 0:9/9; 1:9/9; 2:9/9 | 1745.3 | complete |
| 1000 | pi | pi 1.0.4 / no Effect (5526e585/f1947b8f); 3/3 deployments matched | 1541 [1244, 1645] (3/3) | 1142 [760, 1361.5] (27/27) | 1142 [905, 1282.5] (3/3) | 0:9/9; 1:9/9; 2:9/9 | 1562 | complete |
| 1000 | tardie | tardie@4.0.0-rc.115 (c8fbbb72/9f22f5b1); 3/3 deployments matched | 8904 [6798.5, 8988.5] (3/3) | 3024 [2468, 4176.5] (27/27) | 3173 [2793, 3614] (3/3) | 0:9/9; 1:9/9; 2:9/9 | 6310.6 | complete |

| Size | Pair | Cold ratio median [IQR] (pairs/3) | Control/base cold spread | Object-median ratio median [IQR] (pairs/3) | Control/base Object spread | Paired-turn ratio median [IQR] (turn pairs/27; diagnostic) | Control/base turn spread | Status |
|---:|---|---:|---:|---:|---:|---:|---:|---|
| 50 | head/base | 0.914316 [0.820688, 1.02619] (3/3) | 1.03186 [0.874018, 1.13087] (3/3); range [0.716174, 1.22989] | 1.01728 [0.962098, 1.02619] (3/3) | 0.978843 [0.96603, 1.04436] (3/3); range [0.953216, 1.10988] | 1 [0.903466, 1.05341] (27/27) | 1.00545 [0.956542, 1.05143] (27/27); range [0.804293, 1.31354] | complete |
| 50 | control/base | 1.03186 [0.874018, 1.13087] (3/3) | 1.03186 [0.874018, 1.13087] (3/3); range [0.716174, 1.22989] | 0.978843 [0.96603, 1.04436] (3/3) | 0.978843 [0.96603, 1.04436] (3/3); range [0.953216, 1.10988] | 1.00545 [0.956542, 1.05143] (27/27) | 1.00545 [0.956542, 1.05143] (27/27); range [0.804293, 1.31354] | complete |
| 50 | head/control | 1.0152 [0.879308, 1.05907] (3/3) | 1.03186 [0.874018, 1.13087] (3/3); range [0.716174, 1.22989] | 0.926513 [0.921543, 1.0062] (3/3) | 0.978843 [0.96603, 1.04436] (3/3); range [0.953216, 1.10988] | 0.98254 [0.910622, 1.02064] (27/27) | 1.00545 [0.956542, 1.05143] (27/27); range [0.804293, 1.31354] | complete |
| 50 | head/pi | 1.43405 [1.38293, 1.73979] (3/3) | 1.03186 [0.874018, 1.13087] (3/3); range [0.716174, 1.22989] | 1.44196 [1.41687, 1.71375] (3/3) | 0.978843 [0.96603, 1.04436] (3/3); range [0.953216, 1.10988] | 1.44842 [1.32713, 1.89899] (27/27) | 1.00545 [0.956542, 1.05143] (27/27); range [0.804293, 1.31354] | complete |
| 50 | head/tardie | 0.379773 [0.369853, 0.429398] (3/3) | 1.03186 [0.874018, 1.13087] (3/3); range [0.716174, 1.22989] | 0.320498 [0.312546, 0.324594] (3/3) | 0.978843 [0.96603, 1.04436] (3/3); range [0.953216, 1.10988] | 0.314275 [0.294253, 0.330682] (27/27) | 1.00545 [0.956542, 1.05143] (27/27); range [0.804293, 1.31354] | complete |
| 50 | pinned/pi | 1.63583 [1.2742, 2.07819] (3/3) | 1.03186 [0.874018, 1.13087] (3/3); range [0.716174, 1.22989] | 1.65584 [1.23118, 1.78696] (3/3) | 0.978843 [0.96603, 1.04436] (3/3); range [0.953216, 1.10988] | 1.59708 [0.90523, 1.79976] (27/27) | 1.00545 [0.956542, 1.05143] (27/27); range [0.804293, 1.31354] | complete |
| 50 | pinned/tardie | 0.383081 [0.312377, 0.532141] (3/3) | 1.03186 [0.874018, 1.13087] (3/3); range [0.716174, 1.22989] | 0.309607 [0.246726, 0.335997] (3/3) | 0.978843 [0.96603, 1.04436] (3/3); range [0.953216, 1.10988] | 0.290079 [0.230601, 0.352576] (27/27) | 1.00545 [0.956542, 1.05143] (27/27); range [0.804293, 1.31354] | complete |
| 250 | head/base | 1.0954 [1.03434, 1.36492] (3/3) | 0.901309 [0.820936, 0.992497] (3/3); range [0.740563, 1.08368] | 1.14349 [1.01977, 1.22909] (3/3) | 0.755534 [0.751516, 0.860285] (3/3); range [0.747497, 0.965035] | 1.08508 [0.98523, 1.28599] (27/27) | 0.808977 [0.750894, 0.853641] (27/27); range [0.600676, 1.09955] | complete |
| 250 | control/base | 0.901309 [0.820936, 0.992497] (3/3) | 0.901309 [0.820936, 0.992497] (3/3); range [0.740563, 1.08368] | 0.755534 [0.751516, 0.860285] (3/3) | 0.755534 [0.751516, 0.860285] (3/3); range [0.747497, 0.965035] | 0.808977 [0.750894, 0.853641] (27/27) | 0.808977 [0.750894, 0.853641] (27/27); range [0.600676, 1.09955] | complete |
| 250 | head/control | 1.47915 [1.18863, 1.64628] (3/3) | 0.901309 [0.820936, 0.992497] (3/3); range [0.740563, 1.08368] | 1.36232 [1.27415, 1.44604] (3/3) | 0.755534 [0.751516, 0.860285] (3/3); range [0.747497, 0.965035] | 1.30835 [1.19172, 1.49482] (27/27) | 0.808977 [0.750894, 0.853641] (27/27); range [0.600676, 1.09955] | complete |
| 250 | head/pi | 1.79125 [1.44135, 1.87027] (3/3) | 0.901309 [0.820936, 0.992497] (3/3); range [0.740563, 1.08368] | 1.7483 [1.71592, 1.76599] (3/3) | 0.755534 [0.751516, 0.860285] (3/3); range [0.747497, 0.965035] | 1.65136 [1.54996, 1.83359] (27/27) | 0.808977 [0.750894, 0.853641] (27/27); range [0.600676, 1.09955] | complete |
| 250 | head/tardie | 0.65024 [0.585564, 0.786761] (3/3) | 0.901309 [0.820936, 0.992497] (3/3); range [0.740563, 1.08368] | 0.531374 [0.475397, 0.614899] (3/3) | 0.755534 [0.751516, 0.860285] (3/3); range [0.747497, 0.965035] | 0.557455 [0.450914, 0.687266] (27/27) | 0.808977 [0.750894, 0.853641] (27/27); range [0.600676, 1.09955] | complete |
| 250 | pinned/pi | 1.8092 [1.34744, 1.97855] (3/3) | 0.901309 [0.820936, 0.992497] (3/3); range [0.740563, 1.08368] | 1.65465 [1.62409, 1.78031] (3/3) | 0.755534 [0.751516, 0.860285] (3/3); range [0.747497, 0.965035] | 1.64896 [1.43961, 1.88381] (27/27) | 0.808977 [0.750894, 0.853641] (27/27); range [0.600676, 1.09955] | complete |
| 250 | pinned/tardie | 0.527644 [0.526877, 0.772495] (3/3) | 0.901309 [0.820936, 0.992497] (3/3); range [0.740563, 1.08368] | 0.492934 [0.437613, 0.641816] (3/3) | 0.755534 [0.751516, 0.860285] (3/3); range [0.747497, 0.965035] | 0.527962 [0.385485, 0.669938] (27/27) | 0.808977 [0.750894, 0.853641] (27/27); range [0.600676, 1.09955] | complete |
| 1000 | head/base | 1.01263 [0.908866, 1.01897] (3/3) | 1.12927 [1.05901, 1.1309] (3/3); range [0.988748, 1.13253] | 1.01036 [1.00466, 1.06183] (3/3) | 1.03627 [1.00032, 1.22623] (3/3); range [0.964361, 1.41618] | 0.992806 [0.908987, 1.14865] (27/27) | 1.06101 [0.9409, 1.21894] (27/27); range [0.668182, 1.93342] | complete |
| 1000 | control/base | 1.12927 [1.05901, 1.1309] (3/3) | 1.12927 [1.05901, 1.1309] (3/3); range [0.988748, 1.13253] | 1.03627 [1.00032, 1.22623] (3/3) | 1.03627 [1.00032, 1.22623] (3/3); range [0.964361, 1.41618] | 1.06101 [0.9409, 1.21894] (27/27) | 1.06101 [0.9409, 1.21894] (27/27); range [0.668182, 1.93342] | complete |
| 1000 | head/control | 0.896711 [0.8038, 0.966847] (3/3) | 1.12927 [1.05901, 1.1309] (3/3); range [0.988748, 1.13253] | 0.975 [0.880561, 1.00543] (3/3) | 1.03627 [1.00032, 1.22623] (3/3); range [0.964361, 1.41618] | 1.01957 [0.826488, 1.06553] (27/27) | 1.06101 [0.9409, 1.21894] (27/27); range [0.668182, 1.93342] | complete |
| 1000 | head/pi | 0.946139 [0.797826, 1.19271] (3/3) | 1.12927 [1.05901, 1.1309] (3/3); range [0.988748, 1.13253] | 0.834501 [0.75562, 1.00108] (3/3) | 1.03627 [1.00032, 1.22623] (3/3); range [0.964361, 1.41618] | 0.92244 [0.739575, 1.07018] (27/27) | 1.06101 [0.9409, 1.21894] (27/27); range [0.668182, 1.93342] | complete |
| 1000 | head/tardie | 0.160697 [0.156887, 0.20138] (3/3) | 1.12927 [1.05901, 1.1309] (3/3); range [0.988748, 1.13253] | 0.303498 [0.269258, 0.313374] (3/3) | 1.03627 [1.00032, 1.22623] (3/3); range [0.964361, 1.41618] | 0.308587 [0.212631, 0.353822] (27/27) | 1.06101 [0.9409, 1.21894] (27/27); range [0.668182, 1.93342] | complete |
| 1000 | pinned/pi | 1.35756 [1.23224, 1.63443] (3/3) | 1.12927 [1.05901, 1.1309] (3/3); range [0.988748, 1.13253] | 1.338 [1.11067, 1.61361] (3/3) | 1.03627 [1.00032, 1.22623] (3/3); range [0.964361, 1.41618] | 1.44636 [0.969698, 1.75218] (27/27) | 1.06101 [0.9409, 1.21894] (27/27); range [0.668182, 1.93342] | complete |
| 1000 | pinned/tardie | 0.230574 [0.216927, 0.321552] (3/3) | 1.12927 [1.05901, 1.1309] (3/3); range [0.988748, 1.13253] | 0.396155 [0.386487, 0.459578] (3/3) | 1.03627 [1.00032, 1.22623] (3/3); range [0.964361, 1.41618] | 0.462865 [0.305622, 0.56188] (27/27) | 1.06101 [0.9409, 1.21894] (27/27); range [0.668182, 1.93342] | complete |

## RPC doInvocationWallMs (ms)

Object-median eligibility: at least 7 of 9 warm observations. Per-cluster counts below retain the sample ID and mark ineligible medians.

| Size | Role | Deployed build set | Cold median [p25, p75] (clusters/3) | README warm median [p25, p75] (turns/27) | Object-median median [p25, p75] (eligible clusters/3) | Warm n per cluster | Warm-turn p95 | Coverage |
|---:|---|---|---:|---:|---:|---|---:|---|
| 50 | base | base@4.0.1 (deabe970/289122cd); 3/3 deployments matched | 1063 [1048.5, 1077.5] (2/3) | 537 [505.5, 600.5] (27/27) | 523 [509.5, 572.5] (3/3) | 1:9/9; 2:9/9; 3:9/9 | 667 | incomplete |
| 50 | head | head@4.0.1 (fc50f45a/289122cd); 3/3 deployments matched | 751 [744, 972.5] (3/3) | 530 [485, 613.5] (27/27) | 524 [505, 575.5] (3/3) | 1:9/9; 2:9/9; 3:9/9 | 695.7 | complete |
| 50 | control | control@4.0.1 (deabe970/289122cd); 3/3 deployments matched | 985 [863.5, 1032.5] (3/3) | 524 [478.5, 629] (27/27) | 495 [484.5, 605] (3/3) | 1:9/9; 2:9/9; 3:9/9 | 753.6 | complete |
| 50 | pinned | pinned@4.0.0 (c823d174/289122cd); 3/3 deployments matched | 924 [729.5, 1225.5] (3/3) | 591 [412, 620.5] (27/27) | 593 [462, 612] (3/3) | 1:9/9; 2:9/9; 3:9/9 | 739.8 | complete |
| 50 | pi | pi 1.0.4 / no Effect (5526e585/866f1f7f); 3/3 deployments matched | 471 [459, 511] (3/3) | 290 [273.5, 320] (24/27) | 291 [283, 295] (3/3) | 1:9/9; 2:8/9; 3:7/9 | 399.75 | incomplete |
| 50 | tardie | tardie@4.0.0-rc.115 (c8fbbb72/8a0cb11b); 3/3 deployments matched | 2299 [2259.5, 2512] (3/3) | 2075 [1931.5, 2252] (27/27) | 2018 [1962.5, 2203] (3/3) | 1:9/9; 2:9/9; 3:9/9 | 2734.1 | complete |
| 250 | base | base@4.0.1 (deabe970/289122cd); base@4.0.1 (deabe970/6b2bef15); 3/3 deployments matched | 1237 [1053.5, 1280.5] (3/3) | 753 [642, 842] (27/27) | 771 [681, 807.5] (3/3) | 0:9/9; 1:9/9; 2:9/9 | 930.7 | complete |
| 250 | head | head@4.0.1 (fc50f45a/289122cd); head@4.0.1 (fc50f45a/6b2bef15); 3/3 deployments matched | 1456 [1317.5, 1465.5] (3/3) | 792 [717, 854.75] (26/27) | 812 [772, 843.25] (3/3) | 0:9/9; 1:8/9; 2:9/9 | 998.25 | incomplete |
| 250 | control | control@4.0.1 (deabe970/289122cd); control@4.0.1 (deabe970/6b2bef15); 3/3 deployments matched | 1062 [890, 1234] (2/3) | 588 [512.5, 659] (19/27) | 584 [544.5, 623.5] (2/3) | 0:7/9; 1:3/9 ineligible; 2:9/9 | 753.7 | incomplete |
| 250 | pinned | pinned@4.0.0 (c823d174/289122cd); pinned@4.0.0 (c823d174/6b2bef15); 3/3 deployments matched | 1335 [1253.5, 1379] (3/3) | 784.5 [733.25, 865.25] (26/27) | 797 [768.5, 823.25] (3/3) | 0:9/9; 1:9/9; 2:8/9 | 1061.5 | incomplete |
| 250 | pi | pi 1.0.4 / no Effect (5526e585/866f1f7f); pi 1.0.4 / no Effect (5526e585/f1947b8f); 3/3 deployments matched | 693 [606.5, 998] (3/3) | 355 [334.5, 387.5] (27/27) | 355 [347.5, 373] (3/3) | 0:9/9; 1:9/9; 2:9/9 | 471 | complete |
| 250 | tardie | tardie@4.0.0-rc.115 (c8fbbb72/8a0cb11b); tardie@4.0.0-rc.115 (c8fbbb72/9f22f5b1); 3/3 deployments matched | 2170.5 [1784.75, 2556.25] (2/3) | 1928 [1237, 2219] (25/27) | 1597.5 [1426.75, 1964.25] (3/3) | 0:8/9; 1:9/9; 2:8/9 | 2654.8 | incomplete |
| 1000 | base | base@4.0.1 (deabe970/6b2bef15); 3/3 deployments matched | 1265 [1243, 1280] (3/3) | 742 [677, 839] (27/27) | 744 [715, 750] (3/3) | 0:9/9; 1:9/9; 2:9/9 | 985.4 | complete |
| 1000 | head | head@4.0.1 (fc50f45a/6b2bef15); 3/3 deployments matched | 1155.5 [1098.25, 1212.75] (2/3) | 836 [768, 865] (25/27) | 802 [800.75, 881.75] (3/3) | 0:9/9; 1:8/9; 2:8/9 | 1174.8 | incomplete |
| 1000 | control | control@4.0.1 (deabe970/6b2bef15); 3/3 deployments matched | 1460 [1344.5, 1470] (3/3) | 815.5 [774.75, 1084.25] (26/27) | 815 [796, 965.75] (3/3) | 0:9/9; 1:8/9; 2:9/9 | 1288.75 | incomplete |
| 1000 | pinned | pinned@4.0.0 (c823d174/6b2bef15); 3/3 deployments matched | 1847 [1777.5, 1871.5] (3/3) | 1334 [1148, 1479.5] (27/27) | 1230 [1196, 1353] (3/3) | 0:9/9; 1:9/9; 2:9/9 | 1668.2 | complete |
| 1000 | pi | pi 1.0.4 / no Effect (5526e585/f1947b8f); 3/3 deployments matched | 1462 [1146, 1511] (3/3) | 912 [605.5, 1169.5] (27/27) | 912 [742, 1073] (3/3) | 0:9/9; 1:9/9; 2:9/9 | 1372.7 | complete |
| 1000 | tardie | tardie@4.0.0-rc.115 (c8fbbb72/9f22f5b1); 3/3 deployments matched | 8803 [6730.5, 8890.5] (3/3) | 2973 [2365, 4114] (27/27) | 3151 [2739, 3556] (3/3) | 0:9/9; 1:9/9; 2:9/9 | 6197 | complete |

| Size | Pair | Cold ratio median [IQR] (pairs/3) | Control/base cold spread | Object-median ratio median [IQR] (pairs/3) | Control/base Object spread | Paired-turn ratio median [IQR] (turn pairs/27; diagnostic) | Control/base turn spread | Status |
|---:|---|---:|---:|---:|---:|---:|---:|---|
| 50 | head/base | 0.921234 [0.804481, 1.03799] (2/3) | 0.861987 [0.770737, 0.953237] (2/3); range [0.679487, 1.04449] | 1.00804 [0.968646, 1.03225] (3/3) | 0.955645 [0.951054, 1.05258] (3/3); range [0.946463, 1.14952] | 1.02745 [0.918344, 1.0953] (27/27) | 1 [0.946761, 1.09372] (27/27); range [0.772955, 1.41187] | incomplete |
| 50 | control/base | 0.861987 [0.770737, 0.953237] (2/3) | 0.861987 [0.770737, 0.953237] (2/3); range [0.679487, 1.04449] | 0.955645 [0.951054, 1.05258] (3/3) | 0.955645 [0.951054, 1.05258] (3/3); range [0.946463, 1.14952] | 1 [0.946761, 1.09372] (27/27) | 1 [0.946761, 1.09372] (27/27); range [0.772955, 1.41187] | incomplete |
| 50 | head/control | 1.01213 [0.880176, 1.05884] (3/3) | 0.861987 [0.770737, 0.953237] (2/3); range [0.679487, 1.04449] | 0.981818 [0.929371, 1.04365] (3/3) | 0.955645 [0.951054, 1.05258] (3/3); range [0.946463, 1.14952] | 0.979439 [0.915307, 1.05121] (27/27) | 1 [0.946761, 1.09372] (27/27); range [0.772955, 1.41187] | incomplete |
| 50 | head/pi | 1.68009 [1.62242, 1.92353] (3/3) | 0.861987 [0.770737, 0.953237] (2/3); range [0.679487, 1.04449] | 1.76727 [1.75989, 1.96096] (3/3) | 0.955645 [0.951054, 1.05258] (3/3); range [0.946463, 1.14952] | 1.87217 [1.57771, 2.05326] (24/27) | 1.01468 [0.950406, 1.10054] (24/27); range [0.772955, 1.41187] | incomplete |
| 50 | head/tardie | 0.331982 [0.329323, 0.385074] (3/3) | 0.861987 [0.770737, 0.953237] (2/3); range [0.679487, 1.04449] | 0.259663 [0.257257, 0.261113] (3/3) | 0.955645 [0.951054, 1.05258] (3/3); range [0.946463, 1.14952] | 0.258383 [0.245031, 0.267828] (27/27) | 1 [0.946761, 1.09372] (27/27); range [0.772955, 1.41187] | incomplete |
| 50 | pinned/pi | 1.67695 [1.43691, 2.45949] (3/3) | 0.861987 [0.770737, 0.953237] (2/3); range [0.679487, 1.04449] | 2.0378 [1.57241, 2.16617] (3/3) | 0.955645 [0.951054, 1.05258] (3/3); range [0.946463, 1.14952] | 2.0014 [1.41337, 2.21854] (24/27) | 1.01468 [0.950406, 1.10054] (24/27); range [0.772955, 1.41187] | incomplete |
| 50 | pinned/tardie | 0.339083 [0.285896, 0.51346] (3/3) | 0.861987 [0.770737, 0.953237] (2/3); range [0.679487, 1.04449] | 0.248325 [0.206174, 0.289606] (3/3) | 0.955645 [0.951054, 1.05258] (3/3); range [0.946463, 1.14952] | 0.236014 [0.20168, 0.316058] (27/27) | 1 [0.946761, 1.09372] (27/27); range [0.772955, 1.41187] | incomplete |
| 250 | head/base | 1.11405 [1.03358, 1.39381] (3/3) | 0.980954 [0.903121, 1.05879] (2/3); range [0.825287, 1.13662] | 1.13424 [1.00077, 1.25409] (3/3) | 0.820014 [0.80278, 0.837249] (2/3); range [0.785545, 0.854484] | 1.10752 [0.916881, 1.30329] (26/27) | 0.810235 [0.737648, 0.866749] (19/27); range [0.63597, 1.04364] | incomplete |
| 250 | control/base | 0.980954 [0.903121, 1.05879] (2/3) | 0.980954 [0.903121, 1.05879] (2/3); range [0.825287, 1.13662] | 0.820014 [0.80278, 0.837249] (2/3) | 0.820014 [0.80278, 0.837249] (2/3); range [0.785545, 0.854484] | 0.810235 [0.737648, 0.866749] (19/27) | 0.810235 [0.737648, 0.866749] (19/27); range [0.63597, 1.04364] | incomplete |
| 250 | head/control | 1.4332 [1.13588, 1.73053] (2/3) | 0.980954 [0.903121, 1.05879] (2/3); range [0.825287, 1.13662] | 1.356 [1.23003, 1.48196] (2/3) | 0.820014 [0.80278, 0.837249] (2/3); range [0.785545, 0.854484] | 1.35446 [1.03211, 1.54039] (19/27) | 0.810235 [0.737648, 0.866749] (19/27); range [0.63597, 1.04364] | incomplete |
| 250 | head/pi | 2.12843 [1.62292, 2.19787] (3/3) | 0.980954 [0.903121, 1.05879] (2/3); range [0.825287, 1.13662] | 2.23657 [2.14927, 2.3124] (3/3) | 0.820014 [0.80278, 0.837249] (2/3); range [0.785545, 0.854484] | 2.10931 [1.99744, 2.2962] (26/27) | 0.810235 [0.737648, 0.866749] (19/27); range [0.63597, 1.04364] | incomplete |
| 250 | head/tardie | 0.672052 [0.586706, 0.757399] (2/3) | 1.13662 [1.13662, 1.13662] (1/3); range [1.13662, 1.13662] | 0.508294 [0.441728, 0.545548] (3/3) | 0.820014 [0.80278, 0.837249] (2/3); range [0.785545, 0.854484] | 0.475921 [0.412151, 0.594515] (24/27) | 0.810619 [0.725832, 0.884344] (17/27); range [0.63597, 1.04364] | incomplete |
| 250 | pinned/pi | 2.05339 [1.47643, 2.31035] (3/3) | 0.980954 [0.903121, 1.05879] (2/3); range [0.825287, 1.13662] | 2.17647 [2.10742, 2.28471] (3/3) | 0.820014 [0.80278, 0.837249] (2/3); range [0.785545, 0.854484] | 2.11618 [1.79105, 2.47795] (26/27) | 0.805484 [0.73174, 0.846349] (18/27); range [0.63597, 1.04364] | incomplete |
| 250 | pinned/tardie | 0.718969 [0.601327, 0.836611] (2/3) | 1.13662 [1.13662, 1.13662] (1/3); range [1.13662, 1.13662] | 0.463224 [0.402569, 0.569789] (3/3) | 0.820014 [0.80278, 0.837249] (2/3); range [0.785545, 0.854484] | 0.43867 [0.352155, 0.609346] (24/27) | 0.805676 [0.719808, 0.857952] (16/27); range [0.63597, 1.04364] | incomplete |
| 1000 | head/base | 0.903907 [0.853884, 0.95393] (2/3) | 1.14869 [1.13805, 1.15932] (2/3); range [1.12741, 1.16996] | 1.16545 [1.1217, 1.21864] (3/3) | 1.13265 [1.11404, 1.30475] (3/3); range [1.09543, 1.47685] | 1.08717 [1.03415, 1.24781] (25/27) | 1.11137 [1.01544, 1.32572] (24/27); range [0.686543, 1.9469] | incomplete |
| 1000 | control/base | 1.12741 [1.06698, 1.14869] (3/3) | 1.12741 [1.06698, 1.14869] (3/3); range [1.00655, 1.16996] | 1.13265 [1.11404, 1.30475] (3/3) | 1.13265 [1.11404, 1.30475] (3/3); range [1.09543, 1.47685] | 1.11137 [1.01334, 1.34199] (26/27) | 1.11137 [1.01334, 1.34199] (26/27); range [0.686543, 1.9469] | incomplete |
| 1000 | head/control | 0.785561 [0.749287, 0.821835] (2/3) | 1.14869 [1.13805, 1.15932] (2/3); range [1.12741, 1.16996] | 0.984049 [0.922611, 1.0065] (3/3) | 1.13265 [1.11404, 1.30475] (3/3); range [1.09543, 1.47685] | 0.983206 [0.834644, 1.05315] (24/27) | 1.11137 [1.01544, 1.32572] (24/27); range [0.686543, 1.9469] | incomplete |
| 1000 | head/pi | 1.09871 [0.883011, 1.31442] (2/3) | 1.14869 [1.13805, 1.15932] (2/3); range [1.12741, 1.16996] | 0.879386 [0.82928, 1.13856] (3/3) | 1.13265 [1.11404, 1.30475] (3/3); range [1.09543, 1.47685] | 0.98007 [0.731684, 1.14757] (25/27) | 1.11137 [1.01544, 1.32572] (24/27); range [0.686543, 1.9469] | incomplete |
| 1000 | head/tardie | 0.183878 [0.164073, 0.203682] (2/3) | 1.14869 [1.13805, 1.15932] (2/3); range [1.12741, 1.16996] | 0.305141 [0.253808, 0.324358] (3/3) | 1.13265 [1.11404, 1.30475] (3/3); range [1.09543, 1.47685] | 0.304188 [0.211058, 0.336681] (25/27) | 1.11137 [1.01544, 1.32572] (24/27); range [0.686543, 1.9469] | incomplete |
| 1000 | pinned/pi | 1.26334 [1.23936, 1.66058] (3/3) | 1.12741 [1.06698, 1.14869] (3/3); range [1.00655, 1.16996] | 1.61842 [1.30759, 1.82494] (3/3) | 1.13265 [1.11404, 1.30475] (3/3); range [1.09543, 1.47685] | 1.55393 [1.09447, 1.89124] (27/27) | 1.11137 [1.01334, 1.34199] (26/27); range [0.686543, 1.9469] | incomplete |
| 1000 | pinned/tardie | 0.205725 [0.199875, 0.306383] (3/3) | 1.12741 [1.06698, 1.14869] (3/3); range [1.00655, 1.16996] | 0.390352 [0.381493, 0.444854] (3/3) | 1.13265 [1.11404, 1.30475] (3/3); range [1.09543, 1.47685] | 0.45329 [0.292089, 0.539644] (27/27) | 1.11137 [1.01334, 1.34199] (26/27); range [0.686543, 1.9469] | incomplete |

## RPC cpuTimeMs (ms)

Object-median eligibility: at least 7 of 9 warm observations. Per-cluster counts below retain the sample ID and mark ineligible medians.

| Size | Role | Deployed build set | Cold median [p25, p75] (clusters/3) | README warm median [p25, p75] (turns/27) | Object-median median [p25, p75] (eligible clusters/3) | Warm n per cluster | Warm-turn p95 | Coverage |
|---:|---|---|---:|---:|---:|---|---:|---|
| 50 | base | base@4.0.1 (deabe970/289122cd); 3/3 deployments matched | 587.5 [533.25, 641.75] (2/3) | 199 [170, 288.5] (27/27) | 177 [173, 259.5] (3/3) | 1:9/9; 2:9/9; 3:9/9 | 384.6 | incomplete |
| 50 | head | head@4.0.1 (fc50f45a/289122cd); 3/3 deployments matched | 376 [369.5, 598] (3/3) | 211 [184.5, 288] (27/27) | 194 [189.5, 259.5] (3/3) | 1:9/9; 2:9/9; 3:9/9 | 398.5 | complete |
| 50 | control | control@4.0.1 (deabe970/289122cd); 3/3 deployments matched | 582 [480.5, 639.5] (3/3) | 180 [157, 310.5] (27/27) | 170 [157.5, 277.5] (3/3) | 1:9/9; 2:9/9; 3:9/9 | 404 | complete |
| 50 | pinned | pinned@4.0.0 (c823d174/289122cd); 3/3 deployments matched | 614 [466.5, 778.5] (3/3) | 302 [232, 331] (27/27) | 321 [241.5, 323] (3/3) | 1:9/9; 2:9/9; 3:9/9 | 429.9 | complete |
| 50 | pi | pi 1.0.4 / no Effect (5526e585/866f1f7f); 3/3 deployments matched | 131 [130, 145] (3/3) | 99.5 [93.75, 114] (24/27) | 102 [99, 102.5] (3/3) | 1:9/9; 2:8/9; 3:7/9 | 154.75 | incomplete |
| 50 | tardie | tardie@4.0.0-rc.115 (c8fbbb72/8a0cb11b); 3/3 deployments matched | 205 [183.5, 272] (3/3) | 9 [7, 10.5] (27/27) | 9 [8, 10] (3/3) | 1:9/9; 2:9/9; 3:9/9 | 12.7 | complete |
| 250 | base | base@4.0.1 (deabe970/289122cd); base@4.0.1 (deabe970/6b2bef15); 3/3 deployments matched | 785 [649, 866.5] (3/3) | 429 [333, 531.5] (27/27) | 464 [368, 498.5] (3/3) | 0:9/9; 1:9/9; 2:9/9 | 617.7 | complete |
| 250 | head | head@4.0.1 (fc50f45a/289122cd); head@4.0.1 (fc50f45a/6b2bef15); 3/3 deployments matched | 1079 [882.5, 1126.5] (3/3) | 466.5 [302.25, 568.25] (26/27) | 516.5 [391.25, 541.25] (3/3) | 0:9/9; 1:8/9; 2:9/9 | 679.75 | incomplete |
| 250 | control | control@4.0.1 (deabe970/289122cd); control@4.0.1 (deabe970/6b2bef15); 3/3 deployments matched | 786.5 [611.75, 961.25] (2/3) | 313 [219, 434.5] (19/27) | 322 [265.5, 378.5] (2/3) | 0:7/9; 1:3/9 ineligible; 2:9/9 | 531.4 | incomplete |
| 250 | pinned | pinned@4.0.0 (c823d174/289122cd); pinned@4.0.0 (c823d174/6b2bef15); 3/3 deployments matched | 907 [901, 998] (3/3) | 515 [447, 575.25] (26/27) | 513 [500, 518] (3/3) | 0:9/9; 1:9/9; 2:8/9 | 727.5 | incomplete |
| 250 | pi | pi 1.0.4 / no Effect (5526e585/866f1f7f); pi 1.0.4 / no Effect (5526e585/f1947b8f); 3/3 deployments matched | 224 [207.5, 230] (3/3) | 157 [143.5, 188.5] (27/27) | 154 [153.5, 171.5] (3/3) | 0:9/9; 1:9/9; 2:9/9 | 271.6 | complete |
| 250 | tardie | tardie@4.0.0-rc.115 (c8fbbb72/8a0cb11b); tardie@4.0.0-rc.115 (c8fbbb72/9f22f5b1); 3/3 deployments matched | 465 [426, 504] (2/3) | 16 [14, 24] (25/27) | 15 [14.5, 19.75] (3/3) | 0:8/9; 1:9/9; 2:8/9 | 27.8 | incomplete |
| 1000 | base | base@4.0.1 (deabe970/6b2bef15); 3/3 deployments matched | 872 [870, 885] (3/3) | 466 [444.5, 552.5] (27/27) | 464 [460.5, 479] (3/3) | 0:9/9; 1:9/9; 2:9/9 | 753.8 | complete |
| 1000 | head | head@4.0.1 (fc50f45a/6b2bef15); 3/3 deployments matched | 816 [765.5, 866.5] (2/3) | 513 [480, 621] (25/27) | 565 [528.5, 609.5] (3/3) | 0:9/9; 1:8/9; 2:8/9 | 860.8 | incomplete |
| 1000 | control | control@4.0.1 (deabe970/6b2bef15); 3/3 deployments matched | 929 [873.5, 1057.5] (3/3) | 485 [454.75, 816.5] (26/27) | 478 [469, 669.5] (3/3) | 0:9/9; 1:8/9; 2:9/9 | 1046 | incomplete |
| 1000 | pinned | pinned@4.0.0 (c823d174/6b2bef15); 3/3 deployments matched | 1470 [1452.5, 1515.5] (3/3) | 1058 [928.5, 1211.5] (27/27) | 971 [965.5, 1099] (3/3) | 0:9/9; 1:9/9; 2:9/9 | 1332.7 | complete |
| 1000 | pi | pi 1.0.4 / no Effect (5526e585/f1947b8f); 3/3 deployments matched | 1075 [815.5, 1078.5] (3/3) | 754 [462, 887] (27/27) | 774 [605, 825.5] (3/3) | 0:9/9; 1:9/9; 2:9/9 | 1035.3 | complete |
| 1000 | tardie | tardie@4.0.0-rc.115 (c8fbbb72/9f22f5b1); 3/3 deployments matched | 2347 [1824, 2911] (3/3) | 48 [37.5, 56.5] (27/27) | 47 [40.5, 52.5] (3/3) | 0:9/9; 1:9/9; 2:9/9 | 78.5 | complete |

| Size | Pair | Cold ratio median [IQR] (pairs/3) | Control/base cold spread | Object-median ratio median [IQR] (pairs/3) | Control/base Object spread | Paired-turn ratio median [IQR] (turn pairs/27; diagnostic) | Control/base turn spread | Status |
|---:|---|---:|---:|---:|---:|---:|---:|---|
| 50 | head/base | 0.967995 [0.862912, 1.07308] (2/3) | 0.896334 [0.843783, 0.948886] (2/3); range [0.791232, 1.00144] | 1.0452 [0.997745, 1.09656] (3/3) | 1.00592 [0.912563, 1.06582] (3/3); range [0.819209, 1.12573] | 1.09605 [0.920764, 1.20244] (27/27) | 0.974874 [0.863708, 1.08526] (27/27); range [0.657609, 1.4794] | incomplete |
| 50 | control/base | 0.896334 [0.843783, 0.948886] (2/3) | 0.896334 [0.843783, 0.948886] (2/3); range [0.791232, 1.00144] | 1.00592 [0.912563, 1.06582] (3/3) | 1.00592 [0.912563, 1.06582] (3/3); range [0.819209, 1.12573] | 0.974874 [0.863708, 1.08526] (27/27) | 0.974874 [0.863708, 1.08526] (27/27); range [0.657609, 1.4794] | incomplete |
| 50 | head/control | 0.957784 [0.801916, 1.06713] (3/3) | 0.896334 [0.843783, 0.948886] (2/3); range [0.791232, 1.00144] | 1.14118 [0.992666, 1.20852] (3/3) | 1.00592 [0.912563, 1.06582] (3/3); range [0.819209, 1.12573] | 1.07164 [0.968515, 1.18635] (27/27) | 0.974874 [0.863708, 1.08526] (27/27); range [0.657609, 1.4794] | incomplete |
| 50 | head/pi | 2.87023 [2.57662, 4.61341] (3/3) | 0.896334 [0.843783, 0.948886] (2/3); range [0.791232, 1.00144] | 1.92708 [1.90529, 2.55668] (3/3) | 1.00592 [0.912563, 1.06582] (3/3); range [0.819209, 1.12573] | 2.11954 [1.68794, 2.68705] (24/27) | 0.971412 [0.860339, 1.11992] (24/27); range [0.657609, 1.4794] | incomplete |
| 50 | head/tardie | 2.32099 [2.04586, 2.36993] (3/3) | 0.896334 [0.843783, 0.948886] (2/3); range [0.791232, 1.00144] | 26.4286 [23.9921, 27.987] (3/3) | 1.00592 [0.912563, 1.06582] (3/3); range [0.819209, 1.12573] | 26.2857 [23.1508, 31.3056] (27/27) | 0.974874 [0.863708, 1.08526] (27/27); range [0.657609, 1.4794] | incomplete |
| 50 | pinned/pi | 4.75969 [3.38299, 5.97908] (3/3) | 0.896334 [0.843783, 0.948886] (2/3); range [0.791232, 1.00144] | 3.14706 [2.35994, 3.26624] (3/3) | 1.00592 [0.912563, 1.06582] (3/3); range [0.819209, 1.12573] | 2.9949 [1.82288, 3.47872] (24/27) | 0.971412 [0.860339, 1.11992] (24/27); range [0.657609, 1.4794] | incomplete |
| 50 | pinned/tardie | 1.81121 [1.68365, 3.8161] (3/3) | 0.896334 [0.843783, 0.948886] (2/3); range [0.791232, 1.00144] | 29.1818 [23.5909, 37.8052] (3/3) | 1.00592 [0.912563, 1.06582] (3/3); range [0.819209, 1.12573] | 32.9 [24, 43.381] (27/27) | 0.974874 [0.863708, 1.08526] (27/27); range [0.657609, 1.4794] | incomplete |
| 250 | head/base | 1.13819 [1.00604, 1.71334] (3/3) | 1.14949 [1.00067, 1.29831] (2/3); range [0.851852, 1.44713] | 0.969043 [0.77116, 1.52496] (3/3) | 0.852941 [0.810662, 0.895221] (2/3); range [0.768382, 0.9375] | 0.984905 [0.696757, 1.71758] (26/27) | 0.848361 [0.730558, 1.05666] (19/27); range [0.363977, 1.31481] | incomplete |
| 250 | control/base | 1.14949 [1.00067, 1.29831] (2/3) | 1.14949 [1.00067, 1.29831] (2/3); range [0.851852, 1.44713] | 0.852941 [0.810662, 0.895221] (2/3) | 0.852941 [0.810662, 0.895221] (2/3); range [0.768382, 0.9375] | 0.848361 [0.730558, 1.05666] (19/27) | 0.848361 [0.730558, 1.05666] (19/27); range [0.363977, 1.31481] | incomplete |
| 250 | head/control | 1.64519 [1.12453, 2.16584] (2/3) | 1.14949 [1.00067, 1.29831] (2/3); range [0.851852, 1.44713] | 1.65981 [1.13565, 2.18397] (2/3) | 0.852941 [0.810662, 0.895221] (2/3); range [0.768382, 0.9375] | 1.62939 [0.620955, 2.35443] (19/27) | 0.848361 [0.730558, 1.05666] (19/27); range [0.363977, 1.31481] | incomplete |
| 250 | head/pi | 4.81696 [3.86187, 5.48178] (3/3) | 1.14949 [1.00067, 1.29831] (2/3); range [0.851852, 1.44713] | 2.7328 [2.23568, 3.20406] (3/3) | 0.852941 [0.810662, 0.895221] (2/3); range [0.768382, 0.9375] | 2.40367 [1.87657, 3.28675] (26/27) | 0.848361 [0.730558, 1.05666] (19/27); range [0.363977, 1.31481] | incomplete |
| 250 | head/tardie | 1.87986 [1.82623, 1.93348] (2/3) | 1.44713 [1.44713, 1.44713] (1/3); range [1.44713, 1.44713] | 23.102 [21.051, 28.7677] (3/3) | 0.852941 [0.810662, 0.895221] (2/3); range [0.768382, 0.9375] | 22.18 [17.6382, 30.1412] (24/27) | 0.848361 [0.767782, 1.07752] (17/27); range [0.363977, 1.31481] | incomplete |
| 250 | pinned/pi | 4.68586 [4.26454, 4.77374] (3/3) | 1.14949 [1.00067, 1.29831] (2/3); range [0.851852, 1.44713] | 3.18301 [2.9751, 3.25709] (3/3) | 0.852941 [0.810662, 0.895221] (2/3); range [0.768382, 0.9375] | 2.93452 [2.38052, 3.6477] (26/27) | 0.845136 [0.711946, 1.01316] (18/27); range [0.363977, 1.31481] | incomplete |
| 250 | pinned/tardie | 2.1746 [2.09006, 2.25913] (2/3) | 1.44713 [1.44713, 1.44713] (1/3); range [1.44713, 1.44713] | 34.7857 [27.8622, 34.8262] (3/3) | 0.852941 [0.810662, 0.895221] (2/3); range [0.768382, 0.9375] | 29.8375 [20.7193, 37.7917] (24/27) | 0.845136 [0.744711, 1.04623] (16/27); range [0.363977, 1.31481] | incomplete |
| 1000 | head/base | 0.937669 [0.880701, 0.994637] (2/3) | 1.21586 [1.14062, 1.29111] (2/3); range [1.06537, 1.36636] | 1.21767 [1.14713, 1.27078] (3/3) | 1.03017 [1.01837, 1.38654] (3/3); range [1.00656, 1.74291] | 1.06701 [0.978118, 1.29124] (25/27) | 1.08837 [0.873122, 1.34326] (24/27); range [0.512897, 2.46469] | incomplete |
| 1000 | control/base | 1.06537 [0.98814, 1.21586] (3/3) | 1.06537 [0.98814, 1.21586] (3/3); range [0.910913, 1.36636] | 1.03017 [1.01837, 1.38654] (3/3) | 1.03017 [1.01837, 1.38654] (3/3); range [1.00656, 1.74291] | 1.08837 [0.868818, 1.39041] (26/27) | 1.08837 [0.868818, 1.39041] (26/27); range [0.512897, 2.46469] | incomplete |
| 1000 | head/control | 0.794975 [0.698921, 0.891029] (2/3) | 1.21586 [1.14062, 1.29111] (2/3); range [1.06537, 1.36636] | 1.06957 [0.914574, 1.12579] (3/3) | 1.03017 [1.01837, 1.38654] (3/3); range [1.00656, 1.74291] | 1.08009 [0.83469, 1.11181] (24/27) | 1.08837 [0.873122, 1.34326] (24/27); range [0.512897, 2.46469] | incomplete |
| 1000 | head/pi | 1.1572 [0.911157, 1.40324] (2/3) | 1.21586 [1.14062, 1.29111] (2/3); range [1.06537, 1.36636] | 0.745724 [0.690691, 1.0208] (3/3) | 1.03017 [1.01837, 1.38654] (3/3); range [1.00656, 1.74291] | 0.740295 [0.570071, 1.16313] (25/27) | 1.08837 [0.873122, 1.34326] (24/27); range [0.512897, 2.46469] | incomplete |
| 1000 | head/tardie | 0.406731 [0.335308, 0.478154] (2/3) | 1.21586 [1.14062, 1.29111] (2/3); range [1.06537, 1.36636] | 10.4681 [10.1047, 14.8517] (3/3) | 1.03017 [1.01837, 1.38654] (3/3); range [1.00656, 1.74291] | 11.1277 [9.46154, 13.375] (25/27) | 1.08837 [0.873122, 1.34326] (24/27); range [0.512897, 2.46469] | incomplete |
| 1000 | pinned/pi | 1.45209 [1.38917, 2.04799] (3/3) | 1.06537 [0.98814, 1.21586] (3/3); range [0.910913, 1.36636] | 1.58527 [1.33996, 1.90617] (3/3) | 1.03017 [1.01837, 1.38654] (3/3); range [1.00656, 1.74291] | 1.46313 [1.11497, 1.89579] (27/27) | 1.08837 [0.868818, 1.39041] (26/27); range [0.512897, 2.46469] | incomplete |
| 1000 | pinned/tardie | 0.611419 [0.51722, 0.905633] (3/3) | 1.06537 [0.98814, 1.21586] (3/3); range [0.910913, 1.36636] | 26.1064 [21.4239, 27.1708] (3/3) | 1.03017 [1.01837, 1.38654] (3/3); range [1.00656, 1.74291] | 22.8571 [18.2179, 26.7112] (27/27) | 1.08837 [0.868818, 1.39041] (26/27); range [0.512897, 2.46469] | incomplete |

## RPC doWallMinusCpuMs (ms)

Object-median eligibility: at least 7 of 9 warm observations. Per-cluster counts below retain the sample ID and mark ineligible medians.

| Size | Role | Deployed build set | Cold median [p25, p75] (clusters/3) | README warm median [p25, p75] (turns/27) | Object-median median [p25, p75] (eligible clusters/3) | Warm n per cluster | Warm-turn p95 | Coverage |
|---:|---|---|---:|---:|---:|---|---:|---|
| 50 | base | base@4.0.1 (deabe970/289122cd); 3/3 deployments matched | 475.5 [406.75, 544.25] (2/3) | 328 [283.5, 335.5] (27/27) | 330 [301, 333.5] (3/3) | 1:9/9; 2:9/9; 3:9/9 | 377.4 | incomplete |
| 50 | head | head@4.0.1 (fc50f45a/289122cd); 3/3 deployments matched | 374 [367.5, 381] (3/3) | 307 [294.5, 322] (27/27) | 307 [297, 314] (3/3) | 1:9/9; 2:9/9; 3:9/9 | 339.5 | complete |
| 50 | control | control@4.0.1 (deabe970/289122cd); 3/3 deployments matched | 383 [373, 393] (3/3) | 330 [318, 344.5] (27/27) | 330 [318.5, 335.5] (3/3) | 1:9/9; 2:9/9; 3:9/9 | 362 | complete |
| 50 | pinned | pinned@4.0.0 (c823d174/289122cd); 3/3 deployments matched | 310 [263, 447] (3/3) | 272 [178, 302.5] (27/27) | 272 [224, 292.5] (3/3) | 1:9/9; 2:9/9; 3:9/9 | 323.7 | complete |
| 50 | pi | pi 1.0.4 / no Effect (5526e585/866f1f7f); 3/3 deployments matched | 340 [314, 381] (3/3) | 183.5 [179, 190] (24/27) | 180 [179.75, 187] (3/3) | 1:9/9; 2:8/9; 3:7/9 | 217.2 | incomplete |
| 50 | tardie | tardie@4.0.0-rc.115 (c8fbbb72/8a0cb11b); 3/3 deployments matched | 2094 [2076, 2240] (3/3) | 2066 [1924.5, 2242.5] (27/27) | 2005 [1952.5, 2191] (3/3) | 1:9/9; 2:9/9; 3:9/9 | 2724.4 | complete |
| 250 | base | base@4.0.1 (deabe970/289122cd); base@4.0.1 (deabe970/6b2bef15); 3/3 deployments matched | 376 [366.5, 414] (3/3) | 311 [242, 371.5] (27/27) | 311 [271.5, 346.5] (3/3) | 0:9/9; 1:9/9; 2:9/9 | 396.9 | complete |
| 250 | head | head@4.0.1 (fc50f45a/289122cd); head@4.0.1 (fc50f45a/6b2bef15); 3/3 deployments matched | 396 [339, 444.5] (3/3) | 348 [245, 411.75] (26/27) | 348 [290.5, 393.5] (3/3) | 0:9/9; 1:8/9; 2:9/9 | 463.25 | incomplete |
| 250 | control | control@4.0.1 (deabe970/289122cd); control@4.0.1 (deabe970/6b2bef15); 3/3 deployments matched | 275.5 [272.75, 278.25] (2/3) | 254 [223.5, 285] (19/27) | 244 [233, 255] (2/3) | 0:7/9; 1:3/9 ineligible; 2:9/9 | 298.5 | incomplete |
| 250 | pinned | pinned@4.0.0 (c823d174/289122cd); pinned@4.0.0 (c823d174/6b2bef15); 3/3 deployments matched | 334 [305.5, 381] (3/3) | 240.5 [223.5, 353.75] (26/27) | 232 [227.5, 296.75] (3/3) | 0:9/9; 1:9/9; 2:8/9 | 385.5 | incomplete |
| 250 | pi | pi 1.0.4 / no Effect (5526e585/866f1f7f); pi 1.0.4 / no Effect (5526e585/f1947b8f); 3/3 deployments matched | 469 [376.5, 790.5] (3/3) | 194 [188.5, 199.5] (27/27) | 194 [190.5, 198] (3/3) | 0:9/9; 1:9/9; 2:9/9 | 203.7 | complete |
| 250 | tardie | tardie@4.0.0-rc.115 (c8fbbb72/8a0cb11b); tardie@4.0.0-rc.115 (c8fbbb72/9f22f5b1); 3/3 deployments matched | 1705.5 [1358.75, 2052.25] (2/3) | 1902 [1220, 2205] (25/27) | 1572.5 [1406.25, 1943.25] (3/3) | 0:8/9; 1:9/9; 2:8/9 | 2640.4 | incomplete |
| 1000 | base | base@4.0.1 (deabe970/6b2bef15); 3/3 deployments matched | 393 [358, 410] (3/3) | 258 [236, 282] (27/27) | 258 [236, 273] (3/3) | 0:9/9; 1:9/9; 2:9/9 | 295.7 | complete |
| 1000 | head | head@4.0.1 (fc50f45a/6b2bef15); 3/3 deployments matched | 339.5 [332.75, 346.25] (2/3) | 306 [236, 313] (25/27) | 302.5 [267.75, 307.75] (3/3) | 0:9/9; 1:8/9; 2:8/9 | 357.4 | incomplete |
| 1000 | control | control@4.0.1 (deabe970/6b2bef15); 3/3 deployments matched | 411 [342.5, 481] (3/3) | 299.5 [272, 341] (26/27) | 296 [271.75, 327] (3/3) | 0:9/9; 1:8/9; 2:9/9 | 371.5 | incomplete |
| 1000 | pinned | pinned@4.0.0 (c823d174/6b2bef15); 3/3 deployments matched | 335 [286.5, 373.5] (3/3) | 237 [201.5, 267.5] (27/27) | 205 [196, 234] (3/3) | 0:9/9; 1:9/9; 2:9/9 | 423.1 | complete |
| 1000 | pi | pi 1.0.4 / no Effect (5526e585/f1947b8f); 3/3 deployments matched | 380 [327, 432.5] (3/3) | 155 [137.5, 296.5] (27/27) | 152 [145, 254.5] (3/3) | 0:9/9; 1:9/9; 2:9/9 | 373 | complete |
| 1000 | tardie | tardie@4.0.0-rc.115 (c8fbbb72/9f22f5b1); 3/3 deployments matched | 5328 [4342.5, 5979.5] (3/3) | 2890 [2284, 4069.5] (27/27) | 3117 [2685, 3515.5] (3/3) | 0:9/9; 1:9/9; 2:9/9 | 6152 | complete |

| Size | Pair | Cold ratio median [IQR] (pairs/3) | Control/base cold spread | Object-median ratio median [IQR] (pairs/3) | Control/base Object spread | Paired-turn ratio median [IQR] (turn pairs/27; diagnostic) | Control/base turn spread | Status |
|---:|---|---:|---:|---:|---:|---:|---:|---|
| 50 | head/base | 0.869731 [0.751342, 0.98812] (2/3) | 0.862653 [0.727411, 0.997894] (2/3); range [0.59217, 1.13314] | 0.972727 [0.941853, 1.01394] (3/3) | 1.01187 [0.971086, 1.11255] (3/3); range [0.930303, 1.21324] | 0.99308 [0.933731, 1.07774] (27/27) | 1.02424 [0.972156, 1.18663] (27/27); range [0.766667, 1.408] | incomplete |
| 50 | control/base | 0.862653 [0.727411, 0.997894] (2/3) | 0.862653 [0.727411, 0.997894] (2/3); range [0.59217, 1.13314] | 1.01187 [0.971086, 1.11255] (3/3) | 1.01187 [0.971086, 1.11255] (3/3); range [0.930303, 1.21324] | 1.02424 [0.972156, 1.18663] (27/27) | 1.02424 [0.972156, 1.18663] (27/27); range [0.766667, 1.408] | incomplete |
| 50 | head/control | 0.976501 [0.936141, 1.02269] (3/3) | 0.862653 [0.727411, 0.997894] (2/3); range [0.59217, 1.13314] | 0.900293 [0.884995, 0.972948] (3/3) | 1.01187 [0.971086, 1.11255] (3/3); range [0.930303, 1.21324] | 0.929825 [0.876235, 1.02072] (27/27) | 1.02424 [0.972156, 1.18663] (27/27); range [0.766667, 1.408] | incomplete |
| 50 | head/pi | 1.06176 [0.97401, 1.20449] (3/3) | 0.862653 [0.727411, 0.997894] (2/3); range [0.59217, 1.13314] | 1.65464 [1.62454, 1.68247] (3/3) | 1.01187 [0.971086, 1.11255] (3/3); range [0.930303, 1.21324] | 1.66483 [1.59188, 1.69169] (24/27) | 1.02576 [0.973957, 1.19181] (24/27); range [0.766667, 1.408] | incomplete |
| 50 | head/tardie | 0.175413 [0.16608, 0.180352] (3/3) | 0.862653 [0.727411, 0.997894] (2/3); range [0.59217, 1.13314] | 0.1601 [0.14042, 0.160839] (3/3) | 1.01187 [0.971086, 1.11255] (3/3); range [0.930303, 1.21324] | 0.158105 [0.129936, 0.163385] (27/27) | 1.02424 [0.972156, 1.18663] (27/27); range [0.766667, 1.408] | incomplete |
| 50 | pinned/pi | 0.75 [0.742299, 1.23382] (3/3) | 0.862653 [0.727411, 0.997894] (2/3); range [0.59217, 1.13314] | 1.51111 [1.20916, 1.62742] (3/3) | 1.01187 [0.971086, 1.11255] (3/3); range [0.930303, 1.21324] | 1.51955 [1.01497, 1.6582] (24/27) | 1.02576 [0.973957, 1.19181] (24/27); range [0.766667, 1.408] | incomplete |
| 50 | pinned/tardie | 0.129925 [0.116538, 0.206848] (3/3) | 0.862653 [0.727411, 0.997894] (2/3); range [0.59217, 1.13314] | 0.11443 [0.101105, 0.139583] (3/3) | 1.01187 [0.971086, 1.11255] (3/3); range [0.930303, 1.21324] | 0.11443 [0.0904384, 0.147188] (27/27) | 1.02424 [0.972156, 1.18663] (27/27); range [0.766667, 1.408] | incomplete |
| 250 | head/base | 1.05319 [0.921554, 1.07195] (3/3) | 0.69223 [0.644788, 0.739672] (2/3); range [0.597345, 0.787115] | 1.14921 [0.949205, 1.32461] (3/3) | 0.718229 [0.64969, 0.786767] (2/3); range [0.581152, 0.855305] | 1.12279 [0.787252, 1.3849] (26/27) | 0.772036 [0.586029, 0.913625] (19/27); range [0.545455, 1.27311] | incomplete |
| 250 | control/base | 0.69223 [0.644788, 0.739672] (2/3) | 0.69223 [0.644788, 0.739672] (2/3); range [0.597345, 0.787115] | 0.718229 [0.64969, 0.786767] (2/3) | 0.718229 [0.64969, 0.786767] (2/3); range [0.581152, 0.855305] | 0.772036 [0.586029, 0.913625] (19/27) | 0.772036 [0.586029, 0.913625] (19/27); range [0.545455, 1.27311] | incomplete |
| 250 | head/control | 1.41474 [1.20915, 1.62033] (2/3) | 0.69223 [0.644788, 0.739672] (2/3); range [0.597345, 0.787115] | 1.42671 [1.15132, 1.70209] (2/3) | 0.718229 [0.64969, 0.786767] (2/3); range [0.581152, 0.855305] | 1.32993 [0.930466, 1.93056] (19/27) | 0.772036 [0.586029, 0.913625] (19/27); range [0.545455, 1.27311] | incomplete |
| 250 | head/pi | 0.84435 [0.548973, 1.29013] (3/3) | 0.69223 [0.644788, 0.739672] (2/3); range [0.597345, 0.787115] | 1.79381 [1.5199, 1.98354] (3/3) | 0.718229 [0.64969, 0.786767] (2/3); range [0.581152, 0.855305] | 1.77668 [1.32205, 2.0589] (26/27) | 0.772036 [0.586029, 0.913625] (19/27); range [0.545455, 1.27311] | incomplete |
| 250 | head/tardie | 0.326111 [0.24559, 0.406633] (2/3) | 0.597345 [0.597345, 0.597345] (1/3); range [0.597345, 0.597345] | 0.150389 [0.14928, 0.252211] (3/3) | 0.718229 [0.64969, 0.786767] (2/3); range [0.581152, 0.855305] | 0.192164 [0.136713, 0.306794] (24/27) | 0.772036 [0.581633, 0.9279] (17/27); range [0.545455, 1.27311] | incomplete |
| 250 | pinned/pi | 0.712154 [0.480627, 1.1096] (3/3) | 0.69223 [0.644788, 0.739672] (2/3); range [0.597345, 0.787115] | 1.19588 [1.19419, 1.49274] (3/3) | 0.718229 [0.64969, 0.786767] (2/3); range [0.581152, 0.855305] | 1.24985 [1.18162, 1.73318] (26/27) | 0.793139 [0.587434, 0.920762] (18/27); range [0.545455, 1.27311] | incomplete |
| 250 | pinned/tardie | 0.281075 [0.21015, 0.352] (2/3) | 0.597345 [0.597345, 0.597345] (1/3); range [0.597345, 0.597345] | 0.141812 [0.121036, 0.216672] (3/3) | 0.718229 [0.64969, 0.786767] (2/3); range [0.581152, 0.855305] | 0.14689 [0.105338, 0.257829] (24/27) | 0.793139 [0.580105, 0.936247] (16/27); range [0.545455, 1.27311] | incomplete |
| 1000 | head/base | 0.830842 [0.797154, 0.864531] (2/3) | 1.02186 [0.831774, 1.21195] (2/3); range [0.641686, 1.40204] | 1.08879 [1.0878, 1.13063] (3/3) | 1.24306 [1.10118, 1.31312] (3/3); range [0.959302, 1.38318] | 1.11848 [1.06545, 1.20946] (25/27) | 1.23552 [1.07232, 1.32587] (24/27); range [0.883212, 1.55607] | incomplete |
| 1000 | control/base | 1.27245 [0.957066, 1.33724] (3/3) | 1.27245 [0.957066, 1.33724] (3/3); range [0.641686, 1.40204] | 1.24306 [1.10118, 1.31312] (3/3) | 1.24306 [1.10118, 1.31312] (3/3); range [0.959302, 1.38318] | 1.23039 [1.07191, 1.3028] (26/27) | 1.23039 [1.07191, 1.3028] (26/27); range [0.883212, 1.55607] | incomplete |
| 1000 | head/control | 0.915217 [0.777935, 1.0525] (2/3) | 1.02186 [0.831774, 1.21195] (2/3); range [0.641686, 1.40204] | 0.874302 [0.830732, 1.04826] (3/3) | 1.24306 [1.10118, 1.31312] (3/3); range [0.959302, 1.38318] | 0.903226 [0.773397, 1.09787] (24/27) | 1.23552 [1.07232, 1.32587] (24/27); range [0.883212, 1.55607] | incomplete |
| 1000 | head/pi | 0.980243 [0.826204, 1.13428] (2/3) | 1.02186 [0.831774, 1.21195] (2/3); range [0.641686, 1.40204] | 1.68841 [1.26787, 1.87381] (3/3) | 1.24306 [1.10118, 1.31312] (3/3); range [0.959302, 1.38318] | 1.62238 [1.01736, 1.93989] (25/27) | 1.23552 [1.07232, 1.32587] (24/27); range [0.883212, 1.55607] | incomplete |
| 1000 | head/tardie | 0.0816821 [0.0739679, 0.0893963] (2/3) | 1.02186 [0.831774, 1.21195] (2/3); range [0.641686, 1.40204] | 0.0970484 [0.0885089, 0.100233] (3/3) | 1.24306 [1.10118, 1.31312] (3/3); range [0.959302, 1.38318] | 0.0978499 [0.0799693, 0.11366] (25/27) | 1.23552 [1.07232, 1.32587] (24/27); range [0.883212, 1.55607] | incomplete |
| 1000 | pinned/pi | 0.868613 [0.779667, 0.976412] (3/3) | 1.27245 [0.957066, 1.33724] (3/3); range [0.641686, 1.40204] | 1.48551 [1.00466, 1.60789] (3/3) | 1.24306 [1.10118, 1.31312] (3/3); range [0.959302, 1.38318] | 1.45304 [0.834791, 1.87892] (27/27) | 1.23039 [1.07191, 1.3028] (26/27); range [0.883212, 1.55607] | incomplete |
| 1000 | pinned/tardie | 0.0621324 [0.053401, 0.0809619] (3/3) | 1.27245 [0.957066, 1.33724] (3/3); range [0.641686, 1.40204] | 0.0671947 [0.0635941, 0.0790922] (3/3) | 1.24306 [1.10118, 1.31312] (3/3); range [0.959302, 1.38318] | 0.0882353 [0.0531705, 0.106245] (27/27) | 1.23039 [1.07191, 1.3028] (26/27); range [0.883212, 1.55607] | incomplete |

## RPC clientWallMinusDoWallMs (ms)

Object-median eligibility: at least 7 of 9 warm observations. Per-cluster counts below retain the sample ID and mark ineligible medians.

| Size | Role | Deployed build set | Cold median [p25, p75] (clusters/3) | README warm median [p25, p75] (turns/27) | Object-median median [p25, p75] (eligible clusters/3) | Warm n per cluster | Warm-turn p95 | Coverage |
|---:|---|---|---:|---:|---:|---|---:|---|
| 50 | base | base@4.0.1 (deabe970/289122cd); 3/3 deployments matched | 192 [191, 193] (2/3) | 191 [188, 196.5] (27/27) | 193 [192, 194.5] (3/3) | 1:9/9; 2:9/9; 3:9/9 | 205.7 | incomplete |
| 50 | head | head@4.0.1 (fc50f45a/289122cd); 3/3 deployments matched | 184 [161, 191.5] (3/3) | 182 [137, 196.5] (27/27) | 182 [156.5, 189.5] (3/3) | 1:9/9; 2:9/9; 3:9/9 | 201 | complete |
| 50 | control | control@4.0.1 (deabe970/289122cd); 3/3 deployments matched | 183 [181, 187.5] (3/3) | 187 [180.5, 196] (27/27) | 187 [183, 192] (3/3) | 1:9/9; 2:9/9; 3:9/9 | 200.4 | complete |
| 50 | pinned | pinned@4.0.0 (c823d174/289122cd); 3/3 deployments matched | 129 [94.5, 159.5] (3/3) | 128 [72, 187] (27/27) | 128 [97.5, 158] (3/3) | 1:9/9; 2:9/9; 3:9/9 | 192.7 | complete |
| 50 | pi | pi 1.0.4 / no Effect (5526e585/866f1f7f); 3/3 deployments matched | 186 [158, 195.5] (3/3) | 184 [124.75, 187.25] (24/27) | 185 [154, 186] (3/3) | 1:9/9; 2:8/9; 3:7/9 | 191.4 | incomplete |
| 50 | tardie | tardie@4.0.0-rc.115 (c8fbbb72/8a0cb11b); 3/3 deployments matched | 183 [173, 197] (3/3) | 182 [167, 200.5] (27/27) | 182 [173, 192] (3/3) | 1:9/9; 2:9/9; 3:9/9 | 228.3 | complete |
| 250 | base | base@4.0.1 (deabe970/289122cd); base@4.0.1 (deabe970/6b2bef15); 3/3 deployments matched | 133 [128, 159] (3/3) | 133 [128, 183.5] (27/27) | 131 [130, 163] (3/3) | 0:9/9; 1:9/9; 2:9/9 | 310.1 | complete |
| 250 | head | head@4.0.1 (fc50f45a/289122cd); head@4.0.1 (fc50f45a/6b2bef15); 3/3 deployments matched | 167 [144, 186] (3/3) | 129.5 [124.25, 196.5] (26/27) | 128 [125.5, 163] (3/3) | 0:9/9; 1:8/9; 2:9/9 | 265 | incomplete |
| 250 | control | control@4.0.1 (deabe970/289122cd); control@4.0.1 (deabe970/6b2bef15); 3/3 deployments matched | 156 [145.5, 166.5] (2/3) | 142 [126, 178] (19/27) | 153 [139, 167] (2/3) | 0:7/9; 1:3/9 ineligible; 2:9/9 | 182.3 | incomplete |
| 250 | pinned | pinned@4.0.0 (c823d174/289122cd); pinned@4.0.0 (c823d174/6b2bef15); 3/3 deployments matched | 189 [167, 189.5] (3/3) | 143 [128, 183] (26/27) | 138 [133.5, 161] (3/3) | 0:9/9; 1:9/9; 2:8/9 | 216.5 | incomplete |
| 250 | pi | pi 1.0.4 / no Effect (5526e585/866f1f7f); pi 1.0.4 / no Effect (5526e585/f1947b8f); 3/3 deployments matched | 190 [187, 194] (3/3) | 195 [188, 197.5] (27/27) | 193 [188, 194.5] (3/3) | 0:9/9; 1:9/9; 2:9/9 | 246.8 | complete |
| 250 | tardie | tardie@4.0.0-rc.115 (c8fbbb72/8a0cb11b); tardie@4.0.0-rc.115 (c8fbbb72/9f22f5b1); 3/3 deployments matched | 111 [105.5, 116.5] (2/3) | 122 [119, 183] (25/27) | 120 [109.5, 151.5] (3/3) | 0:8/9; 1:9/9; 2:8/9 | 188 | incomplete |
| 1000 | base | base@4.0.1 (deabe970/6b2bef15); 3/3 deployments matched | 116 [98.5, 158.5] (3/3) | 90 [79, 193.5] (27/27) | 80 [71.5, 137.5] (3/3) | 0:9/9; 1:9/9; 2:9/9 | 221.3 | complete |
| 1000 | head | head@4.0.1 (fc50f45a/6b2bef15); 3/3 deployments matched | 94 [93.5, 94.5] (2/3) | 97 [22, 136] (25/27) | 93 [56.5, 119] (3/3) | 0:9/9; 1:8/9; 2:8/9 | 165.4 | incomplete |
| 1000 | control | control@4.0.1 (deabe970/6b2bef15); 3/3 deployments matched | 138 [89, 157.5] (3/3) | 104 [31.5, 108.75] (26/27) | 105 [66.5, 106.5] (3/3) | 0:9/9; 1:8/9; 2:9/9 | 131.25 | incomplete |
| 1000 | pinned | pinned@4.0.0 (c823d174/6b2bef15); 3/3 deployments matched | 102 [71, 173.5] (3/3) | 62 [43.5, 99.5] (27/27) | 66 [49, 79.5] (3/3) | 0:9/9; 1:9/9; 2:9/9 | 122.7 | complete |
| 1000 | pi | pi 1.0.4 / no Effect (5526e585/f1947b8f); 3/3 deployments matched | 117 [98, 153] (3/3) | 131 [93.5, 189] (27/27) | 114 [102.5, 151.5] (3/3) | 0:9/9; 1:9/9; 2:9/9 | 234 | complete |
| 1000 | tardie | tardie@4.0.0-rc.115 (c8fbbb72/9f22f5b1); 3/3 deployments matched | 95 [65, 98] (3/3) | 89 [48, 94] (27/27) | 90 [63.5, 90.5] (3/3) | 0:9/9; 1:9/9; 2:9/9 | 155.4 | complete |

| Size | Pair | Cold ratio median [IQR] (pairs/3) | Control/base cold spread | Object-median ratio median [IQR] (pairs/3) | Control/base Object spread | Paired-turn ratio median [IQR] (turn pairs/27; diagnostic) | Control/base turn spread | Status |
|---:|---|---:|---:|---:|---:|---:|---:|---|
| 50 | head/base | 0.997911 [0.973182, 1.02264] (2/3) | 0.942919 [0.9328, 0.953039] (2/3); range [0.92268, 0.963158] | 0.943005 [0.814435, 0.974054] (3/3) | 0.954082 [0.940771, 0.992748] (3/3); range [0.927461, 1.03141] | 0.944444 [0.73267, 0.973233] (27/27) | 0.958549 [0.923159, 1.03446] (27/27); range [0.883838, 1.15135] | incomplete |
| 50 | control/base | 0.942919 [0.9328, 0.953039] (2/3) | 0.942919 [0.9328, 0.953039] (2/3); range [0.92268, 0.963158] | 0.954082 [0.940771, 0.992748] (3/3) | 0.954082 [0.940771, 0.992748] (3/3); range [0.927461, 1.03141] | 0.958549 [0.923159, 1.03446] (27/27) | 0.958549 [0.923159, 1.03446] (27/27); range [0.883838, 1.15135] | incomplete |
| 50 | head/control | 1.02793 [0.873341, 1.05768] (3/3) | 0.942919 [0.9328, 0.953039] (2/3); range [0.92268, 0.963158] | 1.01676 [0.840867, 1.03512] (3/3) | 0.954082 [0.940771, 0.992748] (3/3); range [0.927461, 1.03141] | 1.00552 [0.699065, 1.03695] (27/27) | 0.958549 [0.923159, 1.03446] (27/27); range [0.883838, 1.15135] | incomplete |
| 50 | head/pi | 0.897561 [0.819748, 1.21417] (3/3) | 0.942919 [0.9328, 0.953039] (2/3); range [0.92268, 0.963158] | 0.973262 [0.840685, 1.28744] (3/3) | 0.954082 [0.940771, 0.992748] (3/3); range [0.927461, 1.03141] | 0.980993 [0.738132, 1.57631] (24/27) | 0.968803 [0.916028, 1.03074] (24/27); range [0.883838, 1.15135] | incomplete |
| 50 | head/tardie | 1.08743 [0.87073, 1.10813] (3/3) | 0.942919 [0.9328, 0.953039] (2/3); range [0.92268, 0.963158] | 1.08242 [0.865466, 1.09609] (3/3) | 0.954082 [0.940771, 0.992748] (3/3); range [0.927461, 1.03141] | 1.07568 [0.674869, 1.09982] (27/27) | 0.958549 [0.923159, 1.03446] (27/27); range [0.883838, 1.15135] | incomplete |
| 50 | pinned/pi | 0.693548 [0.493116, 1.07754] (3/3) | 0.942919 [0.9328, 0.953039] (2/3); range [0.92268, 0.963158] | 0.691892 [0.52509, 1.11017] (3/3) | 0.954082 [0.940771, 0.992748] (3/3); range [0.927461, 1.03141] | 0.697553 [0.465044, 1.50005] (24/27) | 0.968803 [0.916028, 1.03074] (24/27); range [0.883838, 1.15135] | incomplete |
| 50 | pinned/tardie | 0.611374 [0.489736, 0.824813] (3/3) | 0.942919 [0.9328, 0.953039] (2/3); range [0.92268, 0.963158] | 0.633663 [0.5211, 0.833315] (3/3) | 0.954082 [0.940771, 0.992748] (3/3); range [0.927461, 1.03141] | 0.625 [0.493072, 1.01081] (27/27) | 0.958549 [0.923159, 1.03446] (27/27); range [0.883838, 1.15135] | incomplete |
| 250 | head/base | 1.10811 [1.00894, 1.23292] (3/3) | 1.08438 [0.907053, 1.2617] (2/3); range [0.72973, 1.43902] | 0.992248 [0.96559, 1.00382] (3/3) | 1.02206 [0.831544, 1.21258] (2/3); range [0.641026, 1.4031] | 0.97656 [0.903409, 1.04433] (26/27) | 1.02344 [0.653352, 1.33202] (19/27); range [0.238007, 1.46774] | incomplete |
| 250 | control/base | 1.08438 [0.907053, 1.2617] (2/3) | 1.08438 [0.907053, 1.2617] (2/3); range [0.72973, 1.43902] | 1.02206 [0.831544, 1.21258] (2/3) | 1.02206 [0.831544, 1.21258] (2/3); range [0.641026, 1.4031] | 1.02344 [0.653352, 1.33202] (19/27) | 1.02344 [0.653352, 1.33202] (19/27); range [0.238007, 1.46774] | incomplete |
| 250 | head/control | 1.23101 [1.08726, 1.37476] (2/3) | 1.08438 [0.907053, 1.2617] (2/3); range [0.72973, 1.43902] | 1.14559 [0.926387, 1.3648] (2/3) | 1.02206 [0.831544, 1.21258] (2/3); range [0.641026, 1.4031] | 1.38158 [0.748884, 1.61323] (19/27) | 1.02344 [0.653352, 1.33202] (19/27); range [0.238007, 1.46774] | incomplete |
| 250 | head/pi | 0.907609 [0.75936, 0.993278] (3/3) | 1.08438 [0.907053, 1.2617] (2/3); range [0.72973, 1.43902] | 0.699454 [0.663502, 0.86268] (3/3) | 1.02206 [0.831544, 1.21258] (2/3); range [0.641026, 1.4031] | 0.697804 [0.624365, 1.01556] (26/27) | 1.02344 [0.653352, 1.33202] (19/27); range [0.238007, 1.46774] | incomplete |
| 250 | head/tardie | 1.5209 [1.25635, 1.78545] (2/3) | 0.72973 [0.72973, 0.72973] (1/3); range [0.72973, 0.72973] | 1.025 [0.862227, 1.5125] (3/3) | 1.02206 [0.831544, 1.21258] (2/3); range [0.641026, 1.4031] | 1.0084 [0.720807, 1.56218] (24/27) | 1.02344 [0.661202, 1.32331] (17/27); range [0.427711, 1.46774] | incomplete |
| 250 | pinned/pi | 0.954545 [0.871294, 0.977273] (3/3) | 1.08438 [0.907053, 1.2617] (2/3); range [0.72973, 1.43902] | 0.754098 [0.706131, 0.853733] (3/3) | 1.02206 [0.831544, 1.21258] (2/3); range [0.641026, 1.4031] | 0.70687 [0.650379, 0.968334] (26/27) | 1.04965 [0.664791, 1.33638] (18/27); range [0.238007, 1.46774] | incomplete |
| 250 | pinned/tardie | 1.72459 [1.63689, 1.8123] (2/3) | 0.72973 [0.72973, 0.72973] (1/3); range [0.72973, 0.72973] | 1.075 [0.914549, 1.46679] (3/3) | 1.02206 [0.831544, 1.21258] (2/3); range [0.641026, 1.4031] | 1.07083 [0.803412, 1.57237] (24/27) | 1.04965 [0.671967, 1.34934] (16/27); range [0.427711, 1.46774] | incomplete |
| 1000 | head/base | 0.983557 [0.901261, 1.06585] (2/3) | 0.841741 [0.667784, 1.0157] (2/3); range [0.493827, 1.18966] | 0.74359 [0.496795, 1.10989] (3/3) | 0.553846 [0.451923, 1.11026] (3/3); range [0.35, 1.66667] | 0.658228 [0.2625, 0.853403] (25/27) | 0.545705 [0.368376, 0.823085] (24/27); range [0.178082, 2.01754] | incomplete |
| 1000 | control/base | 0.880597 [0.687212, 1.03513] (3/3) | 0.880597 [0.687212, 1.03513] (3/3); range [0.493827, 1.18966] | 0.553846 [0.451923, 1.11026] (3/3) | 0.553846 [0.451923, 1.11026] (3/3); range [0.35, 1.66667] | 0.545705 [0.351034, 1.03627] (26/27) | 0.545705 [0.351034, 1.03627] (26/27); range [0.178082, 2.01754] | incomplete |
| 1000 | head/control | 1.5067 [1.09755, 1.91585] (2/3) | 0.841741 [0.667784, 1.0157] (2/3); range [0.493827, 1.18966] | 0.885714 [0.8, 1.11415] (3/3) | 0.553846 [0.451923, 1.11026] (3/3); range [0.35, 1.66667] | 0.943939 [0.755495, 1.31642] (24/27) | 0.545705 [0.368376, 0.823085] (24/27); range [0.178082, 2.01754] | incomplete |
| 1000 | head/pi | 0.648759 [0.575702, 0.721815] (2/3) | 0.841741 [0.667784, 1.0157] (2/3); range [0.493827, 1.18966] | 0.492063 [0.333751, 1.04274] (3/3) | 0.553846 [0.451923, 1.11026] (3/3); range [0.35, 1.66667] | 0.475771 [0.23913, 1.07087] (25/27) | 0.545705 [0.368376, 0.823085] (24/27); range [0.178082, 2.01754] | incomplete |
| 1000 | head/tardie | 1.81754 [1.36917, 2.26591] (2/3) | 0.841741 [0.667784, 1.0157] (2/3); range [0.493827, 1.18966] | 1.59341 [0.907814, 2.05346] (3/3) | 0.553846 [0.451923, 1.11026] (3/3); range [0.35, 1.66667] | 1.32979 [0.247191, 2.11765] (25/27) | 0.545705 [0.368376, 0.823085] (24/27); range [0.178082, 2.01754] | incomplete |
| 1000 | pinned/pi | 0.871795 [0.541718, 1.98653] (3/3) | 0.880597 [0.687212, 1.03513] (3/3); range [0.493827, 1.18966] | 0.725275 [0.447293, 0.770532] (3/3) | 0.553846 [0.451923, 1.11026] (3/3); range [0.35, 1.66667] | 0.519685 [0.249496, 0.721663] (27/27) | 0.545705 [0.351034, 1.03627] (26/27); range [0.178082, 2.01754] | incomplete |
| 1000 | pinned/tardie | 1.14286 [1.07638, 1.8609] (3/3) | 0.880597 [0.687212, 1.03513] (3/3); range [0.493827, 1.18966] | 0.864865 [0.79507, 0.949099] (3/3) | 0.553846 [0.451923, 1.11026] (3/3); range [0.35, 1.66667] | 1.02703 [0.584883, 1.28139] (27/27) | 0.545705 [0.351034, 1.03627] (26/27); range [0.178082, 2.01754] | incomplete |

## RPC normalizedClientWallRatio (ms client wall / ms reference client wall)

Object-median eligibility: at least 9 of 9 warm observations. Per-cluster counts below retain the sample ID and mark ineligible medians.

| Size | Role | Deployed build set | Cold median [p25, p75] (clusters/3) | README warm median [p25, p75] (turns/27) | Object-median median [p25, p75] (eligible clusters/3) | Warm n per cluster | Warm-turn p95 | Coverage |
|---:|---|---|---:|---:|---:|---|---:|---|
| 50 | base | base@4.0.1 (deabe970/289122cd); 3/3 deployments matched | 2.2583 [2.12603, 2.50308] (3/3) | 1.27699 [1.18258, 1.74931] (27/27) | 1.26173 [1.21831, 1.53736] (3/3) | 1:9/9; 2:9/9; 3:9/9 | 1.90354 | complete |
| 50 | head | head@4.0.1 (fc50f45a/289122cd); 3/3 deployments matched | 2.36486 [2.16047, 2.56164] (3/3) | 1.91351 [1.3147, 2.14547] (27/27) | 1.92857 [1.59686, 2.0851] (3/3) | 1:9/9; 2:9/9; 3:9/9 | 2.37929 | complete |
| 50 | control | control@4.0.1 (deabe970/289122cd); 3/3 deployments matched | 2.59251 [2.20454, 2.62853] (3/3) | 1.53028 [1.29231, 1.69297] (27/27) | 1.53028 [1.3874, 1.6783] (3/3) | 1:9/9; 2:9/9; 3:9/9 | 1.96218 | complete |
| 50 | pinned | pinned@4.0.0 (c823d174/289122cd); 3/3 deployments matched | 2.18004 [2.09509, 3.09002] (3/3) | 1.49817 [1.27974, 1.83862] (27/27) | 1.46272 [1.27323, 1.71494] (3/3) | 1:9/9; 2:9/9; 3:9/9 | 2.01513 | complete |
| 50 | pi | pi 1.0.4 / no Effect (5526e585/866f1f7f); 3/3 deployments matched | 1.40385 [1.38393, 1.58405] (3/3) | 1.33108 [0.90308, 1.42339] (27/27) | 1.34153 [1.10135, 1.39279] (3/3) | 1:9/9; 2:9/9; 3:9/9 | 1.55432 | complete |
| 50 | tardie | tardie@4.0.0-rc.115 (c8fbbb72/8a0cb11b); 3/3 deployments matched | 5.95902 [5.5118, 6.44711] (3/3) | 4.37814 [4.00275, 6.92475] (27/27) | 4.27531 [3.97158, 5.9635] (3/3) | 1:9/9; 2:9/9; 3:9/9 | 7.90709 | complete |
| 250 | base | base@4.0.1 (deabe970/289122cd); base@4.0.1 (deabe970/6b2bef15); 3/3 deployments matched | 2.72414 [2.71125, 3.08429] (3/3) | 2.44 [1.67604, 2.85276] (27/27) | 2.44 [1.99878, 2.74891] (3/3) | 0:9/9; 1:9/9; 2:9/9 | 3.48131 | complete |
| 250 | head | head@4.0.1 (fc50f45a/289122cd); head@4.0.1 (fc50f45a/6b2bef15); 3/3 deployments matched | 3.85511 [3.34269, 3.9427] (3/3) | 1.94413 [1.64247, 2.25566] (27/27) | 1.89279 [1.83659, 2.0959] (3/3) | 0:9/9; 1:9/9; 2:9/9 | 2.5019 | complete |
| 250 | control | control@4.0.1 (deabe970/289122cd); control@4.0.1 (deabe970/6b2bef15); 3/3 deployments matched | 3.01397 [2.55499, 3.39085] (3/3) | 2.44444 [2.0516, 2.70497] (27/27) | 2.47308 [2.25695, 2.64359] (3/3) | 0:9/9; 1:9/9; 2:9/9 | 2.93891 | complete |
| 250 | pinned | pinned@4.0.0 (c823d174/289122cd); pinned@4.0.0 (c823d174/6b2bef15); 3/3 deployments matched | 3.00746 [2.90025, 3.08287] (3/3) | 1.59552 [1.44348, 1.72718] (27/27) | 1.59552 [1.52774, 1.64738] (3/3) | 0:9/9; 1:9/9; 2:9/9 | 2.03815 | complete |
| 250 | pi | pi 1.0.4 / no Effect (5526e585/866f1f7f); pi 1.0.4 / no Effect (5526e585/f1947b8f); 3/3 deployments matched | 1.94967 [1.75164, 2.66461] (3/3) | 1.50992 [1.45655, 1.66104] (27/27) | 1.46629 [1.4618, 1.56132] (3/3) | 0:9/9; 1:9/9; 2:9/9 | 1.83591 | complete |
| 250 | tardie | tardie@4.0.0-rc.115 (c8fbbb72/8a0cb11b); tardie@4.0.0-rc.115 (c8fbbb72/9f22f5b1); 3/3 deployments matched | 4.57982 [4.36035, 6.56924] (3/3) | 3.32222 [2.80721, 4.8414] (27/27) | 3.2864 [2.86152, 4.19632] (3/3) | 0:9/9; 1:9/9; 2:9/9 | 5.83611 | complete |
| 1000 | base | base@4.0.1 (deabe970/6b2bef15); 3/3 deployments matched | 5.13091 [3.8629, 5.51721] (3/3) | 5.75385 [1.8348, 6.5942] (27/27) | 5.89313 [3.80434, 6.08062] (3/3) | 0:9/9; 1:9/9; 2:9/9 | 8.65608 | complete |
| 1000 | head | head@4.0.1 (fc50f45a/6b2bef15); 3/3 deployments matched | 3.90885 [3.89299, 6.16121] (3/3) | 3.67586 [3.08465, 10.8792] (27/27) | 3.36014 [3.06535, 8.2749] (3/3) | 0:9/9; 1:9/9; 2:9/9 | 15.2079 | complete |
| 1000 | control | control@4.0.1 (deabe970/6b2bef15); 3/3 deployments matched | 5.57937 [5.10562, 6.76874] (3/3) | 5.47159 [2.80546, 6.82383] (27/27) | 5.47159 [4.03072, 6.24457] (3/3) | 0:9/9; 1:9/9; 2:9/9 | 8.09461 | complete |
| 1000 | pinned | pinned@4.0.0 (c823d174/6b2bef15); 3/3 deployments matched | 6.45679 [5.62893, 8.02047] (3/3) | 8.75916 [3.91296, 12.1999] (27/27) | 8.75916 [6.24174, 11.9256] (3/3) | 0:9/9; 1:9/9; 2:9/9 | 18.829 | complete |
| 1000 | pi | pi 1.0.4 / no Effect (5526e585/f1947b8f); 3/3 deployments matched | 3.17423 [3.0668, 3.5427] (3/3) | 3.50638 [3.2788, 5.13462] (27/27) | 3.45161 [3.37715, 5.65316] (3/3) | 0:9/9; 1:9/9; 2:9/9 | 9.22379 | complete |
| 1000 | tardie | tardie@4.0.0-rc.115 (c8fbbb72/9f22f5b1); 3/3 deployments matched | 27.6059 [27.3853, 29.479] (3/3) | 11.03 [7.1768, 15.3395] (27/27) | 10.917 [8.37798, 16.1527] (3/3) | 0:9/9; 1:9/9; 2:9/9 | 30.156 | complete |

| Size | Pair | Cold ratio median [IQR] (pairs/3) | Control/base cold spread | Object-median ratio median [IQR] (pairs/3) | Control/base Object spread | Paired-turn ratio median [IQR] (turn pairs/27; diagnostic) | Control/base turn spread | Status |
|---:|---|---:|---:|---:|---:|---:|---:|---|
| 50 | head/base | 1.18614 [0.948995, 1.2038] (3/3) | 1.17989 [0.920489, 1.24011] (3/3); range [0.661084, 1.30032] | 1.07681 [1.07028, 1.42672] (3/3) | 1.00736 [0.996861, 1.15492] (3/3); range [0.986362, 1.30248] | 1.1163 [1.05139, 1.68129] (27/27) | 1.07193 [0.99903, 1.15342] (27/27); range [0.842273, 1.48811] | complete |
| 50 | control/base | 1.17989 [0.920489, 1.24011] (3/3) | 1.17989 [0.920489, 1.24011] (3/3); range [0.661084, 1.30032] | 1.00736 [0.996861, 1.15492] (3/3) | 1.00736 [0.996861, 1.15492] (3/3); range [0.986362, 1.30248] | 1.07193 [0.99903, 1.15342] (27/27) | 1.07193 [0.99903, 1.15342] (27/27); range [0.842273, 1.48811] | complete |
| 50 | head/control | 1.03522 [0.973708, 1.05601] (3/3) | 1.17989 [0.920489, 1.24011] (3/3); range [0.661084, 1.30032] | 1.05598 [0.94136, 1.42859] (3/3) | 1.00736 [0.996861, 1.15492] (3/3); range [0.986362, 1.30248] | 1.04713 [0.927051, 1.66798] (27/27) | 1.07193 [0.99903, 1.15342] (27/27); range [0.842273, 1.48811] | complete |
| 50 | head/pi | 1.56351 [1.49878, 1.62403] (3/3) | 1.17989 [0.920489, 1.24011] (3/3); range [0.661084, 1.30032] | 1.43759 [1.15685, 2.02031] (3/3) | 1.00736 [0.996861, 1.15492] (3/3); range [0.986362, 1.30248] | 1.38378 [0.92472, 2.43355] (27/27) | 1.07193 [0.99903, 1.15342] (27/27); range [0.842273, 1.48811] | complete |
| 50 | head/tardie | 0.462898 [0.372473, 0.46492] (3/3) | 1.17989 [0.920489, 1.24011] (3/3); range [0.661084, 1.30032] | 0.295917 [0.273981, 0.453538] (3/3) | 1.00736 [0.996861, 1.15492] (3/3); range [0.986362, 1.30248] | 0.291436 [0.263963, 0.533509] (27/27) | 1.07193 [0.99903, 1.15342] (27/27); range [0.842273, 1.48811] | complete |
| 50 | pinned/pi | 1.47369 [1.35468, 2.1615] (3/3) | 1.17989 [0.920489, 1.24011] (3/3); range [0.661084, 1.30032] | 1.36225 [1.08505, 1.53039] (3/3) | 1.00736 [0.996861, 1.15492] (3/3); range [0.986362, 1.30248] | 1.29813 [1.1354, 1.53524] (27/27) | 1.07193 [0.99903, 1.15342] (27/27); range [0.842273, 1.48811] | complete |
| 50 | pinned/tardie | 0.365839 [0.327842, 0.577819] (3/3) | 1.17989 [0.920489, 1.24011] (3/3); range [0.661084, 1.30032] | 0.398794 [0.270214, 0.429459] (3/3) | 1.00736 [0.996861, 1.15492] (3/3); range [0.986362, 1.30248] | 0.385651 [0.200686, 0.443066] (27/27) | 1.07193 [0.99903, 1.15342] (27/27); range [0.842273, 1.48811] | complete |
| 250 | head/base | 1.17009 [1.10452, 1.29938] (3/3) | 0.875023 [0.825897, 1.12906] (3/3); range [0.776772, 1.38309] | 0.942221 [0.78061, 1.04264] (3/3) | 1.15332 [0.981046, 1.23179] (3/3); range [0.80877, 1.31026] | 0.90163 [0.726086, 1.15009] (27/27) | 1.08017 [0.921083, 1.22079] (27/27); range [0.639343, 1.56078] | complete |
| 250 | control/base | 0.875023 [0.825897, 1.12906] (3/3) | 0.875023 [0.825897, 1.12906] (3/3); range [0.776772, 1.38309] | 1.15332 [0.981046, 1.23179] (3/3) | 1.15332 [0.981046, 1.23179] (3/3); range [0.80877, 1.31026] | 1.08017 [0.921083, 1.22079] (27/27) | 1.08017 [0.921083, 1.22079] (27/27); range [0.639343, 1.56078] | complete |
| 250 | head/control | 1.33721 [1.0442, 1.58823] (3/3) | 0.875023 [0.825897, 1.12906] (3/3); range [0.776772, 1.38309] | 0.816964 [0.791161, 0.84468] (3/3) | 1.15332 [0.981046, 1.23179] (3/3); range [0.80877, 1.31026] | 0.809797 [0.731289, 0.906905] (27/27) | 1.08017 [0.921083, 1.22079] (27/27); range [0.639343, 1.56078] | complete |
| 250 | head/pi | 1.82173 [1.48123, 1.94445] (3/3) | 0.875023 [0.825897, 1.12906] (3/3); range [0.776772, 1.38309] | 1.21422 [1.17849, 1.3959] (3/3) | 1.15332 [0.981046, 1.23179] (3/3); range [0.80877, 1.31026] | 1.32555 [1.08157, 1.51636] (27/27) | 1.08017 [0.921083, 1.22079] (27/27); range [0.639343, 1.56078] | complete |
| 250 | head/tardie | 0.683493 [0.577198, 0.762627] (3/3) | 0.875023 [0.825897, 1.12906] (3/3); range [0.776772, 1.38309] | 0.699556 [0.535118, 0.715117] (3/3) | 1.15332 [0.981046, 1.23179] (3/3); range [0.80877, 1.31026] | 0.626921 [0.438566, 0.768888] (27/27) | 1.08017 [0.921083, 1.22079] (27/27); range [0.639343, 1.56078] | complete |
| 250 | pinned/pi | 1.54255 [1.23854, 1.67016] (3/3) | 0.875023 [0.825897, 1.12906] (3/3); range [0.776772, 1.38309] | 0.995686 [0.979484, 1.08085] (3/3) | 1.15332 [0.981046, 1.23179] (3/3); range [0.80877, 1.31026] | 1.02778 [0.901012, 1.12777] (27/27) | 1.08017 [0.921083, 1.22079] (27/27); range [0.639343, 1.56078] | complete |
| 250 | pinned/tardie | 0.674503 [0.512949, 0.682055] (3/3) | 0.875023 [0.825897, 1.12906] (3/3); range [0.776772, 1.38309] | 0.517052 [0.414758, 0.558112] (3/3) | 1.15332 [0.981046, 1.23179] (3/3); range [0.80877, 1.31026] | 0.43305 [0.342092, 0.641985] (27/27) | 1.08017 [0.921083, 1.22079] (27/27); range [0.639343, 1.56078] | complete |
| 1000 | head/base | 1.42518 [1.09041, 1.46577] (3/3) | 1.34803 [1.12539, 1.74908] (3/3); range [0.902741, 2.15014] | 1.95864 [1.20032, 2.09839] (3/3) | 1.1908 [0.80199, 2.19011] (3/3); range [0.413179, 3.18941] | 1.83859 [0.566477, 2.08468] (27/27) | 1.12655 [0.452546, 3.04497] (27/27); range [0.286617, 4.01088] | complete |
| 1000 | control/base | 1.34803 [1.12539, 1.74908] (3/3) | 1.34803 [1.12539, 1.74908] (3/3); range [0.902741, 2.15014] | 1.1908 [0.80199, 2.19011] (3/3) | 1.1908 [0.80199, 2.19011] (3/3); range [0.413179, 3.18941] | 1.12655 [0.452546, 3.04497] (27/27) | 1.12655 [0.452546, 3.04497] (27/27); range [0.286617, 4.01088] | complete |
| 1000 | head/control | 0.837053 [0.768822, 0.947143] (3/3) | 1.34803 [1.12539, 1.74908] (3/3); range [0.902741, 2.15014] | 1.06978 [0.841942, 1.47465] (3/3) | 1.1908 [0.80199, 2.19011] (3/3); range [0.413179, 3.18941] | 1.09746 [0.645138, 1.59546] (27/27) | 1.12655 [0.452546, 3.04497] (27/27); range [0.286617, 4.01088] | complete |
| 1000 | head/pi | 1.22144 [1.11042, 2.03223] (3/3) | 1.34803 [1.12539, 1.74908] (3/3); range [0.902741, 2.15014] | 0.838881 [0.633334, 2.33009] (3/3) | 1.1908 [0.80199, 2.19011] (3/3); range [0.413179, 3.18941] | 0.859252 [0.561766, 2.9911] (27/27) | 1.12655 [0.452546, 3.04497] (27/27); range [0.286617, 4.01088] | complete |
| 1000 | head/tardie | 0.143895 [0.14217, 0.206126] (3/3) | 1.34803 [1.12539, 1.74908] (3/3); range [0.902741, 2.15014] | 0.253785 [0.205443, 1.25634] (3/3) | 1.1908 [0.80199, 2.19011] (3/3); range [0.413179, 3.18941] | 0.288936 [0.179083, 1.27838] (27/27) | 1.12655 [0.452546, 3.04497] (27/27); range [0.286617, 4.01088] | complete |
| 1000 | pinned/pi | 1.65086 [1.63659, 2.33511] (3/3) | 1.34803 [1.12539, 1.74908] (3/3); range [0.902741, 2.15014] | 2.5377 [1.50593, 3.55367] (3/3) | 1.1908 [0.80199, 2.19011] (3/3); range [0.413179, 3.18941] | 2.51163 [0.754004, 3.60789] (27/27) | 1.12655 [0.452546, 3.04497] (27/27); range [0.286617, 4.01088] | complete |
| 1000 | pinned/tardie | 0.237691 [0.195412, 0.292434] (3/3) | 1.34803 [1.12539, 1.74908] (3/3); range [0.902741, 2.15014] | 1.38244 [0.778287, 1.44128] (3/3) | 1.1908 [0.80199, 2.19011] (3/3); range [0.413179, 3.18941] | 1.11153 [0.290541, 1.44987] (27/27) | 1.12655 [0.452546, 3.04497] (27/27); range [0.286617, 4.01088] | complete |

## RPC normalizedDoWallRatio (ms DO wall / ms reference DO wall)

Object-median eligibility: at least 7 of 9 warm observations. Per-cluster counts below retain the sample ID and mark ineligible medians.

| Size | Role | Deployed build set | Cold median [p25, p75] (clusters/3) | README warm median [p25, p75] (turns/27) | Object-median median [p25, p75] (eligible clusters/3) | Warm n per cluster | Warm-turn p95 | Coverage |
|---:|---|---|---:|---:|---:|---|---:|---|
| 50 | base | base@4.0.1 (deabe970/289122cd); 3/3 deployments matched | 3.48563 [3.22845, 3.74282] (2/3) | 1.38859 [1.27236, 2.57867] (27/27) | 1.37757 [1.30564, 2.03661] (3/3) | 1:9/9; 2:9/9; 3:9/9 | 2.90994 | incomplete |
| 50 | head | head@4.0.1 (fc50f45a/289122cd); 3/3 deployments matched | 3.16309 [2.84585, 3.53892] (3/3) | 2.7868 [1.49446, 3.30453] (27/27) | 2.7868 [2.08234, 3.08002] (3/3) | 1:9/9; 2:9/9; 3:9/9 | 3.67193 | complete |
| 50 | control | control@4.0.1 (deabe970/289122cd); 3/3 deployments matched | 3.67347 [3.04128, 3.69523] (3/3) | 1.74766 [1.43111, 2.38684] (27/27) | 1.74766 [1.57211, 2.16891] (3/3) | 1:9/9; 2:9/9; 3:9/9 | 2.82503 | complete |
| 50 | pinned | pinned@4.0.0 (c823d174/289122cd); 3/3 deployments matched | 2.85185 [2.57894, 4.11431] (3/3) | 1.64532 [1.34542, 2.57143] (25/27) | 1.61306 [1.39208, 2.22616] (3/3) | 1:8/9; 2:8/9; 3:9/9 | 2.94983 | incomplete |
| 50 | pi | pi 1.0.4 / no Effect (5526e585/866f1f7f); 3/3 deployments matched | 1.73162 [1.66118, 1.95045] (3/3) | 1.59518 [0.835234, 1.80179] (24/27) | 1.63483 [1.20605, 1.71005] (3/3) | 1:9/9; 2:8/9; 3:7/9 | 1.9963 | incomplete |
| 50 | tardie | tardie@4.0.0-rc.115 (c8fbbb72/8a0cb11b); 3/3 deployments matched | 8.90523 [8.50371, 9.38609] (3/3) | 5.85166 [5.41558, 13.0685] (25/27) | 5.82948 [5.49503, 9.47435] (3/3) | 1:8/9; 2:9/9; 3:8/9 | 14.1421 | incomplete |
| 250 | base | base@4.0.1 (deabe970/289122cd); base@4.0.1 (deabe970/6b2bef15); 3/3 deployments matched | 3.65976 [3.60539, 4.07395] (3/3) | 4.0243 [1.93711, 4.35839] (26/27) | 4.15842 [2.9883, 4.5753] (3/3) | 0:9/9; 1:8/9; 2:9/9 | 5.47909 | incomplete |
| 250 | head | head@4.0.1 (fc50f45a/289122cd); head@4.0.1 (fc50f45a/6b2bef15); 3/3 deployments matched | 5.05556 [4.47332, 5.2096] (3/3) | 2.19811 [1.79144, 2.76207] (23/27) | 1.98408 [1.97135, 1.9968] (2/3) | 0:9/9; 1:8/9; 2:6/9 ineligible | 3.63959 | incomplete |
| 250 | control | control@4.0.1 (deabe970/289122cd); control@4.0.1 (deabe970/6b2bef15); 3/3 deployments matched | 3.88548 [3.37874, 4.39222] (2/3) | 4.09045 [3.22057, 4.4553] (16/27) | 4.34783 [4.34783, 4.34783] (1/3) | 0:6/9 ineligible; 1:1/9 ineligible; 2:9/9 | 4.88826 | incomplete |
| 250 | pinned | pinned@4.0.0 (c823d174/289122cd); pinned@4.0.0 (c823d174/6b2bef15); 3/3 deployments matched | 3.71866 [3.62923, 3.92271] (3/3) | 1.80728 [1.58393, 2.01566] (26/27) | 1.91549 [1.75775, 1.91776] (3/3) | 0:9/9; 1:9/9; 2:8/9 | 2.44879 | incomplete |
| 250 | pi | pi 1.0.4 / no Effect (5526e585/866f1f7f); pi 1.0.4 / no Effect (5526e585/f1947b8f); 3/3 deployments matched | 2.75 [2.32391, 3.89044] (3/3) | 2.00562 [1.902, 2.27285] (27/27) | 1.97222 [1.93782, 2.21083] (3/3) | 0:9/9; 1:9/9; 2:9/9 | 2.86994 | complete |
| 250 | tardie | tardie@4.0.0-rc.115 (c8fbbb72/8a0cb11b); tardie@4.0.0-rc.115 (c8fbbb72/9f22f5b1); 3/3 deployments matched | 8.89275 [7.10607, 10.6794] (2/3) | 4.19348 [3.7451, 6.25929] (24/27) | 3.88245 [3.28055, 5.18774] (3/3) | 0:7/9; 1:9/9; 2:8/9 | 7.50531 | incomplete |
| 1000 | base | base@4.0.1 (deabe970/6b2bef15); 3/3 deployments matched | 8.60544 [6.37925, 8.64836] (3/3) | 10.5862 [2.29372, 13.0337] (27/27) | 11.7778 [6.928, 12.485] (3/3) | 0:9/9; 1:9/9; 2:9/9 | 15.3572 | complete |
| 1000 | head | head@4.0.1 (fc50f45a/6b2bef15); 3/3 deployments matched | 8.35946 [7.33428, 9.38465] (2/3) | 5.06444 [4.37479, 15.6711] (24/27) | 5.08485 [4.17677, 12.7242] (3/3) | 0:9/9; 1:8/9; 2:7/9 | 25.0953 | incomplete |
| 1000 | control | control@4.0.1 (deabe970/6b2bef15); 3/3 deployments matched | 8.08553 [7.04688, 8.58264] (3/3) | 9.36145 [3.18277, 11.6032] (25/27) | 9.36145 [6.18196, 10.8175] (3/3) | 0:8/9; 1:8/9; 2:9/9 | 13.5567 | incomplete |
| 1000 | pinned | pinned@4.0.0 (c823d174/6b2bef15); 3/3 deployments matched | 9.56995 [8.21469, 10.0811] (3/3) | 14.3523 [4.81172, 24.0607] (27/27) | 14.3523 [9.45068, 20.4208] (3/3) | 0:9/9; 1:9/9; 2:9/9 | 31.8265 | complete |
| 1000 | pi | pi 1.0.4 / no Effect (5526e585/f1947b8f); 3/3 deployments matched | 4.2623 [4.21657, 5.78615] (3/3) | 7.34211 [5.94616, 16.299] (27/27) | 7.25316 [6.41446, 13.3287] (3/3) | 0:9/9; 1:9/9; 2:9/9 | 23.2204 | complete |
| 1000 | tardie | tardie@4.0.0-rc.115 (c8fbbb72/9f22f5b1); 3/3 deployments matched | 38.0424 [35.1948, 41.593] (3/3) | 15.1914 [8.82511, 24.4209] (26/27) | 12.1347 [9.66258, 25.8531] (3/3) | 0:9/9; 1:9/9; 2:8/9 | 53.308 | incomplete |

| Size | Pair | Cold ratio median [IQR] (pairs/3) | Control/base cold spread | Object-median ratio median [IQR] (pairs/3) | Control/base Object spread | Paired-turn ratio median [IQR] (turn pairs/27; diagnostic) | Control/base turn spread | Status |
|---:|---|---:|---:|---:|---:|---:|---:|---|
| 50 | head/base | 0.974847 [0.803501, 1.14619] (2/3) | 0.919302 [0.760788, 1.07782] (2/3); range [0.602273, 1.23633] | 1.11685 [1.07533, 1.78277] (3/3) | 1.01378 [0.987322, 1.21519] (3/3); range [0.960867, 1.41659] | 1.1732 [1.03909, 2.35181] (27/27) | 1.09539 [0.970629, 1.18599] (27/27); range [0.785489, 1.65205] | incomplete |
| 50 | control/base | 0.919302 [0.760788, 1.07782] (2/3) | 0.919302 [0.760788, 1.07782] (2/3); range [0.602273, 1.23633] | 1.01378 [0.987322, 1.21519] (3/3) | 1.01378 [0.987322, 1.21519] (3/3); range [0.960867, 1.41659] | 1.09539 [0.970629, 1.18599] (27/27) | 1.09539 [0.970629, 1.18599] (27/27); range [0.785489, 1.65205] | incomplete |
| 50 | head/control | 1.04962 [0.9503, 1.05765] (3/3) | 0.919302 [0.760788, 1.07782] (2/3); range [0.602273, 1.23633] | 1.07592 [0.932162, 1.74566] (3/3) | 1.01378 [0.987322, 1.21519] (3/3); range [0.960867, 1.41659] | 1.09582 [0.907345, 2.2362] (27/27) | 1.09539 [0.970629, 1.18599] (27/27); range [0.785489, 1.65205] | incomplete |
| 50 | head/pi | 1.80462 [1.6971, 1.81565] (3/3) | 0.919302 [0.760788, 1.07782] (2/3); range [0.602273, 1.23633] | 1.70464 [1.23822, 3.02225] (3/3) | 1.01378 [0.987322, 1.21519] (3/3); range [0.960867, 1.41659] | 1.5783 [0.788985, 4.25247] (24/27) | 1.13783 [0.959083, 1.20721] (24/27); range [0.785489, 1.65205] | incomplete |
| 50 | head/tardie | 0.390399 [0.323335, 0.415001] (3/3) | 0.919302 [0.760788, 1.07782] (2/3); range [0.602273, 1.23633] | 0.236363 [0.224392, 0.445009] (3/3) | 1.01378 [0.987322, 1.21519] (3/3); range [0.960867, 1.41659] | 0.241243 [0.204649, 0.636731] (25/27) | 1.09539 [0.9704, 1.19628] (25/27); range [0.785489, 1.65205] | incomplete |
| 50 | pinned/pi | 1.44965 [1.38215, 2.27735] (3/3) | 0.919302 [0.760788, 1.07782] (2/3); range [0.602273, 1.23633] | 1.59037 [1.15336, 1.83283] (3/3) | 1.01378 [0.987322, 1.21519] (3/3); range [0.960867, 1.41659] | 1.58263 [0.958632, 1.82831] (22/27) | 1.10737 [0.93645, 1.17567] (22/27); range [0.785489, 1.65205] | incomplete |
| 50 | pinned/tardie | 0.320245 [0.276979, 0.491931] (3/3) | 0.919302 [0.760788, 1.07782] (2/3); range [0.602273, 1.23633] | 0.312574 [0.20092, 0.399812] (3/3) | 1.01378 [0.987322, 1.21519] (3/3); range [0.960867, 1.41659] | 0.310029 [0.123039, 0.410855] (23/27) | 1.08028 [0.947767, 1.17566] (23/27); range [0.785489, 1.65205] | incomplete |
| 250 | head/base | 1.19507 [1.12914, 1.30938] (3/3) | 1.07369 [0.941236, 1.20614] (2/3); range [0.808782, 1.3386] | 0.748789 [0.570564, 0.927013] (2/3) | unavailable [unavailable, unavailable] (0/3); range [unavailable, unavailable] | 0.873934 [0.485118, 1.11653] (22/27) | 1.19336 [1.06241, 1.60407] (13/27); range [0.835398, 2.06268] | incomplete |
| 250 | control/base | 1.07369 [0.941236, 1.20614] (2/3) | 1.07369 [0.941236, 1.20614] (2/3); range [0.808782, 1.3386] | 1.04555 [1.04555, 1.04555] (1/3) | 1.04555 [1.04555, 1.04555] (1/3); range [1.04555, 1.04555] | 1.18092 [1.04943, 1.57567] (16/27) | 1.18092 [1.04943, 1.57567] (16/27); range [0.835398, 2.06268] | incomplete |
| 250 | head/control | 1.27728 [1.03577, 1.51879] (2/3) | 1.07369 [0.941236, 1.20614] (2/3); range [0.808782, 1.3386] | unavailable [unavailable, unavailable] (0/3) | unavailable [unavailable, unavailable] (0/3); range [unavailable, unavailable] | 0.68709 [0.63396, 0.78373] (13/27) | 1.19336 [1.06241, 1.60407] (13/27); range [0.835398, 2.06268] | incomplete |
| 250 | head/pi | 1.95041 [1.47766, 2.00036] (3/3) | 1.07369 [0.941236, 1.20614] (2/3); range [0.808782, 1.3386] | 0.927686 [0.863655, 0.991718] (2/3) | unavailable [unavailable, unavailable] (0/3); range [unavailable, unavailable] | 1.01815 [0.877244, 1.41651] (23/27) | 1.19336 [1.06241, 1.60407] (13/27); range [0.835398, 2.06268] | incomplete |
| 250 | head/tardie | 0.580875 [0.505566, 0.656183] (2/3) | 1.3386 [1.3386, 1.3386] (1/3); range [1.3386, 1.3386] | 0.525926 [0.413788, 0.638063] (2/3) | unavailable [unavailable, unavailable] (0/3); range [unavailable, unavailable] | 0.628355 [0.308739, 0.901308] (21/27) | 1.19303 [1.02723, 1.58715] (12/27); range [0.835398, 2.06268] | incomplete |
| 250 | pinned/pi | 1.2872 [1.05374, 1.62332] (3/3) | 1.07369 [0.941236, 1.20614] (2/3); range [0.808782, 1.3386] | 0.840597 [0.811305, 0.907064] (3/3) | 1.04555 [1.04555, 1.04555] (1/3); range [1.04555, 1.04555] | 0.849977 [0.71997, 0.97392] (26/27) | 1.16913 [1.03645, 1.58513] (15/27); range [0.835398, 2.06268] | incomplete |
| 250 | pinned/tardie | 0.491515 [0.387735, 0.595296] (2/3) | 1.3386 [1.3386, 1.3386] (1/3); range [1.3386, 1.3386] | 0.494538 [0.394772, 0.545927] (3/3) | 1.04555 [1.04555, 1.04555] (1/3); range [1.04555, 1.04555] | 0.368897 [0.307686, 0.594858] (23/27) | 1.11814 [1.01048, 1.5662] (13/27); range [0.835398, 2.06268] | incomplete |
| 1000 | head/base | 0.967796 [0.846853, 1.08874] (2/3) | 0.873206 [0.78225, 0.964162] (2/3); range [0.691294, 1.05512] | 1.5436 [0.910565, 1.99517] (3/3) | 0.709614 [0.48227, 3.3077] (3/3); range [0.254927, 5.90579] | 1.79283 [0.386905, 2.33334] (24/27) | 0.749356 [0.29846, 5.92652] (22/27); range [0.177178, 7.54351] | incomplete |
| 1000 | control/base | 1.05512 [0.873206, 1.501] (3/3) | 1.05512 [0.873206, 1.501] (3/3); range [0.691294, 1.94688] | 0.709614 [0.48227, 3.3077] (3/3) | 0.709614 [0.48227, 3.3077] (3/3); range [0.254927, 5.90579] | 0.6741 [0.302191, 5.85858] (25/27) | 0.6741 [0.302191, 5.85858] (25/27); range [0.177178, 7.54351] | incomplete |
| 1000 | head/control | 1.09828 [1.07418, 1.12239] (2/3) | 0.873206 [0.78225, 0.964162] (2/3); range [0.691294, 1.05512] | 1.08867 [0.751482, 1.63197] (3/3) | 0.709614 [0.48227, 3.3077] (3/3); range [0.254927, 5.90579] | 1.1073 [0.432071, 1.72481] (22/27) | 0.749356 [0.29846, 5.92652] (22/27); range [0.177178, 7.54351] | incomplete |
| 1000 | head/pi | 1.98803 [1.73412, 2.24194] (2/3) | 0.873206 [0.78225, 0.964162] (2/3); range [0.691294, 1.05512] | 0.586233 [0.424141, 1.69689] (3/3) | 0.709614 [0.48227, 3.3077] (3/3); range [0.254927, 5.90579] | 0.552812 [0.281343, 2.27083] (24/27) | 0.749356 [0.29846, 5.92652] (22/27); range [0.177178, 7.54351] | incomplete |
| 1000 | head/tardie | 0.212818 [0.203931, 0.221706] (2/3) | 0.873206 [0.78225, 0.964162] (2/3); range [0.691294, 1.05512] | 0.269368 [0.198933, 1.5507] (3/3) | 0.709614 [0.48227, 3.3077] (3/3); range [0.254927, 5.90579] | 0.25923 [0.171805, 0.59231] (23/27) | 0.780242 [0.297216, 5.94917] (21/27); range [0.177178, 7.54351] | incomplete |
| 1000 | pinned/pi | 1.64461 [1.47689, 2.06485] (3/3) | 1.05512 [0.873206, 1.501] (3/3); range [0.691294, 1.94688] | 1.97876 [1.1066, 3.36478] (3/3) | 0.709614 [0.48227, 3.3077] (3/3); range [0.254927, 5.90579] | 2.03868 [0.278663, 3.86192] (27/27) | 0.6741 [0.302191, 5.85858] (25/27); range [0.177178, 7.54351] | incomplete |
| 1000 | pinned/tardie | 0.25156 [0.201754, 0.289506] (3/3) | 1.05512 [0.873206, 1.501] (3/3); range [0.691294, 1.94688] | 1.99601 [1.05549, 2.08948] (3/3) | 0.709614 [0.48227, 3.3077] (3/3); range [0.254927, 5.90579] | 1.5847 [0.176025, 2.40229] (26/27) | 0.673858 [0.300947, 5.88123] (24/27); range [0.177178, 7.54351] | incomplete |

## RPC receipt.insideWallMs (ms)

Object-median eligibility: at least 9 of 9 warm observations. Per-cluster counts below retain the sample ID and mark ineligible medians.

| Size | Role | Deployed build set | Cold median [p25, p75] (clusters/3) | README warm median [p25, p75] (turns/27) | Object-median median [p25, p75] (eligible clusters/3) | Warm n per cluster | Warm-turn p95 | Coverage |
|---:|---|---|---:|---:|---:|---|---:|---|
| 50 | base | base@4.0.1 (deabe970/289122cd); 3/3 deployments matched | 0 [0, 0] (3/3) | 0 [0, 0] (27/27) | 0 [0, 0] (3/3) | 1:9/9; 2:9/9; 3:9/9 | 0 | complete |
| 50 | head | head@4.0.1 (fc50f45a/289122cd); 3/3 deployments matched | 0 [0, 0] (3/3) | 0 [0, 0] (27/27) | 0 [0, 0] (3/3) | 1:9/9; 2:9/9; 3:9/9 | 0 | complete |
| 50 | control | control@4.0.1 (deabe970/289122cd); 3/3 deployments matched | 0 [0, 0] (3/3) | 0 [0, 0] (27/27) | 0 [0, 0] (3/3) | 1:9/9; 2:9/9; 3:9/9 | 0 | complete |
| 50 | pinned | pinned@4.0.0 (c823d174/289122cd); 3/3 deployments matched | 0 [0, 0] (3/3) | 0 [0, 0] (27/27) | 0 [0, 0] (3/3) | 1:9/9; 2:9/9; 3:9/9 | 0 | complete |
| 50 | pi | pi 1.0.4 / no Effect (5526e585/866f1f7f); 3/3 deployments matched | 0 [0, 0] (3/3) | 0 [0, 0] (27/27) | 0 [0, 0] (3/3) | 1:9/9; 2:9/9; 3:9/9 | 0 | complete |
| 50 | tardie | tardie@4.0.0-rc.115 (c8fbbb72/8a0cb11b); 3/3 deployments matched | 1829 [1809.5, 2004] (3/3) | 1871 [1754.5, 2062] (27/27) | 1814 [1795.5, 1996] (3/3) | 1:9/9; 2:9/9; 3:9/9 | 2541 | complete |
| 250 | base | base@4.0.1 (deabe970/289122cd); base@4.0.1 (deabe970/6b2bef15); 3/3 deployments matched | 0 [0, 0] (3/3) | 0 [0, 0] (27/27) | 0 [0, 0] (3/3) | 0:9/9; 1:9/9; 2:9/9 | 0 | complete |
| 250 | head | head@4.0.1 (fc50f45a/289122cd); head@4.0.1 (fc50f45a/6b2bef15); 3/3 deployments matched | 0 [0, 0] (3/3) | 0 [0, 0] (27/27) | 0 [0, 0] (3/3) | 0:9/9; 1:9/9; 2:9/9 | 0 | complete |
| 250 | control | control@4.0.1 (deabe970/289122cd); control@4.0.1 (deabe970/6b2bef15); 3/3 deployments matched | 0 [0, 0] (3/3) | 0 [0, 0] (27/27) | 0 [0, 0] (3/3) | 0:9/9; 1:9/9; 2:9/9 | 0 | complete |
| 250 | pinned | pinned@4.0.0 (c823d174/289122cd); pinned@4.0.0 (c823d174/6b2bef15); 3/3 deployments matched | 0 [0, 0] (3/3) | 0 [0, 0] (27/27) | 0 [0, 0] (3/3) | 0:9/9; 1:9/9; 2:9/9 | 0 | complete |
| 250 | pi | pi 1.0.4 / no Effect (5526e585/866f1f7f); pi 1.0.4 / no Effect (5526e585/f1947b8f); 3/3 deployments matched | 0 [0, 0] (3/3) | 0 [0, 0] (27/27) | 0 [0, 0] (3/3) | 0:9/9; 1:9/9; 2:9/9 | 0 | complete |
| 250 | tardie | tardie@4.0.0-rc.115 (c8fbbb72/8a0cb11b); tardie@4.0.0-rc.115 (c8fbbb72/9f22f5b1); 3/3 deployments matched | 1125 [966, 1811.5] (3/3) | 1045 [863, 2149] (27/27) | 1044 [962.5, 1660] (3/3) | 0:9/9; 1:9/9; 2:9/9 | 2557.4 | complete |
| 1000 | base | base@4.0.1 (deabe970/6b2bef15); 3/3 deployments matched | 0 [0, 0] (3/3) | 0 [0, 0] (27/27) | 0 [0, 0] (3/3) | 0:9/9; 1:9/9; 2:9/9 | 0 | complete |
| 1000 | head | head@4.0.1 (fc50f45a/6b2bef15); 3/3 deployments matched | 0 [0, 0] (3/3) | 0 [0, 0] (27/27) | 0 [0, 0] (3/3) | 0:9/9; 1:9/9; 2:9/9 | 0 | complete |
| 1000 | control | control@4.0.1 (deabe970/6b2bef15); 3/3 deployments matched | 0 [0, 0] (3/3) | 0 [0, 0] (27/27) | 0 [0, 0] (3/3) | 0:9/9; 1:9/9; 2:9/9 | 0 | complete |
| 1000 | pinned | pinned@4.0.0 (c823d174/6b2bef15); 3/3 deployments matched | 0 [0, 0] (3/3) | 0 [0, 0] (27/27) | 0 [0, 0] (3/3) | 0:9/9; 1:9/9; 2:9/9 | 0 | complete |
| 1000 | pi | pi 1.0.4 / no Effect (5526e585/f1947b8f); 3/3 deployments matched | 0 [0, 0] (3/3) | 0 [0, 0] (27/27) | 0 [0, 0] (3/3) | 0:9/9; 1:9/9; 2:9/9 | 0 | complete |
| 1000 | tardie | tardie@4.0.0-rc.115 (c8fbbb72/9f22f5b1); 3/3 deployments matched | 8055 [6161.5, 8257] (3/3) | 3011 [2208, 4054.5] (27/27) | 3171 [2682.5, 3455] (3/3) | 0:9/9; 1:9/9; 2:9/9 | 5798.8 | complete |

| Size | Pair | Cold ratio median [IQR] (pairs/3) | Control/base cold spread | Object-median ratio median [IQR] (pairs/3) | Control/base Object spread | Paired-turn ratio median [IQR] (turn pairs/27; diagnostic) | Control/base turn spread | Status |
|---:|---|---:|---:|---:|---:|---:|---:|---|
| 50 | head/base | unavailable [unavailable, unavailable] (0/3) | unavailable [unavailable, unavailable] (0/3); range [unavailable, unavailable] | unavailable [unavailable, unavailable] (0/3) | unavailable [unavailable, unavailable] (0/3); range [unavailable, unavailable] | unavailable [unavailable, unavailable] (0/27) | unavailable [unavailable, unavailable] (0/27); range [unavailable, unavailable] | incomplete |
| 50 | control/base | unavailable [unavailable, unavailable] (0/3) | unavailable [unavailable, unavailable] (0/3); range [unavailable, unavailable] | unavailable [unavailable, unavailable] (0/3) | unavailable [unavailable, unavailable] (0/3); range [unavailable, unavailable] | unavailable [unavailable, unavailable] (0/27) | unavailable [unavailable, unavailable] (0/27); range [unavailable, unavailable] | incomplete |
| 50 | head/control | unavailable [unavailable, unavailable] (0/3) | unavailable [unavailable, unavailable] (0/3); range [unavailable, unavailable] | unavailable [unavailable, unavailable] (0/3) | unavailable [unavailable, unavailable] (0/3); range [unavailable, unavailable] | unavailable [unavailable, unavailable] (0/27) | unavailable [unavailable, unavailable] (0/27); range [unavailable, unavailable] | incomplete |
| 50 | head/pi | unavailable [unavailable, unavailable] (0/3) | unavailable [unavailable, unavailable] (0/3); range [unavailable, unavailable] | unavailable [unavailable, unavailable] (0/3) | unavailable [unavailable, unavailable] (0/3); range [unavailable, unavailable] | unavailable [unavailable, unavailable] (0/27) | unavailable [unavailable, unavailable] (0/27); range [unavailable, unavailable] | incomplete |
| 50 | head/tardie | 0 [0, 0] (3/3) | unavailable [unavailable, unavailable] (0/3); range [unavailable, unavailable] | 0 [0, 0] (3/3) | unavailable [unavailable, unavailable] (0/3); range [unavailable, unavailable] | 0 [0, 0] (27/27) | unavailable [unavailable, unavailable] (0/27); range [unavailable, unavailable] | incomplete |
| 50 | pinned/pi | unavailable [unavailable, unavailable] (0/3) | unavailable [unavailable, unavailable] (0/3); range [unavailable, unavailable] | unavailable [unavailable, unavailable] (0/3) | unavailable [unavailable, unavailable] (0/3); range [unavailable, unavailable] | unavailable [unavailable, unavailable] (0/27) | unavailable [unavailable, unavailable] (0/27); range [unavailable, unavailable] | incomplete |
| 50 | pinned/tardie | 0 [0, 0] (3/3) | unavailable [unavailable, unavailable] (0/3); range [unavailable, unavailable] | 0 [0, 0] (3/3) | unavailable [unavailable, unavailable] (0/3); range [unavailable, unavailable] | 0 [0, 0] (27/27) | unavailable [unavailable, unavailable] (0/27); range [unavailable, unavailable] | incomplete |
| 250 | head/base | unavailable [unavailable, unavailable] (0/3) | unavailable [unavailable, unavailable] (0/3); range [unavailable, unavailable] | unavailable [unavailable, unavailable] (0/3) | unavailable [unavailable, unavailable] (0/3); range [unavailable, unavailable] | unavailable [unavailable, unavailable] (0/27) | unavailable [unavailable, unavailable] (0/27); range [unavailable, unavailable] | incomplete |
| 250 | control/base | unavailable [unavailable, unavailable] (0/3) | unavailable [unavailable, unavailable] (0/3); range [unavailable, unavailable] | unavailable [unavailable, unavailable] (0/3) | unavailable [unavailable, unavailable] (0/3); range [unavailable, unavailable] | unavailable [unavailable, unavailable] (0/27) | unavailable [unavailable, unavailable] (0/27); range [unavailable, unavailable] | incomplete |
| 250 | head/control | unavailable [unavailable, unavailable] (0/3) | unavailable [unavailable, unavailable] (0/3); range [unavailable, unavailable] | unavailable [unavailable, unavailable] (0/3) | unavailable [unavailable, unavailable] (0/3); range [unavailable, unavailable] | unavailable [unavailable, unavailable] (0/27) | unavailable [unavailable, unavailable] (0/27); range [unavailable, unavailable] | incomplete |
| 250 | head/pi | unavailable [unavailable, unavailable] (0/3) | unavailable [unavailable, unavailable] (0/3); range [unavailable, unavailable] | unavailable [unavailable, unavailable] (0/3) | unavailable [unavailable, unavailable] (0/3); range [unavailable, unavailable] | unavailable [unavailable, unavailable] (0/27) | unavailable [unavailable, unavailable] (0/27); range [unavailable, unavailable] | incomplete |
| 250 | head/tardie | 0 [0, 0] (3/3) | unavailable [unavailable, unavailable] (0/3); range [unavailable, unavailable] | 0 [0, 0] (3/3) | unavailable [unavailable, unavailable] (0/3); range [unavailable, unavailable] | 0 [0, 0] (27/27) | unavailable [unavailable, unavailable] (0/27); range [unavailable, unavailable] | incomplete |
| 250 | pinned/pi | unavailable [unavailable, unavailable] (0/3) | unavailable [unavailable, unavailable] (0/3); range [unavailable, unavailable] | unavailable [unavailable, unavailable] (0/3) | unavailable [unavailable, unavailable] (0/3); range [unavailable, unavailable] | unavailable [unavailable, unavailable] (0/27) | unavailable [unavailable, unavailable] (0/27); range [unavailable, unavailable] | incomplete |
| 250 | pinned/tardie | 0 [0, 0] (3/3) | unavailable [unavailable, unavailable] (0/3); range [unavailable, unavailable] | 0 [0, 0] (3/3) | unavailable [unavailable, unavailable] (0/3); range [unavailable, unavailable] | 0 [0, 0] (27/27) | unavailable [unavailable, unavailable] (0/27); range [unavailable, unavailable] | incomplete |
| 1000 | head/base | unavailable [unavailable, unavailable] (0/3) | unavailable [unavailable, unavailable] (0/3); range [unavailable, unavailable] | unavailable [unavailable, unavailable] (0/3) | unavailable [unavailable, unavailable] (0/3); range [unavailable, unavailable] | unavailable [unavailable, unavailable] (0/27) | unavailable [unavailable, unavailable] (0/27); range [unavailable, unavailable] | incomplete |
| 1000 | control/base | unavailable [unavailable, unavailable] (0/3) | unavailable [unavailable, unavailable] (0/3); range [unavailable, unavailable] | unavailable [unavailable, unavailable] (0/3) | unavailable [unavailable, unavailable] (0/3); range [unavailable, unavailable] | unavailable [unavailable, unavailable] (0/27) | unavailable [unavailable, unavailable] (0/27); range [unavailable, unavailable] | incomplete |
| 1000 | head/control | unavailable [unavailable, unavailable] (0/3) | unavailable [unavailable, unavailable] (0/3); range [unavailable, unavailable] | unavailable [unavailable, unavailable] (0/3) | unavailable [unavailable, unavailable] (0/3); range [unavailable, unavailable] | unavailable [unavailable, unavailable] (0/27) | unavailable [unavailable, unavailable] (0/27); range [unavailable, unavailable] | incomplete |
| 1000 | head/pi | unavailable [unavailable, unavailable] (0/3) | unavailable [unavailable, unavailable] (0/3); range [unavailable, unavailable] | unavailable [unavailable, unavailable] (0/3) | unavailable [unavailable, unavailable] (0/3); range [unavailable, unavailable] | unavailable [unavailable, unavailable] (0/27) | unavailable [unavailable, unavailable] (0/27); range [unavailable, unavailable] | incomplete |
| 1000 | head/tardie | 0 [0, 0] (3/3) | unavailable [unavailable, unavailable] (0/3); range [unavailable, unavailable] | 0 [0, 0] (3/3) | unavailable [unavailable, unavailable] (0/3); range [unavailable, unavailable] | 0 [0, 0] (27/27) | unavailable [unavailable, unavailable] (0/27); range [unavailable, unavailable] | incomplete |
| 1000 | pinned/pi | unavailable [unavailable, unavailable] (0/3) | unavailable [unavailable, unavailable] (0/3); range [unavailable, unavailable] | unavailable [unavailable, unavailable] (0/3) | unavailable [unavailable, unavailable] (0/3); range [unavailable, unavailable] | unavailable [unavailable, unavailable] (0/27) | unavailable [unavailable, unavailable] (0/27); range [unavailable, unavailable] | incomplete |
| 1000 | pinned/tardie | 0 [0, 0] (3/3) | unavailable [unavailable, unavailable] (0/3); range [unavailable, unavailable] | 0 [0, 0] (3/3) | unavailable [unavailable, unavailable] (0/3); range [unavailable, unavailable] | 0 [0, 0] (27/27) | unavailable [unavailable, unavailable] (0/27); range [unavailable, unavailable] | incomplete |

## RPC doTotalCpuTimeMs (ms)

Object-median eligibility: at least 7 of 9 warm observations. Per-cluster counts below retain the sample ID and mark ineligible medians.

| Size | Role | Deployed build set | Cold median [p25, p75] (clusters/3) | README warm median [p25, p75] (turns/27) | Object-median median [p25, p75] (eligible clusters/3) | Warm n per cluster | Warm-turn p95 | Coverage |
|---:|---|---|---:|---:|---:|---|---:|---|
| 50 | base | base@4.0.1 (deabe970/289122cd); 3/3 deployments matched | 587.5 [533.25, 641.75] (2/3) | 199 [170, 288.5] (27/27) | 177 [173, 259.5] (3/3) | 1:9/9; 2:9/9; 3:9/9 | 384.6 | incomplete |
| 50 | head | head@4.0.1 (fc50f45a/289122cd); 3/3 deployments matched | 376 [369.5, 598] (3/3) | 211 [184.5, 288] (27/27) | 194 [189.5, 259.5] (3/3) | 1:9/9; 2:9/9; 3:9/9 | 398.5 | complete |
| 50 | control | control@4.0.1 (deabe970/289122cd); 3/3 deployments matched | 582 [480.5, 639.5] (3/3) | 180 [157, 310.5] (27/27) | 170 [157.5, 277.5] (3/3) | 1:9/9; 2:9/9; 3:9/9 | 404 | complete |
| 50 | pinned | pinned@4.0.0 (c823d174/289122cd); 3/3 deployments matched | 614 [466.5, 778.5] (3/3) | 302 [232, 331] (27/27) | 321 [241.5, 323] (3/3) | 1:9/9; 2:9/9; 3:9/9 | 429.9 | complete |
| 50 | pi | pi 1.0.4 / no Effect (5526e585/866f1f7f); 3/3 deployments matched | 131 [130, 145] (3/3) | 99.5 [93.75, 114] (24/27) | 102 [99, 102.5] (3/3) | 1:9/9; 2:8/9; 3:7/9 | 154.75 | incomplete |
| 50 | tardie | tardie@4.0.0-rc.115 (c8fbbb72/8a0cb11b); 3/3 deployments matched | 205 [183.5, 272] (3/3) | 9 [7, 10.5] (27/27) | 9 [8, 10] (3/3) | 1:9/9; 2:9/9; 3:9/9 | 12.7 | complete |
| 250 | base | base@4.0.1 (deabe970/289122cd); base@4.0.1 (deabe970/6b2bef15); 3/3 deployments matched | 785 [649, 866.5] (3/3) | 429 [333, 531.5] (27/27) | 464 [368, 498.5] (3/3) | 0:9/9; 1:9/9; 2:9/9 | 617.7 | complete |
| 250 | head | head@4.0.1 (fc50f45a/289122cd); head@4.0.1 (fc50f45a/6b2bef15); 3/3 deployments matched | 1079 [882.5, 1126.5] (3/3) | 466.5 [302.25, 568.25] (26/27) | 516.5 [391.25, 541.25] (3/3) | 0:9/9; 1:8/9; 2:9/9 | 679.75 | incomplete |
| 250 | control | control@4.0.1 (deabe970/289122cd); control@4.0.1 (deabe970/6b2bef15); 3/3 deployments matched | 786.5 [611.75, 961.25] (2/3) | 313 [219, 434.5] (19/27) | 322 [265.5, 378.5] (2/3) | 0:7/9; 1:3/9 ineligible; 2:9/9 | 531.4 | incomplete |
| 250 | pinned | pinned@4.0.0 (c823d174/289122cd); pinned@4.0.0 (c823d174/6b2bef15); 3/3 deployments matched | 907 [901, 998] (3/3) | 515 [447, 575.25] (26/27) | 513 [500, 518] (3/3) | 0:9/9; 1:9/9; 2:8/9 | 727.5 | incomplete |
| 250 | pi | pi 1.0.4 / no Effect (5526e585/866f1f7f); pi 1.0.4 / no Effect (5526e585/f1947b8f); 3/3 deployments matched | 224 [207.5, 230] (3/3) | 157 [143.5, 188.5] (27/27) | 154 [153.5, 171.5] (3/3) | 0:9/9; 1:9/9; 2:9/9 | 271.6 | complete |
| 250 | tardie | tardie@4.0.0-rc.115 (c8fbbb72/8a0cb11b); tardie@4.0.0-rc.115 (c8fbbb72/9f22f5b1); 3/3 deployments matched | 465 [426, 504] (2/3) | 16 [14, 24] (25/27) | 15 [14.5, 19.75] (3/3) | 0:8/9; 1:9/9; 2:8/9 | 27.8 | incomplete |
| 1000 | base | base@4.0.1 (deabe970/6b2bef15); 3/3 deployments matched | 872 [870, 885] (3/3) | 466 [444.5, 552.5] (27/27) | 464 [460.5, 479] (3/3) | 0:9/9; 1:9/9; 2:9/9 | 753.8 | complete |
| 1000 | head | head@4.0.1 (fc50f45a/6b2bef15); 3/3 deployments matched | 816 [765.5, 866.5] (2/3) | 513 [480, 621] (25/27) | 565 [528.5, 609.5] (3/3) | 0:9/9; 1:8/9; 2:8/9 | 860.8 | incomplete |
| 1000 | control | control@4.0.1 (deabe970/6b2bef15); 3/3 deployments matched | 929 [873.5, 1057.5] (3/3) | 485 [454.75, 816.5] (26/27) | 478 [469, 669.5] (3/3) | 0:9/9; 1:8/9; 2:9/9 | 1046 | incomplete |
| 1000 | pinned | pinned@4.0.0 (c823d174/6b2bef15); 3/3 deployments matched | 1470 [1452.5, 1515.5] (3/3) | 1058 [928.5, 1211.5] (27/27) | 971 [965.5, 1099] (3/3) | 0:9/9; 1:9/9; 2:9/9 | 1332.7 | complete |
| 1000 | pi | pi 1.0.4 / no Effect (5526e585/f1947b8f); 3/3 deployments matched | 1075 [815.5, 1078.5] (3/3) | 754 [462, 887] (27/27) | 774 [605, 825.5] (3/3) | 0:9/9; 1:9/9; 2:9/9 | 1035.3 | complete |
| 1000 | tardie | tardie@4.0.0-rc.115 (c8fbbb72/9f22f5b1); 3/3 deployments matched | 2347 [1824, 2911] (3/3) | 48 [37.5, 56.5] (27/27) | 47 [40.5, 52.5] (3/3) | 0:9/9; 1:9/9; 2:9/9 | 78.5 | complete |

| Size | Pair | Cold ratio median [IQR] (pairs/3) | Control/base cold spread | Object-median ratio median [IQR] (pairs/3) | Control/base Object spread | Paired-turn ratio median [IQR] (turn pairs/27; diagnostic) | Control/base turn spread | Status |
|---:|---|---:|---:|---:|---:|---:|---:|---|
| 50 | head/base | 0.967995 [0.862912, 1.07308] (2/3) | 0.896334 [0.843783, 0.948886] (2/3); range [0.791232, 1.00144] | 1.0452 [0.997745, 1.09656] (3/3) | 1.00592 [0.912563, 1.06582] (3/3); range [0.819209, 1.12573] | 1.09605 [0.920764, 1.20244] (27/27) | 0.974874 [0.863708, 1.08526] (27/27); range [0.657609, 1.4794] | incomplete |
| 50 | control/base | 0.896334 [0.843783, 0.948886] (2/3) | 0.896334 [0.843783, 0.948886] (2/3); range [0.791232, 1.00144] | 1.00592 [0.912563, 1.06582] (3/3) | 1.00592 [0.912563, 1.06582] (3/3); range [0.819209, 1.12573] | 0.974874 [0.863708, 1.08526] (27/27) | 0.974874 [0.863708, 1.08526] (27/27); range [0.657609, 1.4794] | incomplete |
| 50 | head/control | 0.957784 [0.801916, 1.06713] (3/3) | 0.896334 [0.843783, 0.948886] (2/3); range [0.791232, 1.00144] | 1.14118 [0.992666, 1.20852] (3/3) | 1.00592 [0.912563, 1.06582] (3/3); range [0.819209, 1.12573] | 1.07164 [0.968515, 1.18635] (27/27) | 0.974874 [0.863708, 1.08526] (27/27); range [0.657609, 1.4794] | incomplete |
| 50 | head/pi | 2.87023 [2.57662, 4.61341] (3/3) | 0.896334 [0.843783, 0.948886] (2/3); range [0.791232, 1.00144] | 1.92708 [1.90529, 2.55668] (3/3) | 1.00592 [0.912563, 1.06582] (3/3); range [0.819209, 1.12573] | 2.11954 [1.68794, 2.68705] (24/27) | 0.971412 [0.860339, 1.11992] (24/27); range [0.657609, 1.4794] | incomplete |
| 50 | head/tardie | 2.32099 [2.04586, 2.36993] (3/3) | 0.896334 [0.843783, 0.948886] (2/3); range [0.791232, 1.00144] | 26.4286 [23.9921, 27.987] (3/3) | 1.00592 [0.912563, 1.06582] (3/3); range [0.819209, 1.12573] | 26.2857 [23.1508, 31.3056] (27/27) | 0.974874 [0.863708, 1.08526] (27/27); range [0.657609, 1.4794] | incomplete |
| 50 | pinned/pi | 4.75969 [3.38299, 5.97908] (3/3) | 0.896334 [0.843783, 0.948886] (2/3); range [0.791232, 1.00144] | 3.14706 [2.35994, 3.26624] (3/3) | 1.00592 [0.912563, 1.06582] (3/3); range [0.819209, 1.12573] | 2.9949 [1.82288, 3.47872] (24/27) | 0.971412 [0.860339, 1.11992] (24/27); range [0.657609, 1.4794] | incomplete |
| 50 | pinned/tardie | 1.81121 [1.68365, 3.8161] (3/3) | 0.896334 [0.843783, 0.948886] (2/3); range [0.791232, 1.00144] | 29.1818 [23.5909, 37.8052] (3/3) | 1.00592 [0.912563, 1.06582] (3/3); range [0.819209, 1.12573] | 32.9 [24, 43.381] (27/27) | 0.974874 [0.863708, 1.08526] (27/27); range [0.657609, 1.4794] | incomplete |
| 250 | head/base | 1.13819 [1.00604, 1.71334] (3/3) | 1.14949 [1.00067, 1.29831] (2/3); range [0.851852, 1.44713] | 0.969043 [0.77116, 1.52496] (3/3) | 0.852941 [0.810662, 0.895221] (2/3); range [0.768382, 0.9375] | 0.984905 [0.696757, 1.71758] (26/27) | 0.848361 [0.730558, 1.05666] (19/27); range [0.363977, 1.31481] | incomplete |
| 250 | control/base | 1.14949 [1.00067, 1.29831] (2/3) | 1.14949 [1.00067, 1.29831] (2/3); range [0.851852, 1.44713] | 0.852941 [0.810662, 0.895221] (2/3) | 0.852941 [0.810662, 0.895221] (2/3); range [0.768382, 0.9375] | 0.848361 [0.730558, 1.05666] (19/27) | 0.848361 [0.730558, 1.05666] (19/27); range [0.363977, 1.31481] | incomplete |
| 250 | head/control | 1.64519 [1.12453, 2.16584] (2/3) | 1.14949 [1.00067, 1.29831] (2/3); range [0.851852, 1.44713] | 1.65981 [1.13565, 2.18397] (2/3) | 0.852941 [0.810662, 0.895221] (2/3); range [0.768382, 0.9375] | 1.62939 [0.620955, 2.35443] (19/27) | 0.848361 [0.730558, 1.05666] (19/27); range [0.363977, 1.31481] | incomplete |
| 250 | head/pi | 4.81696 [3.86187, 5.48178] (3/3) | 1.14949 [1.00067, 1.29831] (2/3); range [0.851852, 1.44713] | 2.7328 [2.23568, 3.20406] (3/3) | 0.852941 [0.810662, 0.895221] (2/3); range [0.768382, 0.9375] | 2.40367 [1.87657, 3.28675] (26/27) | 0.848361 [0.730558, 1.05666] (19/27); range [0.363977, 1.31481] | incomplete |
| 250 | head/tardie | 1.87986 [1.82623, 1.93348] (2/3) | 1.44713 [1.44713, 1.44713] (1/3); range [1.44713, 1.44713] | 23.102 [21.051, 28.7677] (3/3) | 0.852941 [0.810662, 0.895221] (2/3); range [0.768382, 0.9375] | 22.18 [17.6382, 30.1412] (24/27) | 0.848361 [0.767782, 1.07752] (17/27); range [0.363977, 1.31481] | incomplete |
| 250 | pinned/pi | 4.68586 [4.26454, 4.77374] (3/3) | 1.14949 [1.00067, 1.29831] (2/3); range [0.851852, 1.44713] | 3.18301 [2.9751, 3.25709] (3/3) | 0.852941 [0.810662, 0.895221] (2/3); range [0.768382, 0.9375] | 2.93452 [2.38052, 3.6477] (26/27) | 0.845136 [0.711946, 1.01316] (18/27); range [0.363977, 1.31481] | incomplete |
| 250 | pinned/tardie | 2.1746 [2.09006, 2.25913] (2/3) | 1.44713 [1.44713, 1.44713] (1/3); range [1.44713, 1.44713] | 34.7857 [27.8622, 34.8262] (3/3) | 0.852941 [0.810662, 0.895221] (2/3); range [0.768382, 0.9375] | 29.8375 [20.7193, 37.7917] (24/27) | 0.845136 [0.744711, 1.04623] (16/27); range [0.363977, 1.31481] | incomplete |
| 1000 | head/base | 0.937669 [0.880701, 0.994637] (2/3) | 1.21586 [1.14062, 1.29111] (2/3); range [1.06537, 1.36636] | 1.21767 [1.14713, 1.27078] (3/3) | 1.03017 [1.01837, 1.38654] (3/3); range [1.00656, 1.74291] | 1.06701 [0.978118, 1.29124] (25/27) | 1.08837 [0.873122, 1.34326] (24/27); range [0.512897, 2.46469] | incomplete |
| 1000 | control/base | 1.06537 [0.98814, 1.21586] (3/3) | 1.06537 [0.98814, 1.21586] (3/3); range [0.910913, 1.36636] | 1.03017 [1.01837, 1.38654] (3/3) | 1.03017 [1.01837, 1.38654] (3/3); range [1.00656, 1.74291] | 1.08837 [0.868818, 1.39041] (26/27) | 1.08837 [0.868818, 1.39041] (26/27); range [0.512897, 2.46469] | incomplete |
| 1000 | head/control | 0.794975 [0.698921, 0.891029] (2/3) | 1.21586 [1.14062, 1.29111] (2/3); range [1.06537, 1.36636] | 1.06957 [0.914574, 1.12579] (3/3) | 1.03017 [1.01837, 1.38654] (3/3); range [1.00656, 1.74291] | 1.08009 [0.83469, 1.11181] (24/27) | 1.08837 [0.873122, 1.34326] (24/27); range [0.512897, 2.46469] | incomplete |
| 1000 | head/pi | 1.1572 [0.911157, 1.40324] (2/3) | 1.21586 [1.14062, 1.29111] (2/3); range [1.06537, 1.36636] | 0.745724 [0.690691, 1.0208] (3/3) | 1.03017 [1.01837, 1.38654] (3/3); range [1.00656, 1.74291] | 0.740295 [0.570071, 1.16313] (25/27) | 1.08837 [0.873122, 1.34326] (24/27); range [0.512897, 2.46469] | incomplete |
| 1000 | head/tardie | 0.406731 [0.335308, 0.478154] (2/3) | 1.21586 [1.14062, 1.29111] (2/3); range [1.06537, 1.36636] | 10.4681 [10.1047, 14.8517] (3/3) | 1.03017 [1.01837, 1.38654] (3/3); range [1.00656, 1.74291] | 11.1277 [9.46154, 13.375] (25/27) | 1.08837 [0.873122, 1.34326] (24/27); range [0.512897, 2.46469] | incomplete |
| 1000 | pinned/pi | 1.45209 [1.38917, 2.04799] (3/3) | 1.06537 [0.98814, 1.21586] (3/3); range [0.910913, 1.36636] | 1.58527 [1.33996, 1.90617] (3/3) | 1.03017 [1.01837, 1.38654] (3/3); range [1.00656, 1.74291] | 1.46313 [1.11497, 1.89579] (27/27) | 1.08837 [0.868818, 1.39041] (26/27); range [0.512897, 2.46469] | incomplete |
| 1000 | pinned/tardie | 0.611419 [0.51722, 0.905633] (3/3) | 1.06537 [0.98814, 1.21586] (3/3); range [0.910913, 1.36636] | 26.1064 [21.4239, 27.1708] (3/3) | 1.03017 [1.01837, 1.38654] (3/3); range [1.00656, 1.74291] | 22.8571 [18.2179, 26.7112] (27/27) | 1.08837 [0.868818, 1.39041] (26/27); range [0.512897, 2.46469] | incomplete |

## RPC normalizedCpuRatio (ms CPU / ms reference CPU)

Object-median eligibility: at least 7 of 9 warm observations. Per-cluster counts below retain the sample ID and mark ineligible medians.

| Size | Role | Deployed build set | Cold median [p25, p75] (clusters/3) | README warm median [p25, p75] (turns/27) | Object-median median [p25, p75] (eligible clusters/3) | Warm n per cluster | Warm-turn p95 | Coverage |
|---:|---|---|---:|---:|---:|---|---:|---|
| 50 | base | base@4.0.1 (deabe970/289122cd); 3/3 deployments matched | 3.82276 [3.80268, 3.84283] (2/3) | 1.10687 [0.836012, 4.90588] (27/27) | 1.10687 [0.962526, 3.11404] (3/3) | 1:9/9; 2:9/9; 3:9/9 | 6.22705 | incomplete |
| 50 | head | head@4.0.1 (fc50f45a/289122cd); 3/3 deployments matched | 3.10744 [2.83189, 4.16518] (3/3) | 5.1875 [1.3336, 5.96774] (27/27) | 5.86111 [3.48801, 6.15469] (3/3) | 1:9/9; 2:9/9; 3:9/9 | 7.31793 | complete |
| 50 | control | control@4.0.1 (deabe970/289122cd); 3/3 deployments matched | 4.49677 [3.66257, 4.71449] (3/3) | 1.3255 [0.982502, 3.58632] (27/27) | 1.3255 [1.01881, 3.16275] (3/3) | 1:9/9; 2:9/9; 3:9/9 | 6.02065 | complete |
| 50 | pinned | pinned@4.0.0 (c823d174/289122cd); 3/3 deployments matched | 3.86164 [2.86905, 4.75417] (3/3) | 1.21569 [0.88785, 7.37209] (25/27) | 1.18294 [0.966468, 4.50712] (3/3) | 1:8/9; 2:8/9; 3:9/9 | 8.7814 | incomplete |
| 50 | pi | pi 1.0.4 / no Effect (5526e585/866f1f7f); 3/3 deployments matched | 1.09322 [1.07913, 1.23791] (3/3) | 3.07119 [0.509196, 3.50862] (24/27) | 3.37931 [1.93086, 3.38966] (3/3) | 1:9/9; 2:8/9; 3:7/9 | 4.70414 | incomplete |
| 50 | tardie | tardie@4.0.0-rc.115 (c8fbbb72/8a0cb11b); 3/3 deployments matched | 1.72269 [1.57187, 2.12627] (3/3) | 0.0493274 [0.0368421, 0.181818] (25/27) | 0.0477729 [0.0421156, 0.144199] (3/3) | 1:8/9; 2:9/9; 3:8/9 | 0.319212 | incomplete |
| 250 | base | base@4.0.1 (deabe970/289122cd); base@4.0.1 (deabe970/6b2bef15); 3/3 deployments matched | 4.38547 [4.19664, 4.82607] (3/3) | 9.32917 [1.60786, 12.6744] (26/27) | 11.1429 [6.2583, 12.0548] (3/3) | 0:9/9; 1:8/9; 2:9/9 | 15.1487 | incomplete |
| 250 | head | head@4.0.1 (fc50f45a/289122cd); head@4.0.1 (fc50f45a/6b2bef15); 3/3 deployments matched | 6.42262 [6.11809, 6.54654] (3/3) | 2.1553 [1.72416, 4.58912] (23/27) | 1.89486 [1.78778, 2.00194] (2/3) | 0:9/9; 1:8/9; 2:6/9 ineligible | 9.04327 | incomplete |
| 250 | control | control@4.0.1 (deabe970/289122cd); control@4.0.1 (deabe970/6b2bef15); 3/3 deployments matched | 5.07318 [4.43659, 5.70978] (2/3) | 9.40761 [7.74384, 11.16] (16/27) | 11.1282 [11.1282, 11.1282] (1/3) | 0:6/9 ineligible; 1:1/9 ineligible; 2:9/9 | 13.1429 | incomplete |
| 250 | pinned | pinned@4.0.0 (c823d174/289122cd); pinned@4.0.0 (c823d174/6b2bef15); 3/3 deployments matched | 5.49697 [5.38084, 6.2842] (3/3) | 1.70214 [1.41814, 1.94392] (26/27) | 1.7952 [1.66589, 1.86438] (3/3) | 0:9/9; 1:9/9; 2:8/9 | 2.69283 | incomplete |
| 250 | pi | pi 1.0.4 / no Effect (5526e585/866f1f7f); pi 1.0.4 / no Effect (5526e585/f1947b8f); 3/3 deployments matched | 1.85827 [1.76685, 1.87829] (3/3) | 5.27586 [4.37711, 6.08421] (27/27) | 5.03571 [5.00173, 5.66786] (3/3) | 0:9/9; 1:9/9; 2:9/9 | 9.1354 | complete |
| 250 | tardie | tardie@4.0.0-rc.115 (c8fbbb72/8a0cb11b); tardie@4.0.0-rc.115 (c8fbbb72/9f22f5b1); 3/3 deployments matched | 3.38898 [2.91147, 3.86649] (2/3) | 0.0645576 [0.0597703, 0.0749842] (24/27) | 0.0644714 [0.0636165, 0.0665764] (3/3) | 0:7/9; 1:9/9; 2:8/9 | 0.115004 | incomplete |
| 1000 | base | base@4.0.1 (deabe970/6b2bef15); 3/3 deployments matched | 7.35593 [7.02871, 7.43659] (3/3) | 14.1724 [2.64083, 16.5377] (27/27) | 15.4667 [8.8966, 15.9667] (3/3) | 0:9/9; 1:9/9; 2:9/9 | 24.4243 | complete |
| 1000 | head | head@4.0.1 (fc50f45a/6b2bef15); 3/3 deployments matched | 7.04931 [6.68837, 7.41025] (2/3) | 13.6116 [4.09134, 16.3203] (24/27) | 14.4194 [8.66156, 14.7538] (3/3) | 0:9/9; 1:8/9; 2:7/9 | 20.485 | incomplete |
| 1000 | control | control@4.0.1 (deabe970/6b2bef15); 3/3 deployments matched | 7.7013 [7.43837, 7.96127] (3/3) | 12.4333 [3.50809, 16.4138] (25/27) | 15.5161 [9.36338, 15.7682] (3/3) | 0:8/9; 1:8/9; 2:9/9 | 17.1645 | incomplete |
| 1000 | pinned | pinned@4.0.0 (c823d174/6b2bef15); 3/3 deployments matched | 8.85802 [7.84762, 8.9668] (3/3) | 20.4783 [4.64095, 24.0345] (27/27) | 22.275 [13.2565, 22.597] (3/3) | 0:9/9; 1:9/9; 2:9/9 | 32.7278 | complete |
| 1000 | pi | pi 1.0.4 / no Effect (5526e585/f1947b8f); 3/3 deployments matched | 5.34826 [4.5399, 5.49184] (3/3) | 17.9744 [14.7167, 20.493] (27/27) | 19.8462 [17.1897, 19.9962] (3/3) | 0:9/9; 1:9/9; 2:9/9 | 26.047 | complete |
| 1000 | tardie | tardie@4.0.0-rc.115 (c8fbbb72/9f22f5b1); 3/3 deployments matched | 14.0539 [12.4478, 17.6865] (3/3) | 0.240848 [0.163512, 1.04808] (26/27) | 0.210394 [0.18615, 0.723618] (3/3) | 0:9/9; 1:9/9; 2:8/9 | 1.52206 | incomplete |

| Size | Pair | Cold ratio median [IQR] (pairs/3) | Control/base cold spread | Object-median ratio median [IQR] (pairs/3) | Control/base Object spread | Paired-turn ratio median [IQR] (turn pairs/27; diagnostic) | Control/base turn spread | Status |
|---:|---|---:|---:|---:|---:|---:|---:|---|
| 50 | head/base | 1.02127 [0.841518, 1.20102] (2/3) | 0.960493 [0.846339, 1.07465] (2/3); range [0.732185, 1.1888] | 1.25913 [1.1332, 4.21136] (3/3) | 0.976331 [0.923351, 1.08693] (3/3); range [0.87037, 1.19752] | 1.41158 [1.04366, 6.21835] (27/27) | 1.02573 [0.843184, 1.14949] (27/27); range [0.663396, 1.65812] | incomplete |
| 50 | control/base | 0.960493 [0.846339, 1.07465] (2/3) | 0.960493 [0.846339, 1.07465] (2/3); range [0.732185, 1.1888] | 0.976331 [0.923351, 1.08693] (3/3) | 0.976331 [0.923351, 1.08693] (3/3); range [0.87037, 1.19752] | 1.02573 [0.843184, 1.14949] (27/27) | 1.02573 [0.843184, 1.14949] (27/27); range [0.663396, 1.65812] | incomplete |
| 50 | head/control | 0.903824 [0.766927, 1.03265] (3/3) | 0.960493 [0.846339, 1.07465] (2/3); range [0.732185, 1.1888] | 1.28966 [1.06539, 4.76008] (3/3) | 0.976331 [0.923351, 1.08693] (3/3); range [0.87037, 1.19752] | 1.28966 [0.94719, 6.02067] (27/27) | 1.02573 [0.843184, 1.14949] (27/27); range [0.663396, 1.65812] | incomplete |
| 50 | head/pi | 2.91767 [2.3833, 3.84762] (3/3) | 0.960493 [0.846339, 1.07465] (2/3); range [0.732185, 1.1888] | 1.90816 [1.11804, 7.02897] (3/3) | 0.976331 [0.923351, 1.08693] (3/3); range [0.87037, 1.19752] | 1.77652 [0.3993, 10.4161] (24/27) | 1.0237 [0.843233, 1.18286] (24/27); range [0.663396, 1.65812] | incomplete |
| 50 | head/tardie | 2.06452 [1.77422, 2.12562] (3/3) | 0.960493 [0.846339, 1.07465] (2/3); range [0.732185, 1.1888] | 26.798 [25.0678, 93.78] (3/3) | 0.976331 [0.923351, 1.08693] (3/3); range [0.87037, 1.19752] | 31.846 [25.2943, 140.829] (25/27) | 1.04577 [0.843281, 1.16759] (25/27); range [0.663396, 1.65812] | incomplete |
| 50 | pinned/pi | 3.53235 [2.44477, 4.41711] (3/3) | 0.960493 [0.846339, 1.07465] (2/3); range [0.732185, 1.1888] | 2.30332 [1.26263, 2.37774] (3/3) | 0.976331 [0.923351, 1.08693] (3/3); range [0.87037, 1.19752] | 1.86123 [0.364608, 2.73688] (22/27) | 1.0237 [0.843136, 1.15854] (22/27); range [0.663396, 1.65812] | incomplete |
| 50 | pinned/tardie | 1.52643 [1.30785, 2.75002] (3/3) | 0.960493 [0.846339, 1.07465] (2/3); range [0.732185, 1.1888] | 32.4462 [17.7816, 98.1869] (3/3) | 0.976331 [0.923351, 1.08693] (3/3); range [0.87037, 1.19752] | 33.2301 [6.53909, 143.17] (23/27) | 1.04577 [0.843184, 1.14949] (23/27); range [0.663396, 1.65812] | incomplete |
| 250 | head/base | 1.32564 [1.27256, 1.495] (3/3) | 1.19764 [1.07289, 1.32239] (2/3); range [0.948148, 1.44713] | 0.832431 [0.481024, 1.18384] (2/3) | unavailable [unavailable, unavailable] (0/3); range [unavailable, unavailable] | 0.812369 [0.169349, 1.43551] (22/27) | 1.19382 [1.0358, 5.45103] (13/27); range [0.671067, 6.85142] | incomplete |
| 250 | control/base | 1.19764 [1.07289, 1.32239] (2/3) | 1.19764 [1.07289, 1.32239] (2/3); range [0.948148, 1.44713] | 0.998685 [0.998685, 0.998685] (1/3) | 0.998685 [0.998685, 0.998685] (1/3); range [0.998685, 0.998685] | 1.1387 [0.98981, 5.29741] (16/27) | 1.1387 [0.98981, 5.29741] (16/27); range [0.671067, 6.85142] | incomplete |
| 250 | head/control | 1.33571 [1.12588, 1.54555] (2/3) | 1.19764 [1.07289, 1.32239] (2/3); range [0.948148, 1.44713] | unavailable [unavailable, unavailable] (0/3) | unavailable [unavailable, unavailable] (0/3); range [unavailable, unavailable] | 0.286627 [0.25061, 0.704567] (13/27) | 1.19382 [1.0358, 5.45103] (13/27); range [0.671067, 6.85142] | incomplete |
| 250 | head/pi | 3.38334 [3.25591, 3.68233] (3/3) | 1.19764 [1.07289, 1.32239] (2/3); range [0.948148, 1.44713] | 0.342796 [0.304787, 0.380804] (2/3) | unavailable [unavailable, unavailable] (0/3); range [unavailable, unavailable] | 0.418826 [0.31269, 0.927661] (23/27) | 1.19382 [1.0358, 5.45103] (13/27); range [0.671067, 6.85142] | incomplete |
| 250 | head/tardie | 1.93351 [1.70601, 2.16101] (2/3) | 1.44713 [1.44713, 1.44713] (1/3); range [1.44713, 1.44713] | 28.7433 [27.7613, 29.7253] (2/3) | unavailable [unavailable, unavailable] (0/3); range [unavailable, unavailable] | 31.3812 [22.8132, 101.84] (21/27) | 1.17805 [0.94899, 5.33038] (12/27); range [0.671067, 6.85142] | incomplete |
| 250 | pinned/pi | 3.14229 [3.0502, 3.43371] (3/3) | 1.19764 [1.07289, 1.32239] (2/3); range [0.948148, 1.44713] | 0.306913 [0.306025, 0.334143] (3/3) | 0.998685 [0.998685, 0.998685] (1/3); range [0.998685, 0.998685] | 0.322471 [0.253494, 0.416831] (26/27) | 1.1151 [0.972373, 5.34862] (15/27); range [0.671067, 6.85142] | incomplete |
| 250 | pinned/tardie | 1.94315 [1.78551, 2.1008] (2/3) | 1.44713 [1.44713, 1.44713] (1/3); range [1.44713, 1.44713] | 27.8449 [25.1088, 29.3265] (3/3) | 0.998685 [0.998685, 0.998685] (1/3); range [0.998685, 0.998685] | 24.54 [19.3252, 32.3663] (23/27) | 1.1151 [0.9375, 5.2462] (13/27); range [0.671067, 6.85142] | incomplete |
| 1000 | head/base | 0.946981 [0.903581, 0.990382] (2/3) | 1.0703 [1.05863, 1.08198] (2/3); range [1.04695, 1.09365] | 0.975532 [0.575937, 3.58666] (3/3) | 1.0032 [0.599088, 3.94456] (3/3); range [0.194977, 6.88593] | 1.12653 [0.243962, 5.88677] (24/27) | 1.01699 [0.234941, 6.48989] (22/27); range [0.117679, 8.9552] | incomplete |
| 1000 | control/base | 1.07072 [1.05884, 1.08219] (3/3) | 1.07072 [1.05884, 1.08219] (3/3); range [1.04695, 1.09365] | 1.0032 [0.599088, 3.94456] (3/3) | 1.0032 [0.599088, 3.94456] (3/3); range [0.194977, 6.88593] | 0.812627 [0.263182, 6.45536] (25/27) | 0.812627 [0.263182, 6.45536] (25/27); range [0.117679, 8.9552] | incomplete |
| 1000 | head/control | 0.883432 [0.852519, 0.914345] (2/3) | 1.0703 [1.05863, 1.08198] (2/3); range [1.04695, 1.09365] | 0.904423 [0.902245, 0.938423] (3/3) | 1.0032 [0.599088, 3.94456] (3/3); range [0.194977, 6.88593] | 0.983498 [0.835006, 1.13217] (22/27) | 1.01699 [0.234941, 6.48989] (22/27); range [0.117679, 8.9552] | incomplete |
| 1000 | head/pi | 1.63282 [1.40795, 1.8577] (2/3) | 1.0703 [1.05863, 1.08198] (2/3); range [1.04695, 1.09365] | 0.726557 [0.435345, 0.882369] (3/3) | 1.0032 [0.599088, 3.94456] (3/3); range [0.194977, 6.88593] | 0.693495 [0.18122, 1.00235] (24/27) | 1.01699 [0.234941, 6.48989] (22/27); range [0.117679, 8.9552] | incomplete |
| 1000 | head/tardie | 0.47407 [0.419295, 0.528846] (2/3) | 1.0703 [1.05863, 1.08198] (2/3); range [1.04695, 1.09365] | 17.935 [14.7966, 44.8245] (3/3) | 1.0032 [0.599088, 3.94456] (3/3); range [0.194977, 6.88593] | 14.9298 [10.6925, 45.4528] (23/27) | 1.0208 [0.225527, 6.50141] (21/27); range [0.117679, 8.9552] | incomplete |
| 1000 | pinned/pi | 1.69692 [1.63439, 1.7646] (3/3) | 1.07072 [1.05884, 1.08219] (3/3); range [1.04695, 1.09365] | 1.13762 [0.675585, 1.33515] (3/3) | 1.0032 [0.599088, 3.94456] (3/3); range [0.194977, 6.88593] | 1.0888 [0.260073, 1.42679] (27/27) | 0.812627 [0.263182, 6.45536] (25/27); range [0.117679, 8.9552] | incomplete |
| 1000 | pinned/tardie | 0.63029 [0.4755, 0.733696] (3/3) | 1.07072 [1.05884, 1.08219] (3/3); range [1.04695, 1.09365] | 105.873 [54.6496, 123.715] (3/3) | 1.0032 [0.599088, 3.94456] (3/3); range [0.194977, 6.88593] | 87.9745 [4.37843, 134.662] (26/27) | 0.798494 [0.253768, 6.46687] (24/27); range [0.117679, 8.9552] | incomplete |

## RPC normalizedDoTotalCpuRatio (ms CPU / ms reference CPU)

Object-median eligibility: at least 7 of 9 warm observations. Per-cluster counts below retain the sample ID and mark ineligible medians.

| Size | Role | Deployed build set | Cold median [p25, p75] (clusters/3) | README warm median [p25, p75] (turns/27) | Object-median median [p25, p75] (eligible clusters/3) | Warm n per cluster | Warm-turn p95 | Coverage |
|---:|---|---|---:|---:|---:|---|---:|---|
| 50 | base | base@4.0.1 (deabe970/289122cd); 3/3 deployments matched | 3.82276 [3.80268, 3.84283] (2/3) | 1.10687 [0.836012, 4.90588] (27/27) | 1.10687 [0.962526, 3.11404] (3/3) | 1:9/9; 2:9/9; 3:9/9 | 6.22705 | incomplete |
| 50 | head | head@4.0.1 (fc50f45a/289122cd); 3/3 deployments matched | 3.10744 [2.83189, 4.16518] (3/3) | 5.1875 [1.3336, 5.96774] (27/27) | 5.86111 [3.48801, 6.15469] (3/3) | 1:9/9; 2:9/9; 3:9/9 | 7.31793 | complete |
| 50 | control | control@4.0.1 (deabe970/289122cd); 3/3 deployments matched | 4.49677 [3.66257, 4.71449] (3/3) | 1.3255 [0.982502, 3.58632] (27/27) | 1.3255 [1.01881, 3.16275] (3/3) | 1:9/9; 2:9/9; 3:9/9 | 6.02065 | complete |
| 50 | pinned | pinned@4.0.0 (c823d174/289122cd); 3/3 deployments matched | 3.86164 [2.86905, 4.75417] (3/3) | 1.21569 [0.88785, 7.37209] (25/27) | 1.18294 [0.966468, 4.50712] (3/3) | 1:8/9; 2:8/9; 3:9/9 | 8.7814 | incomplete |
| 50 | pi | pi 1.0.4 / no Effect (5526e585/866f1f7f); 3/3 deployments matched | 1.09322 [1.07913, 1.23791] (3/3) | 3.07119 [0.509196, 3.50862] (24/27) | 3.37931 [1.93086, 3.38966] (3/3) | 1:9/9; 2:8/9; 3:7/9 | 4.70414 | incomplete |
| 50 | tardie | tardie@4.0.0-rc.115 (c8fbbb72/8a0cb11b); 3/3 deployments matched | 1.72269 [1.57187, 2.12627] (3/3) | 0.0493274 [0.0368421, 0.181818] (25/27) | 0.0477729 [0.0421156, 0.144199] (3/3) | 1:8/9; 2:9/9; 3:8/9 | 0.319212 | incomplete |
| 250 | base | base@4.0.1 (deabe970/289122cd); base@4.0.1 (deabe970/6b2bef15); 3/3 deployments matched | 4.38547 [4.19664, 4.82607] (3/3) | 9.32917 [1.60786, 12.6744] (26/27) | 11.1429 [6.2583, 12.0548] (3/3) | 0:9/9; 1:8/9; 2:9/9 | 15.1487 | incomplete |
| 250 | head | head@4.0.1 (fc50f45a/289122cd); head@4.0.1 (fc50f45a/6b2bef15); 3/3 deployments matched | 6.42262 [6.11809, 6.54654] (3/3) | 2.1553 [1.72416, 4.58912] (23/27) | 1.89486 [1.78778, 2.00194] (2/3) | 0:9/9; 1:8/9; 2:6/9 ineligible | 9.04327 | incomplete |
| 250 | control | control@4.0.1 (deabe970/289122cd); control@4.0.1 (deabe970/6b2bef15); 3/3 deployments matched | 5.07318 [4.43659, 5.70978] (2/3) | 9.40761 [7.74384, 11.16] (16/27) | 11.1282 [11.1282, 11.1282] (1/3) | 0:6/9 ineligible; 1:1/9 ineligible; 2:9/9 | 13.1429 | incomplete |
| 250 | pinned | pinned@4.0.0 (c823d174/289122cd); pinned@4.0.0 (c823d174/6b2bef15); 3/3 deployments matched | 5.49697 [5.38084, 6.2842] (3/3) | 1.70214 [1.41814, 1.94392] (26/27) | 1.7952 [1.66589, 1.86438] (3/3) | 0:9/9; 1:9/9; 2:8/9 | 2.69283 | incomplete |
| 250 | pi | pi 1.0.4 / no Effect (5526e585/866f1f7f); pi 1.0.4 / no Effect (5526e585/f1947b8f); 3/3 deployments matched | 1.85827 [1.76685, 1.87829] (3/3) | 5.27586 [4.37711, 6.08421] (27/27) | 5.03571 [5.00173, 5.66786] (3/3) | 0:9/9; 1:9/9; 2:9/9 | 9.1354 | complete |
| 250 | tardie | tardie@4.0.0-rc.115 (c8fbbb72/8a0cb11b); tardie@4.0.0-rc.115 (c8fbbb72/9f22f5b1); 3/3 deployments matched | 3.38898 [2.91147, 3.86649] (2/3) | 0.0645576 [0.0597703, 0.0749842] (24/27) | 0.0644714 [0.0636165, 0.0665764] (3/3) | 0:7/9; 1:9/9; 2:8/9 | 0.115004 | incomplete |
| 1000 | base | base@4.0.1 (deabe970/6b2bef15); 3/3 deployments matched | 7.35593 [7.02871, 7.43659] (3/3) | 14.1724 [2.64083, 16.5377] (27/27) | 15.4667 [8.8966, 15.9667] (3/3) | 0:9/9; 1:9/9; 2:9/9 | 24.4243 | complete |
| 1000 | head | head@4.0.1 (fc50f45a/6b2bef15); 3/3 deployments matched | 7.04931 [6.68837, 7.41025] (2/3) | 13.6116 [4.09134, 16.3203] (24/27) | 14.4194 [8.66156, 14.7538] (3/3) | 0:9/9; 1:8/9; 2:7/9 | 20.485 | incomplete |
| 1000 | control | control@4.0.1 (deabe970/6b2bef15); 3/3 deployments matched | 7.7013 [7.43837, 7.96127] (3/3) | 12.4333 [3.50809, 16.4138] (25/27) | 15.5161 [9.36338, 15.7682] (3/3) | 0:8/9; 1:8/9; 2:9/9 | 17.1645 | incomplete |
| 1000 | pinned | pinned@4.0.0 (c823d174/6b2bef15); 3/3 deployments matched | 8.85802 [7.84762, 8.9668] (3/3) | 20.4783 [4.64095, 24.0345] (27/27) | 22.275 [13.2565, 22.597] (3/3) | 0:9/9; 1:9/9; 2:9/9 | 32.7278 | complete |
| 1000 | pi | pi 1.0.4 / no Effect (5526e585/f1947b8f); 3/3 deployments matched | 5.34826 [4.5399, 5.49184] (3/3) | 17.9744 [14.7167, 20.493] (27/27) | 19.8462 [17.1897, 19.9962] (3/3) | 0:9/9; 1:9/9; 2:9/9 | 26.047 | complete |
| 1000 | tardie | tardie@4.0.0-rc.115 (c8fbbb72/9f22f5b1); 3/3 deployments matched | 14.0539 [12.4478, 17.6865] (3/3) | 0.240848 [0.163512, 1.04808] (26/27) | 0.210394 [0.18615, 0.723618] (3/3) | 0:9/9; 1:9/9; 2:8/9 | 1.52206 | incomplete |

| Size | Pair | Cold ratio median [IQR] (pairs/3) | Control/base cold spread | Object-median ratio median [IQR] (pairs/3) | Control/base Object spread | Paired-turn ratio median [IQR] (turn pairs/27; diagnostic) | Control/base turn spread | Status |
|---:|---|---:|---:|---:|---:|---:|---:|---|
| 50 | head/base | 1.02127 [0.841518, 1.20102] (2/3) | 0.960493 [0.846339, 1.07465] (2/3); range [0.732185, 1.1888] | 1.25913 [1.1332, 4.21136] (3/3) | 0.976331 [0.923351, 1.08693] (3/3); range [0.87037, 1.19752] | 1.41158 [1.04366, 6.21835] (27/27) | 1.02573 [0.843184, 1.14949] (27/27); range [0.663396, 1.65812] | incomplete |
| 50 | control/base | 0.960493 [0.846339, 1.07465] (2/3) | 0.960493 [0.846339, 1.07465] (2/3); range [0.732185, 1.1888] | 0.976331 [0.923351, 1.08693] (3/3) | 0.976331 [0.923351, 1.08693] (3/3); range [0.87037, 1.19752] | 1.02573 [0.843184, 1.14949] (27/27) | 1.02573 [0.843184, 1.14949] (27/27); range [0.663396, 1.65812] | incomplete |
| 50 | head/control | 0.903824 [0.766927, 1.03265] (3/3) | 0.960493 [0.846339, 1.07465] (2/3); range [0.732185, 1.1888] | 1.28966 [1.06539, 4.76008] (3/3) | 0.976331 [0.923351, 1.08693] (3/3); range [0.87037, 1.19752] | 1.28966 [0.94719, 6.02067] (27/27) | 1.02573 [0.843184, 1.14949] (27/27); range [0.663396, 1.65812] | incomplete |
| 50 | head/pi | 2.91767 [2.3833, 3.84762] (3/3) | 0.960493 [0.846339, 1.07465] (2/3); range [0.732185, 1.1888] | 1.90816 [1.11804, 7.02897] (3/3) | 0.976331 [0.923351, 1.08693] (3/3); range [0.87037, 1.19752] | 1.77652 [0.3993, 10.4161] (24/27) | 1.0237 [0.843233, 1.18286] (24/27); range [0.663396, 1.65812] | incomplete |
| 50 | head/tardie | 2.06452 [1.77422, 2.12562] (3/3) | 0.960493 [0.846339, 1.07465] (2/3); range [0.732185, 1.1888] | 26.798 [25.0678, 93.78] (3/3) | 0.976331 [0.923351, 1.08693] (3/3); range [0.87037, 1.19752] | 31.846 [25.2943, 140.829] (25/27) | 1.04577 [0.843281, 1.16759] (25/27); range [0.663396, 1.65812] | incomplete |
| 50 | pinned/pi | 3.53235 [2.44477, 4.41711] (3/3) | 0.960493 [0.846339, 1.07465] (2/3); range [0.732185, 1.1888] | 2.30332 [1.26263, 2.37774] (3/3) | 0.976331 [0.923351, 1.08693] (3/3); range [0.87037, 1.19752] | 1.86123 [0.364608, 2.73688] (22/27) | 1.0237 [0.843136, 1.15854] (22/27); range [0.663396, 1.65812] | incomplete |
| 50 | pinned/tardie | 1.52643 [1.30785, 2.75002] (3/3) | 0.960493 [0.846339, 1.07465] (2/3); range [0.732185, 1.1888] | 32.4462 [17.7816, 98.1869] (3/3) | 0.976331 [0.923351, 1.08693] (3/3); range [0.87037, 1.19752] | 33.2301 [6.53909, 143.17] (23/27) | 1.04577 [0.843184, 1.14949] (23/27); range [0.663396, 1.65812] | incomplete |
| 250 | head/base | 1.32564 [1.27256, 1.495] (3/3) | 1.19764 [1.07289, 1.32239] (2/3); range [0.948148, 1.44713] | 0.832431 [0.481024, 1.18384] (2/3) | unavailable [unavailable, unavailable] (0/3); range [unavailable, unavailable] | 0.812369 [0.169349, 1.43551] (22/27) | 1.19382 [1.0358, 5.45103] (13/27); range [0.671067, 6.85142] | incomplete |
| 250 | control/base | 1.19764 [1.07289, 1.32239] (2/3) | 1.19764 [1.07289, 1.32239] (2/3); range [0.948148, 1.44713] | 0.998685 [0.998685, 0.998685] (1/3) | 0.998685 [0.998685, 0.998685] (1/3); range [0.998685, 0.998685] | 1.1387 [0.98981, 5.29741] (16/27) | 1.1387 [0.98981, 5.29741] (16/27); range [0.671067, 6.85142] | incomplete |
| 250 | head/control | 1.33571 [1.12588, 1.54555] (2/3) | 1.19764 [1.07289, 1.32239] (2/3); range [0.948148, 1.44713] | unavailable [unavailable, unavailable] (0/3) | unavailable [unavailable, unavailable] (0/3); range [unavailable, unavailable] | 0.286627 [0.25061, 0.704567] (13/27) | 1.19382 [1.0358, 5.45103] (13/27); range [0.671067, 6.85142] | incomplete |
| 250 | head/pi | 3.38334 [3.25591, 3.68233] (3/3) | 1.19764 [1.07289, 1.32239] (2/3); range [0.948148, 1.44713] | 0.342796 [0.304787, 0.380804] (2/3) | unavailable [unavailable, unavailable] (0/3); range [unavailable, unavailable] | 0.418826 [0.31269, 0.927661] (23/27) | 1.19382 [1.0358, 5.45103] (13/27); range [0.671067, 6.85142] | incomplete |
| 250 | head/tardie | 1.93351 [1.70601, 2.16101] (2/3) | 1.44713 [1.44713, 1.44713] (1/3); range [1.44713, 1.44713] | 28.7433 [27.7613, 29.7253] (2/3) | unavailable [unavailable, unavailable] (0/3); range [unavailable, unavailable] | 31.3812 [22.8132, 101.84] (21/27) | 1.17805 [0.94899, 5.33038] (12/27); range [0.671067, 6.85142] | incomplete |
| 250 | pinned/pi | 3.14229 [3.0502, 3.43371] (3/3) | 1.19764 [1.07289, 1.32239] (2/3); range [0.948148, 1.44713] | 0.306913 [0.306025, 0.334143] (3/3) | 0.998685 [0.998685, 0.998685] (1/3); range [0.998685, 0.998685] | 0.322471 [0.253494, 0.416831] (26/27) | 1.1151 [0.972373, 5.34862] (15/27); range [0.671067, 6.85142] | incomplete |
| 250 | pinned/tardie | 1.94315 [1.78551, 2.1008] (2/3) | 1.44713 [1.44713, 1.44713] (1/3); range [1.44713, 1.44713] | 27.8449 [25.1088, 29.3265] (3/3) | 0.998685 [0.998685, 0.998685] (1/3); range [0.998685, 0.998685] | 24.54 [19.3252, 32.3663] (23/27) | 1.1151 [0.9375, 5.2462] (13/27); range [0.671067, 6.85142] | incomplete |
| 1000 | head/base | 0.946981 [0.903581, 0.990382] (2/3) | 1.0703 [1.05863, 1.08198] (2/3); range [1.04695, 1.09365] | 0.975532 [0.575937, 3.58666] (3/3) | 1.0032 [0.599088, 3.94456] (3/3); range [0.194977, 6.88593] | 1.12653 [0.243962, 5.88677] (24/27) | 1.01699 [0.234941, 6.48989] (22/27); range [0.117679, 8.9552] | incomplete |
| 1000 | control/base | 1.07072 [1.05884, 1.08219] (3/3) | 1.07072 [1.05884, 1.08219] (3/3); range [1.04695, 1.09365] | 1.0032 [0.599088, 3.94456] (3/3) | 1.0032 [0.599088, 3.94456] (3/3); range [0.194977, 6.88593] | 0.812627 [0.263182, 6.45536] (25/27) | 0.812627 [0.263182, 6.45536] (25/27); range [0.117679, 8.9552] | incomplete |
| 1000 | head/control | 0.883432 [0.852519, 0.914345] (2/3) | 1.0703 [1.05863, 1.08198] (2/3); range [1.04695, 1.09365] | 0.904423 [0.902245, 0.938423] (3/3) | 1.0032 [0.599088, 3.94456] (3/3); range [0.194977, 6.88593] | 0.983498 [0.835006, 1.13217] (22/27) | 1.01699 [0.234941, 6.48989] (22/27); range [0.117679, 8.9552] | incomplete |
| 1000 | head/pi | 1.63282 [1.40795, 1.8577] (2/3) | 1.0703 [1.05863, 1.08198] (2/3); range [1.04695, 1.09365] | 0.726557 [0.435345, 0.882369] (3/3) | 1.0032 [0.599088, 3.94456] (3/3); range [0.194977, 6.88593] | 0.693495 [0.18122, 1.00235] (24/27) | 1.01699 [0.234941, 6.48989] (22/27); range [0.117679, 8.9552] | incomplete |
| 1000 | head/tardie | 0.47407 [0.419295, 0.528846] (2/3) | 1.0703 [1.05863, 1.08198] (2/3); range [1.04695, 1.09365] | 17.935 [14.7966, 44.8245] (3/3) | 1.0032 [0.599088, 3.94456] (3/3); range [0.194977, 6.88593] | 14.9298 [10.6925, 45.4528] (23/27) | 1.0208 [0.225527, 6.50141] (21/27); range [0.117679, 8.9552] | incomplete |
| 1000 | pinned/pi | 1.69692 [1.63439, 1.7646] (3/3) | 1.07072 [1.05884, 1.08219] (3/3); range [1.04695, 1.09365] | 1.13762 [0.675585, 1.33515] (3/3) | 1.0032 [0.599088, 3.94456] (3/3); range [0.194977, 6.88593] | 1.0888 [0.260073, 1.42679] (27/27) | 0.812627 [0.263182, 6.45536] (25/27); range [0.117679, 8.9552] | incomplete |
| 1000 | pinned/tardie | 0.63029 [0.4755, 0.733696] (3/3) | 1.07072 [1.05884, 1.08219] (3/3); range [1.04695, 1.09365] | 105.873 [54.6496, 123.715] (3/3) | 1.0032 [0.599088, 3.94456] (3/3); range [0.194977, 6.88593] | 87.9745 [4.37843, 134.662] (26/27) | 0.798494 [0.253768, 6.46687] (24/27); range [0.117679, 8.9552] | incomplete |

## RPC referenceClientWallMs (ms reference client wall)

Object-median eligibility: at least 9 of 9 warm observations. Per-cluster counts below retain the sample ID and mark ineligible medians.

| Size | Role | Deployed build set | Cold median [p25, p75] (clusters/3) | README warm median [p25, p75] (turns/27) | Object-median median [p25, p75] (eligible clusters/3) | Warm n per cluster | Warm-turn p95 | Coverage |
|---:|---|---|---:|---:|---:|---|---:|---|
| 50 | base | base@4.0.1 (deabe970/289122cd); 3/3 deployments matched | 480 [474, 511] (3/3) | 575 [386, 644] (27/27) | 575 [478.5, 636] (3/3) | 1:9/9; 2:9/9; 3:9/9 | 703.7 | complete |
| 50 | head | head@4.0.1 (fc50f45a/289122cd); 3/3 deployments matched | 478 [424, 491.5] (3/3) | 369 [293, 619] (27/27) | 368 [324, 508.5] (3/3) | 1:9/9; 2:9/9; 3:9/9 | 676.4 | complete |
| 50 | control | control@4.0.1 (deabe970/289122cd); 3/3 deployments matched | 474 [464, 490.5] (3/3) | 548 [382, 592] (27/27) | 548 [455.5, 576] (3/3) | 1:9/9; 2:9/9; 3:9/9 | 626.1 | complete |
| 50 | pinned | pinned@4.0.0 (c823d174/289122cd); 3/3 deployments matched | 414 [355, 462.5] (3/3) | 403 [393, 517.5] (27/27) | 403 [362.5, 469.5] (3/3) | 1:9/9; 2:9/9; 3:9/9 | 556.9 | complete |
| 50 | pi | pi 1.0.4 / no Effect (5526e585/866f1f7f); 3/3 deployments matched | 468 [427, 473] (3/3) | 371 [295, 521] (27/27) | 371 [332, 452] (3/3) | 1:9/9; 2:9/9; 3:9/9 | 606.3 | complete |
| 50 | tardie | tardie@4.0.0-rc.115 (c8fbbb72/8a0cb11b); 3/3 deployments matched | 480 [417.5, 484] (3/3) | 563 [313, 592] (27/27) | 569 [423, 579] (3/3) | 1:9/9; 2:9/9; 3:9/9 | 650.7 | complete |
| 250 | base | base@4.0.1 (deabe970/289122cd); base@4.0.1 (deabe970/6b2bef15); 3/3 deployments matched | 423 [395.5, 472.5] (3/3) | 392 [294, 478] (27/27) | 392 [341.5, 433] (3/3) | 0:9/9; 1:9/9; 2:9/9 | 584.5 | complete |
| 250 | head | head@4.0.1 (fc50f45a/289122cd); head@4.0.1 (fc50f45a/6b2bef15); 3/3 deployments matched | 421 [408.5, 455] (3/3) | 518 [417, 553.5] (27/27) | 525 [466, 531] (3/3) | 0:9/9; 1:9/9; 2:9/9 | 582.4 | complete |
| 250 | control | control@4.0.1 (deabe970/289122cd); control@4.0.1 (deabe970/6b2bef15); 3/3 deployments matched | 409 [383.5, 418] (3/3) | 286 [275, 340] (27/27) | 280 [274, 310.5] (3/3) | 0:9/9; 1:9/9; 2:9/9 | 355.3 | complete |
| 250 | pinned | pinned@4.0.0 (c823d174/289122cd); pinned@4.0.0 (c823d174/6b2bef15); 3/3 deployments matched | 536 [476.5, 541] (3/3) | 600 [554.5, 637.5] (27/27) | 587 [570.5, 612.5] (3/3) | 0:9/9; 1:9/9; 2:9/9 | 705.5 | complete |
| 250 | pi | pi 1.0.4 / no Effect (5526e585/866f1f7f); pi 1.0.4 / no Effect (5526e585/f1947b8f); 3/3 deployments matched | 457 [448.5, 457] (3/3) | 365 [359.5, 369] (27/27) | 364 [362, 366.5] (3/3) | 0:9/9; 1:9/9; 2:9/9 | 386.9 | complete |
| 250 | tardie | tardie@4.0.0-rc.115 (c8fbbb72/8a0cb11b); tardie@4.0.0-rc.115 (c8fbbb72/9f22f5b1); 3/3 deployments matched | 362 [360, 453.5] (3/3) | 477 [429, 695.5] (27/27) | 477 [447.5, 595] (3/3) | 0:9/9; 1:9/9; 2:9/9 | 727.4 | complete |
| 1000 | base | base@4.0.1 (deabe970/6b2bef15); 3/3 deployments matched | 275 [251.5, 411.5] (3/3) | 139 [129, 545] (27/27) | 137 [131.5, 351.5] (3/3) | 0:9/9; 1:9/9; 2:9/9 | 578.1 | complete |
| 1000 | head | head@4.0.1 (fc50f45a/6b2bef15); 3/3 deployments matched | 293 [227.5, 333] (3/3) | 287 [69.5, 313] (27/27) | 287 [174, 306] (3/3) | 0:9/9; 1:9/9; 2:9/9 | 455.7 | complete |
| 1000 | control | control@4.0.1 (deabe970/6b2bef15); 3/3 deployments matched | 252 [221.5, 298.5] (3/3) | 168 [118, 465.5] (27/27) | 168 [138.5, 328] (3/3) | 0:9/9; 1:9/9; 2:9/9 | 512.5 | complete |
| 1000 | pinned | pinned@4.0.0 (c823d174/6b2bef15); 3/3 deployments matched | 324 [263, 350.5] (3/3) | 153 [95, 380.5] (27/27) | 153 [116, 272] (3/3) | 0:9/9; 1:9/9; 2:9/9 | 488.3 | complete |
| 1000 | pi | pi 1.0.4 / no Effect (5526e585/f1947b8f); 3/3 deployments matched | 394 [357, 472.5] (3/3) | 211 [182, 407] (27/27) | 199 [190, 304.5] (3/3) | 0:9/9; 1:9/9; 2:9/9 | 438 | complete |
| 1000 | tardie | tardie@4.0.0-rc.115 (c8fbbb72/9f22f5b1); 3/3 deployments matched | 284 [227, 309] (3/3) | 277 [216, 382.5] (27/27) | 277 [237, 332] (3/3) | 0:9/9; 1:9/9; 2:9/9 | 438.3 | complete |

| Size | Pair | Cold ratio median [IQR] (pairs/3) | Control/base cold spread | Object-median ratio median [IQR] (pairs/3) | Control/base Object spread | Paired-turn ratio median [IQR] (turn pairs/27; diagnostic) | Control/base turn spread | Status |
|---:|---|---:|---:|---:|---:|---:|---:|---|
| 50 | head/base | 0.931734 [0.851284, 0.976551] (3/3) | 0.945833 [0.910186, 1.01458] (3/3); range [0.874539, 1.08333] | 0.931133 [0.709045, 0.947242] (3/3) | 0.950262 [0.908416, 0.951653] (3/3); range [0.866571, 0.953043] | 0.939815 [0.528087, 0.962147] (27/27) | 0.9399 [0.911401, 0.964189] (27/27); range [0.838068, 1.13718] | complete |
| 50 | control/base | 0.945833 [0.910186, 1.01458] (3/3) | 0.945833 [0.910186, 1.01458] (3/3); range [0.874539, 1.08333] | 0.950262 [0.908416, 0.951653] (3/3) | 0.950262 [0.908416, 0.951653] (3/3); range [0.866571, 0.953043] | 0.9399 [0.911401, 0.964189] (27/27) | 0.9399 [0.911401, 0.964189] (27/27); range [0.838068, 1.13718] | complete |
| 50 | head/control | 0.942801 [0.878889, 1.0041] (3/3) | 0.945833 [0.910186, 1.01458] (3/3); range [0.874539, 1.08333] | 1.01377 [0.762362, 1.04414] (3/3) | 0.950262 [0.908416, 0.951653] (3/3); range [0.866571, 0.953043] | 1.0027 [0.523426, 1.03183] (27/27) | 0.9399 [0.911401, 0.964189] (27/27); range [0.838068, 1.13718] | complete |
| 50 | head/pi | 1 [0.895299, 1.15415] (3/3) | 0.945833 [0.910186, 1.01458] (3/3); range [0.874539, 1.08333] | 0.991914 [0.758621, 1.60347] (3/3) | 0.950262 [0.908416, 0.951653] (3/3); range [0.866571, 0.953043] | 0.991914 [0.543216, 2.09842] (27/27) | 0.9399 [0.911401, 0.964189] (27/27); range [0.838068, 1.13718] | complete |
| 50 | head/tardie | 1.03484 [0.902835, 1.19066] (3/3) | 0.945833 [0.910186, 1.01458] (3/3); range [0.874539, 1.08333] | 1.10187 [0.796979, 1.21519] (3/3) | 0.950262 [0.908416, 0.951653] (3/3); range [0.866571, 0.953043] | 1.1 [0.499036, 1.18877] (27/27) | 0.9399 [0.911401, 0.964189] (27/27); range [0.838068, 1.13718] | complete |
| 50 | pinned/pi | 0.884615 [0.751931, 1.10422] (3/3) | 0.945833 [0.910186, 1.01458] (3/3); range [0.874539, 1.08333] | 1.00563 [0.936777, 1.19053] (3/3) | 0.950262 [0.908416, 0.951653] (3/3); range [0.866571, 0.953043] | 1.01845 [0.897997, 1.35641] (27/27) | 0.9399 [0.911401, 0.964189] (27/27); range [0.838068, 1.13718] | complete |
| 50 | pinned/tardie | 0.8625 [0.848151, 0.954816] (3/3) | 0.945833 [0.910186, 1.01458] (3/3); range [0.874539, 1.08333] | 0.942004 [0.813107, 1.05223] (3/3) | 0.950262 [0.908416, 0.951653] (3/3); range [0.866571, 0.953043] | 0.928952 [0.690549, 1.08806] (27/27) | 0.9399 [0.911401, 0.964189] (27/27); range [0.838068, 1.13718] | complete |
| 250 | head/base | 0.936782 [0.936476, 1.0404] (3/3) | 0.846336 [0.81493, 1.00333] (3/3); range [0.783525, 1.16033] | 1.10759 [1.07293, 1.47648] (3/3) | 0.719409 [0.716847, 0.820186] (3/3); range [0.714286, 0.920962] | 1.15447 [1.04973, 1.80641] (27/27) | 0.744283 [0.723236, 0.904594] (27/27); range [0.429467, 1.189] | complete |
| 250 | control/base | 0.846336 [0.81493, 1.00333] (3/3) | 0.846336 [0.81493, 1.00333] (3/3); range [0.783525, 1.16033] | 0.719409 [0.716847, 0.820186] (3/3) | 0.719409 [0.716847, 0.820186] (3/3); range [0.714286, 0.920962] | 0.744283 [0.723236, 0.904594] (27/27) | 0.744283 [0.723236, 0.904594] (27/27); range [0.429467, 1.189] | complete |
| 250 | head/control | 1.10615 [1.04605, 1.15087] (3/3) | 0.846336 [0.81493, 1.00333] (3/3); range [0.783525, 1.16033] | 1.53959 [1.49658, 1.77166] (3/3) | 0.719409 [0.716847, 0.820186] (3/3); range [0.714286, 0.920962] | 1.54438 [1.46983, 1.81114] (27/27) | 0.744283 [0.723236, 0.904594] (27/27); range [0.429467, 1.189] | complete |
| 250 | head/pi | 0.956818 [0.911669, 1.01342] (3/3) | 0.846336 [0.81493, 1.00333] (3/3); range [0.783525, 1.16033] | 1.45833 [1.28066, 1.4668] (3/3) | 0.719409 [0.716847, 0.820186] (3/3); range [0.714286, 0.920962] | 1.42308 [1.13483, 1.5305] (27/27) | 0.744283 [0.723236, 0.904594] (27/27); range [0.429467, 1.189] | complete |
| 250 | head/tardie | 1.10615 [0.939311, 1.22849] (3/3) | 0.846336 [0.81493, 1.00333] (3/3); range [0.783525, 1.16033] | 0.973684 [0.855005, 1.04974] (3/3) | 0.719409 [0.716847, 0.820186] (3/3); range [0.714286, 0.920962] | 0.973684 [0.792942, 1.10422] (27/27) | 0.744283 [0.723236, 0.904594] (27/27); range [0.429467, 1.189] | complete |
| 250 | pinned/pi | 1.17287 [1.0603, 1.18381] (3/3) | 0.846336 [0.81493, 1.00333] (3/3); range [0.783525, 1.16033] | 1.63056 [1.57627, 1.67978] (3/3) | 0.719409 [0.716847, 0.820186] (3/3); range [0.714286, 0.920962] | 1.63753 [1.50116, 1.72302] (27/27) | 0.744283 [0.723236, 0.904594] (27/27); range [0.429467, 1.189] | complete |
| 250 | pinned/tardie | 1.49721 [1.13117, 1.50275] (3/3) | 0.846336 [0.81493, 1.00333] (3/3); range [0.783525, 1.16033] | 1.16143 [0.992354, 1.34387] (3/3) | 0.719409 [0.716847, 0.820186] (3/3); range [0.714286, 0.920962] | 1.15417 [0.842541, 1.48944] (27/27) | 0.744283 [0.723236, 0.904594] (27/27); range [0.429467, 1.189] | complete |
| 1000 | head/base | 0.710526 [0.695592, 0.88799] (3/3) | 0.837719 [0.648787, 1.04613] (3/3); range [0.459854, 1.25455] | 0.507067 [0.476161, 1.54322] (3/3) | 0.79562 [0.54622, 2.33432] (3/3); range [0.29682, 3.87302] | 0.528986 [0.485936, 2.29984] (27/27) | 0.820144 [0.30809, 3.50699] (27/27); range [0.275862, 4.27731] | complete |
| 1000 | control/base | 0.837719 [0.648787, 1.04613] (3/3) | 0.837719 [0.648787, 1.04613] (3/3); range [0.459854, 1.25455] | 0.79562 [0.54622, 2.33432] (3/3) | 0.79562 [0.54622, 2.33432] (3/3); range [0.29682, 3.87302] | 0.820144 [0.30809, 3.50699] (27/27) | 0.820144 [0.30809, 3.50699] (27/27); range [0.275862, 4.27731] | complete |
| 1000 | head/control | 0.849275 [0.848721, 1.16472] (3/3) | 0.837719 [0.648787, 1.04613] (3/3); range [0.459854, 1.25455] | 0.665984 [0.612808, 1.18716] (3/3) | 0.79562 [0.54622, 2.33432] (3/3); range [0.29682, 3.87302] | 0.659619 [0.601933, 1.65328] (27/27) | 0.820144 [0.30809, 3.50699] (27/27); range [0.275862, 4.27731] | complete |
| 1000 | head/pi | 0.53176 [0.519005, 0.73923] (3/3) | 0.837719 [0.648787, 1.04613] (3/3); range [0.459854, 1.25455] | 0.792683 [0.549608, 1.18916] (3/3) | 0.79562 [0.54622, 2.33432] (3/3); range [0.29682, 3.87302] | 0.794621 [0.348781, 1.4957] (27/27) | 0.820144 [0.30809, 3.50699] (27/27); range [0.275862, 4.27731] | complete |
| 1000 | head/tardie | 1.11677 [0.843595, 1.42015] (3/3) | 0.837719 [0.648787, 1.04613] (3/3); range [0.459854, 1.25455] | 1.17329 [0.665454, 1.31507] (3/3) | 0.79562 [0.54622, 2.33432] (3/3); range [0.29682, 3.87302] | 1.12635 [0.172573, 1.47354] (27/27) | 0.820144 [0.30809, 3.50699] (27/27); range [0.275862, 4.27731] | complete |
| 1000 | pinned/pi | 0.822335 [0.594471, 1.00023] (3/3) | 0.837719 [0.648787, 1.04613] (3/3); range [0.459854, 1.25455] | 0.768844 [0.480764, 1.46453] (3/3) | 0.79562 [0.54622, 2.33432] (3/3); range [0.29682, 3.87302] | 0.825581 [0.232311, 2.03274] (27/27) | 0.820144 [0.30809, 3.50699] (27/27); range [0.275862, 4.27731] | complete |
| 1000 | pinned/tardie | 1.18824 [1.07915, 1.25785] (3/3) | 0.837719 [0.648787, 1.04613] (3/3); range [0.459854, 1.25455] | 0.395349 [0.340274, 1.19006] (3/3) | 0.79562 [0.54622, 2.33432] (3/3); range [0.29682, 3.87302] | 0.404762 [0.353229, 1.90945] (27/27) | 0.820144 [0.30809, 3.50699] (27/27); range [0.275862, 4.27731] | complete |

## RPC referenceDoWallMs (ms reference DO wall)

Object-median eligibility: at least 7 of 9 warm observations. Per-cluster counts below retain the sample ID and mark ineligible medians.

| Size | Role | Deployed build set | Cold median [p25, p75] (clusters/3) | README warm median [p25, p75] (turns/27) | Object-median median [p25, p75] (eligible clusters/3) | Warm n per cluster | Warm-turn p95 | Coverage |
|---:|---|---|---:|---:|---:|---|---:|---|
| 50 | base | base@4.0.1 (deabe970/289122cd); 3/3 deployments matched | 310.5 [291.75, 329.25] (2/3) | 379 [190.5, 447.5] (27/27) | 379 [283.5, 436] (3/3) | 1:9/9; 2:9/9; 3:9/9 | 508.7 | incomplete |
| 50 | head | head@4.0.1 (fc50f45a/289122cd); 3/3 deployments matched | 297 [265, 301] (3/3) | 185 [151.5, 423] (27/27) | 185 [166, 319] (3/3) | 1:9/9; 2:9/9; 3:9/9 | 481.1 | complete |
| 50 | control | control@4.0.1 (deabe970/289122cd); 3/3 deployments matched | 294 [279.5, 301] (3/3) | 348 [197.5, 410] (27/27) | 348 [266.5, 382.5] (3/3) | 1:9/9; 2:9/9; 3:9/9 | 435 | complete |
| 50 | pinned | pinned@4.0.0 (c823d174/289122cd); 3/3 deployments matched | 284 [258, 304] (3/3) | 265 [214, 394] (25/27) | 265 [238, 337] (3/3) | 1:8/9; 2:8/9; 3:9/9 | 428.2 | incomplete |
| 50 | pi | pi 1.0.4 / no Effect (5526e585/866f1f7f); 3/3 deployments matched | 272 [263, 276.5] (3/3) | 187 [170, 338] (24/27) | 192 [180, 271.75] (3/3) | 1:9/9; 2:8/9; 3:7/9 | 422.8 | incomplete |
| 50 | tardie | tardie@4.0.0-rc.115 (c8fbbb72/8a0cb11b); 3/3 deployments matched | 274 [253.5, 290] (3/3) | 365 [153, 406] (25/27) | 365 [256.5, 386] (3/3) | 1:8/9; 2:9/9; 3:8/9 | 431.4 | incomplete |
| 250 | base | base@4.0.1 (deabe970/289122cd); base@4.0.1 (deabe970/6b2bef15); 3/3 deployments matched | 295 [270, 316.5] (3/3) | 204 [165.5, 324.25] (26/27) | 203 [180.75, 273] (3/3) | 0:9/9; 1:8/9; 2:9/9 | 354.5 | incomplete |
| 250 | head | head@4.0.1 (fc50f45a/289122cd); head@4.0.1 (fc50f45a/6b2bef15); 3/3 deployments matched | 288 [281.5, 295.5] (3/3) | 395 [299.5, 421] (23/27) | 410.5 [400.25, 420.75] (2/3) | 0:9/9; 1:8/9; 2:6/9 ineligible | 463.8 | incomplete |
| 250 | control | control@4.0.1 (deabe970/289122cd); control@4.0.1 (deabe970/6b2bef15); 3/3 deployments matched | 268.5 [259.25, 277.75] (2/3) | 157.5 [151.75, 161.25] (16/27) | 153 [153, 153] (1/3) | 0:6/9 ineligible; 1:1/9 ineligible; 2:9/9 | 162.5 | incomplete |
| 250 | pinned | pinned@4.0.0 (c823d174/289122cd); pinned@4.0.0 (c823d174/6b2bef15); 3/3 deployments matched | 359 [321.5, 380.5] (3/3) | 449.5 [417.75, 464] (26/27) | 450.5 [425.25, 451.25] (3/3) | 0:9/9; 1:9/9; 2:8/9 | 521.75 | incomplete |
| 250 | pi | pi 1.0.4 / no Effect (5526e585/866f1f7f); pi 1.0.4 / no Effect (5526e585/f1947b8f); 3/3 deployments matched | 259 [255.5, 266.5] (3/3) | 176 [170, 179.5] (27/27) | 178 [170, 178.5] (3/3) | 0:9/9; 1:9/9; 2:9/9 | 186.8 | complete |
| 250 | tardie | tardie@4.0.0-rc.115 (c8fbbb72/8a0cb11b); tardie@4.0.0-rc.115 (c8fbbb72/9f22f5b1); 3/3 deployments matched | 249.5 [242.75, 256.25] (2/3) | 357 [323.75, 474] (24/27) | 357 [326.75, 440.5] (3/3) | 0:7/9; 1:9/9; 2:8/9 | 537.75 | incomplete |
| 1000 | base | base@4.0.1 (deabe970/6b2bef15); 3/3 deployments matched | 149 [148, 221.5] (3/3) | 64 [59, 351.5] (27/27) | 63 [60, 210.5] (3/3) | 0:9/9; 1:9/9; 2:9/9 | 382.7 | complete |
| 1000 | head | head@4.0.1 (fc50f45a/6b2bef15); 3/3 deployments matched | 143.5 [132.75, 154.25] (2/3) | 154.5 [47.25, 250.75] (24/27) | 154 [95.5, 208.75] (3/3) | 0:9/9; 1:8/9; 2:7/9 | 366.4 | incomplete |
| 1000 | control | control@4.0.1 (deabe970/6b2bef15); 3/3 deployments matched | 163 [157.5, 203] (3/3) | 83 [73, 368] (25/27) | 83 [73, 235.5] (3/3) | 0:8/9; 1:8/9; 2:9/9 | 411.6 | incomplete |
| 1000 | pinned | pinned@4.0.0 (c823d174/6b2bef15); 3/3 deployments matched | 193 [186, 221] (3/3) | 83 [47, 305.5] (27/27) | 83 [64, 204] (3/3) | 0:9/9; 1:9/9; 2:9/9 | 329.8 | complete |
| 1000 | pi | pi 1.0.4 / no Effect (5526e585/f1947b8f); 3/3 deployments matched | 200 [199.5, 283] (3/3) | 79 [51, 209.5] (27/27) | 79 [64, 146.5] (3/3) | 0:9/9; 1:9/9; 2:9/9 | 231 | complete |
| 1000 | tardie | tardie@4.0.0-rc.115 (c8fbbb72/9f22f5b1); 3/3 deployments matched | 195 [169.5, 215.5] (3/3) | 238.5 [116.25, 298.5] (26/27) | 240 [171.5, 271.25] (3/3) | 0:9/9; 1:9/9; 2:8/9 | 336.5 | incomplete |

| Size | Pair | Cold ratio median [IQR] (pairs/3) | Control/base cold spread | Object-median ratio median [IQR] (pairs/3) | Control/base Object spread | Paired-turn ratio median [IQR] (turn pairs/27; diagnostic) | Control/base turn spread | Status |
|---:|---|---:|---:|---:|---:|---:|---:|---|
| 50 | head/base | 0.982174 [0.929306, 1.03504] (2/3) | 0.986516 [0.915672, 1.05736] (2/3); range [0.844828, 1.12821] | 0.918864 [0.653363, 0.951453] (3/3) | 0.918206 [0.882024, 0.951124] (3/3); range [0.845842, 0.984043] | 0.930337 [0.408935, 0.996083] (27/27) | 0.930295 [0.861224, 0.984043] (27/27); range [0.787402, 1.18207] | incomplete |
| 50 | control/base | 0.986516 [0.915672, 1.05736] (2/3) | 0.986516 [0.915672, 1.05736] (2/3); range [0.844828, 1.12821] | 0.918206 [0.882024, 0.951124] (3/3) | 0.918206 [0.882024, 0.951124] (3/3); range [0.845842, 0.984043] | 0.930295 [0.861224, 0.984043] (27/27) | 0.930295 [0.861224, 0.984043] (27/27); range [0.787402, 1.18207] | incomplete |
| 50 | head/control | 0.964286 [0.921765, 1.00085] (3/3) | 0.986516 [0.915672, 1.05736] (2/3); range [0.844828, 1.12821] | 1 [0.711207, 1.04317] (3/3) | 0.918206 [0.882024, 0.951124] (3/3); range [0.845842, 0.984043] | 0.981043 [0.432277, 1.0178] (27/27) | 0.930295 [0.861224, 0.984043] (27/27); range [0.787402, 1.18207] | incomplete |
| 50 | head/pi | 1.05694 [0.956779, 1.12886] (3/3) | 0.986516 [0.915672, 1.05736] (2/3); range [0.844828, 1.12821] | 0.963542 [0.690875, 1.82999] (3/3) | 0.918206 [0.882024, 0.951124] (3/3); range [0.845842, 0.984043] | 1.00565 [0.429733, 2.57269] (24/27) | 0.928481 [0.853613, 0.984043] (24/27); range [0.787402, 1.18207] | incomplete |
| 50 | head/tardie | 0.996732 [0.923548, 1.13571] (3/3) | 0.986516 [0.915672, 1.05736] (2/3); range [0.844828, 1.12821] | 1.11302 [0.757881, 1.18151] (3/3) | 0.918206 [0.882024, 0.951124] (3/3); range [0.845842, 0.984043] | 1.07882 [0.404494, 1.23415] (25/27) | 0.926667 [0.854617, 0.984043] (25/27); range [0.787402, 1.18207] | incomplete |
| 50 | pinned/pi | 1.04412 [0.93487, 1.15985] (3/3) | 0.986516 [0.915672, 1.05736] (2/3); range [0.844828, 1.12821] | 1.25595 [1.20977, 1.31808] (3/3) | 0.918206 [0.882024, 0.951124] (3/3); range [0.845842, 0.984043] | 1.27559 [1.19669, 1.3229] (22/27) | 0.928481 [0.851606, 0.984043] (22/27); range [0.787402, 1.18207] | incomplete |
| 50 | pinned/tardie | 1.0365 [1.0161, 1.04766] (3/3) | 0.986516 [0.915672, 1.05736] (2/3); range [0.844828, 1.12821] | 1.12055 [0.819488, 1.45554] (3/3) | 0.918206 [0.882024, 0.951124] (3/3); range [0.845842, 0.984043] | 1.08791 [0.548982, 1.73963] (23/27) | 0.926667 [0.85261, 0.984043] (23/27); range [0.787402, 1.18207] | incomplete |
| 250 | head/base | 0.932203 [0.914327, 1.05386] (3/3) | 0.93476 [0.891936, 0.977584] (2/3); range [0.849112, 1.02041] | 1.92813 [1.53258, 2.32369] (2/3) | unavailable [unavailable, unavailable] (0/3); range [unavailable, unavailable] | 1.18315 [1.0734, 2.54632] (22/27) | 0.741463 [0.493865, 0.753695] (13/27); range [0.441926, 0.84] | incomplete |
| 250 | control/base | 0.93476 [0.891936, 0.977584] (2/3) | 0.93476 [0.891936, 0.977584] (2/3); range [0.849112, 1.02041] | 0.753695 [0.753695, 0.753695] (1/3) | 0.753695 [0.753695, 0.753695] (1/3); range [0.753695, 0.753695] | 0.744494 [0.504344, 0.7612] (16/27) | 0.744494 [0.504344, 0.7612] (16/27); range [0.441926, 0.84] | incomplete |
| 250 | head/control | 1.10387 [1.07981, 1.12794] (2/3) | 0.93476 [0.891936, 0.977584] (2/3); range [0.849112, 1.02041] | unavailable [unavailable, unavailable] (0/3) | unavailable [unavailable, unavailable] (0/3); range [unavailable, unavailable] | 2.35185 [1.33987, 2.44872] (13/27) | 0.741463 [0.493865, 0.753695] (13/27); range [0.441926, 0.84] | incomplete |
| 250 | head/pi | 1.10584 [1.09855, 1.1089] (3/3) | 0.93476 [0.891936, 0.977584] (2/3); range [0.849112, 1.02041] | 2.41963 [2.2992, 2.54006] (2/3) | unavailable [unavailable, unavailable] (0/3); range [unavailable, unavailable] | 2.24339 [1.65655, 2.49935] (23/27) | 0.741463 [0.493865, 0.753695] (13/27); range [0.441926, 0.84] | incomplete |
| 250 | head/tardie | 1.15867 [1.15538, 1.16196] (2/3) | 0.849112 [0.849112, 0.849112] (1/3); range [0.849112, 0.849112] | 0.975779 [0.860027, 1.09153] (2/3) | unavailable [unavailable, unavailable] (0/3); range [unavailable, unavailable] | 0.798095 [0.722222, 1.18976] (21/27) | 0.744494 [0.504344, 0.759852] (12/27); range [0.441926, 0.84] | incomplete |
| 250 | pinned/pi | 1.31022 [1.20337, 1.45273] (3/3) | 0.93476 [0.891936, 0.977584] (2/3); range [0.849112, 1.02041] | 2.52514 [2.49714, 2.52802] (3/3) | 0.753695 [0.753695, 0.753695] (1/3); range [0.753695, 0.753695] | 2.52253 [2.42548, 2.66877] (26/27) | 0.741463 [0.500851, 0.762966] (15/27); range [0.441926, 0.84] | incomplete |
| 250 | pinned/tardie | 1.5342 [1.44961, 1.6188] (2/3) | 0.849112 [0.849112, 0.849112] (1/3); range [0.849112, 0.849112] | 1.12045 [0.991522, 1.31992] (3/3) | 0.753695 [0.753695, 0.753695] (1/3); range [0.753695, 0.753695] | 1.1459 [0.937183, 1.49696] (23/27) | 0.747525 [0.507837, 0.766497] (13/27); range [0.441926, 0.84] | incomplete |
| 1000 | head/base | 0.968657 [0.899295, 1.03802] (2/3) | 1.36986 [1.23935, 1.50037] (2/3); range [1.10884, 1.63087] | 0.649123 [0.539645, 2.41583] (3/3) | 1.45614 [0.816059, 3.80744] (3/3); range [0.175978, 6.15873] | 0.673077 [0.447016, 3.91797] (24/27) | 1.43656 [0.189315, 5.42839] (22/27); range [0.156658, 6.91803] | incomplete |
| 1000 | control/base | 1.10884 [0.812925, 1.36986] (3/3) | 1.10884 [0.812925, 1.36986] (3/3); range [0.517007, 1.63087] | 1.45614 [0.816059, 3.80744] (3/3) | 1.45614 [0.816059, 3.80744] (3/3); range [0.175978, 6.15873] | 1.5283 [0.204188, 5.43421] (25/27) | 1.5283 [0.204188, 5.43421] (25/27); range [0.156658, 6.91803] | incomplete |
| 1000 | head/control | 0.713739 [0.696376, 0.731103] (2/3) | 1.36986 [1.23935, 1.50037] (2/3); range [1.10884, 1.63087] | 0.679124 [0.562453, 1.56178] (3/3) | 1.45614 [0.816059, 3.80744] (3/3); range [0.175978, 6.15873] | 0.832414 [0.570513, 2.37189] (22/27) | 1.43656 [0.189315, 5.42839] (22/27); range [0.156658, 6.91803] | incomplete |
| 1000 | head/pi | 0.531942 [0.491381, 0.572504] (2/3) | 1.36986 [1.23935, 1.50037] (2/3); range [1.10884, 1.63087] | 1.23131 [0.849831, 2.18708] (3/3) | 1.45614 [0.816059, 3.80744] (3/3); range [0.175978, 6.15873] | 1.43547 [0.587931, 3.12395] (24/27) | 1.43656 [0.189315, 5.42839] (22/27); range [0.156658, 6.91803] | incomplete |
| 1000 | head/tardie | 0.885737 [0.755689, 1.01579] (2/3) | 1.36986 [1.23935, 1.50037] (2/3); range [1.10884, 1.63087] | 1.09792 [0.610115, 1.29653] (3/3) | 1.45614 [0.816059, 3.80744] (3/3); range [0.175978, 6.15873] | 1.19016 [0.512601, 1.54071] (23/27) | 1.5283 [0.184358, 5.51563] (21/27); range [0.156658, 6.91803] | incomplete |
| 1000 | pinned/pi | 0.965 [0.727036, 1.10813] (3/3) | 1.10884 [0.812925, 1.36986] (3/3); range [0.517007, 1.63087] | 1.05063 [0.630457, 3.84164] (3/3) | 1.45614 [0.816059, 3.80744] (3/3); range [0.175978, 6.15873] | 0.987805 [0.225431, 6.06481] (27/27) | 1.5283 [0.204188, 5.43421] (25/27); range [0.156658, 6.91803] | incomplete |
| 1000 | pinned/tardie | 1.24306 [1.03043, 1.25999] (3/3) | 1.10884 [0.812925, 1.36986] (3/3); range [0.517007, 1.63087] | 0.27438 [0.23094, 1.71486] (3/3) | 1.45614 [0.816059, 3.80744] (3/3); range [0.175978, 6.15873] | 0.266366 [0.197962, 2.78588] (26/27) | 1.54485 [0.199231, 5.45456] (24/27); range [0.156658, 6.91803] | incomplete |

## RPC referenceCpuTimeMs (ms reference CPU)

Object-median eligibility: at least 7 of 9 warm observations. Per-cluster counts below retain the sample ID and mark ineligible medians.

| Size | Role | Deployed build set | Cold median [p25, p75] (clusters/3) | README warm median [p25, p75] (turns/27) | Object-median median [p25, p75] (eligible clusters/3) | Warm n per cluster | Warm-turn p95 | Coverage |
|---:|---|---|---:|---:|---:|---|---:|---|
| 50 | base | base@4.0.1 (deabe970/289122cd); 3/3 deployments matched | 154 [139, 169] (2/3) | 215 [34, 272] (27/27) | 215 [123, 270] (3/3) | 1:9/9; 2:9/9; 3:9/9 | 336.8 | incomplete |
| 50 | head | head@4.0.1 (fc50f45a/289122cd); 3/3 deployments matched | 142 [131.5, 149.5] (3/3) | 33 [30.5, 265.5] (27/27) | 32 [31, 155] (3/3) | 1:9/9; 2:9/9; 3:9/9 | 319 | complete |
| 50 | control | control@4.0.1 (deabe970/289122cd); 3/3 deployments matched | 134 [126, 144.5] (3/3) | 198 [45, 263.5] (27/27) | 198 [115, 232] (3/3) | 1:9/9; 2:9/9; 3:9/9 | 292.9 | complete |
| 50 | pinned | pinned@4.0.0 (c823d174/289122cd); 3/3 deployments matched | 167 [163, 168.5] (3/3) | 202 [43, 294] (25/27) | 202 [120.75, 249.25] (3/3) | 1:8/9; 2:8/9; 3:9/9 | 323 | incomplete |
| 50 | pi | pi 1.0.4 / no Effect (5526e585/866f1f7f); 3/3 deployments matched | 118 [116.5, 120.5] (3/3) | 38.5 [29, 192.75] (24/27) | 30 [30, 115.75] (3/3) | 1:9/9; 2:8/9; 3:7/9 | 277.8 | incomplete |
| 50 | tardie | tardie@4.0.0-rc.115 (c8fbbb72/8a0cb11b); 3/3 deployments matched | 119 [116.5, 126.5] (3/3) | 195 [33, 238] (25/27) | 195 [113.75, 215.75] (3/3) | 1:8/9; 2:9/9; 3:8/9 | 257.4 | incomplete |
| 250 | base | base@4.0.1 (deabe970/289122cd); base@4.0.1 (deabe970/6b2bef15); 3/3 deployments matched | 179 [153.5, 179.5] (3/3) | 43.5 [41, 198] (26/27) | 43.5 [42.25, 134.75] (3/3) | 0:9/9; 1:8/9; 2:9/9 | 234.25 | incomplete |
| 250 | head | head@4.0.1 (fc50f45a/289122cd); head@4.0.1 (fc50f45a/6b2bef15); 3/3 deployments matched | 168 [143, 172] (3/3) | 267 [151, 293.5] (23/27) | 285.75 [275.875, 295.625] (2/3) | 0:9/9; 1:8/9; 2:6/9 ineligible | 336.6 | incomplete |
| 250 | control | control@4.0.1 (deabe970/289122cd); control@4.0.1 (deabe970/6b2bef15); 3/3 deployments matched | 147 [131, 163] (2/3) | 39 [29, 40.25] (16/27) | 40 [40, 40] (1/3) | 0:6/9 ineligible; 1:1/9 ineligible; 2:9/9 | 47 | incomplete |
| 250 | pinned | pinned@4.0.0 (c823d174/289122cd); pinned@4.0.0 (c823d174/6b2bef15); 3/3 deployments matched | 165 [159.5, 167.5] (3/3) | 293.5 [271, 325] (26/27) | 273 [272.25, 304] (3/3) | 0:9/9; 1:9/9; 2:8/9 | 343.25 | incomplete |
| 250 | pi | pi 1.0.4 / no Effect (5526e585/866f1f7f); pi 1.0.4 / no Effect (5526e585/f1947b8f); 3/3 deployments matched | 118 [116, 122.5] (3/3) | 30 [29, 38] (27/27) | 30 [29.5, 30.5] (3/3) | 0:9/9; 1:9/9; 2:9/9 | 43.7 | complete |
| 250 | tardie | tardie@4.0.0-rc.115 (c8fbbb72/8a0cb11b); tardie@4.0.0-rc.115 (c8fbbb72/9f22f5b1); 3/3 deployments matched | 142 [133.5, 150.5] (2/3) | 238.5 [211.75, 294] (24/27) | 239 [221.5, 291] (3/3) | 0:7/9; 1:9/9; 2:8/9 | 353.35 | incomplete |
| 1000 | base | base@4.0.1 (deabe970/6b2bef15); 3/3 deployments matched | 118 [117, 126] (3/3) | 31 [29, 190.5] (27/27) | 30 [29.5, 115] (3/3) | 0:9/9; 1:9/9; 2:9/9 | 224.7 | complete |
| 1000 | head | head@4.0.1 (fc50f45a/6b2bef15); 3/3 deployments matched | 115.5 [114.25, 116.75] (2/3) | 36 [30.75, 189.75] (24/27) | 33 [32, 116.5] (3/3) | 0:9/9; 1:8/9; 2:7/9 | 301.55 | incomplete |
| 1000 | control | control@4.0.1 (deabe970/6b2bef15); 3/3 deployments matched | 114 [113.5, 134] (3/3) | 31 [29, 271] (25/27) | 31 [30, 164.5] (3/3) | 0:8/9; 1:8/9; 2:9/9 | 313 | incomplete |
| 1000 | pinned | pinned@4.0.0 (c823d174/6b2bef15); 3/3 deployments matched | 172 [167, 193.5] (3/3) | 46 [41, 269] (27/27) | 46 [43.5, 159] (3/3) | 0:9/9; 1:9/9; 2:9/9 | 296.8 | complete |
| 1000 | pi | pi 1.0.4 / no Effect (5526e585/f1947b8f); 3/3 deployments matched | 192 [170.5, 196.5] (3/3) | 41 [30.5, 44] (27/27) | 42 [36, 42.5] (3/3) | 0:9/9; 1:9/9; 2:9/9 | 46.7 | complete |
| 1000 | tardie | tardie@4.0.0-rc.115 (c8fbbb72/9f22f5b1); 3/3 deployments matched | 163 [141.5, 165] (3/3) | 210 [38, 264.75] (26/27) | 210 [123, 239] (3/3) | 0:9/9; 1:9/9; 2:8/9 | 304.5 | incomplete |

| Size | Pair | Cold ratio median [IQR] (pairs/3) | Control/base cold spread | Object-median ratio median [IQR] (pairs/3) | Control/base Object spread | Paired-turn ratio median [IQR] (turn pairs/27; diagnostic) | Control/base turn spread | Status |
|---:|---|---:|---:|---:|---:|---:|---:|---|
| 50 | head/base | 0.999211 [0.926236, 1.07219] (2/3) | 0.961518 [0.901955, 1.02108] (2/3); range [0.842391, 1.08065] | 0.855385 [0.502111, 0.911563] (3/3) | 0.92093 [0.869696, 0.976594] (3/3); range [0.818462, 1.03226] | 0.882353 [0.155316, 0.961861] (27/27) | 0.942857 [0.817661, 1.00152] (27/27); range [0.72093, 1.52941] | incomplete |
| 50 | control/base | 0.961518 [0.901955, 1.02108] (2/3) | 0.961518 [0.901955, 1.02108] (2/3); range [0.842391, 1.08065] | 0.92093 [0.869696, 0.976594] (3/3) | 0.92093 [0.869696, 0.976594] (3/3); range [0.818462, 1.03226] | 0.942857 [0.817661, 1.00152] (27/27) | 0.942857 [0.817661, 1.00152] (27/27); range [0.72093, 1.52941] | incomplete |
| 50 | head/control | 1.02542 [1.01916, 1.04256] (3/3) | 0.961518 [0.901955, 1.02108] (2/3); range [0.842391, 1.08065] | 0.9375 [0.549558, 0.991306] (3/3) | 0.92093 [0.869696, 0.976594] (3/3); range [0.818462, 1.03226] | 0.935484 [0.167949, 1.00386] (27/27) | 0.942857 [0.817661, 1.00152] (27/27); range [0.72093, 1.52941] | incomplete |
| 50 | head/pi | 1.23478 [1.10926, 1.28265] (3/3) | 0.961518 [0.901955, 1.02108] (2/3); range [0.842391, 1.08065] | 1 [0.579404, 5.13333] (3/3) | 0.92093 [0.869696, 0.976594] (3/3); range [0.818462, 1.03226] | 1 [0.177064, 7.45099] (24/27) | 0.942993 [0.818061, 0.985606] (24/27); range [0.72093, 1.5] | incomplete |
| 50 | head/tardie | 1.17164 [1.11652, 1.18246] (3/3) | 0.961518 [0.901955, 1.02108] (2/3); range [0.842391, 1.08065] | 0.923077 [0.54359, 1.04928] (3/3) | 0.92093 [0.869696, 0.976594] (3/3); range [0.818462, 1.03226] | 1 [0.169231, 1.15625] (25/27) | 0.941176 [0.81686, 0.969697] (25/27); range [0.72093, 1.5] | incomplete |
| 50 | pinned/pi | 1.35772 [1.35259, 1.41799] (3/3) | 0.961518 [0.901955, 1.02108] (2/3); range [0.842391, 1.08065] | 1.47146 [1.39407, 4.1024] (3/3) | 0.92093 [0.869696, 0.976594] (3/3); range [0.818462, 1.03226] | 1.42784 [1.28382, 4.05493] (22/27) | 0.935722 [0.817261, 1.01742] (22/27); range [0.72093, 1.5] | incomplete |
| 50 | pinned/tardie | 1.42857 [1.30757, 1.44674] (3/3) | 0.961518 [0.901955, 1.02108] (2/3); range [0.842391, 1.08065] | 1.52051 [0.843766, 3.86795] (3/3) | 0.92093 [0.869696, 0.976594] (3/3); range [0.818462, 1.03226] | 1.45789 [0.176155, 6.10606] (23/27) | 0.928315 [0.816764, 1.00152] (23/27); range [0.72093, 1.5] | incomplete |
| 250 | head/base | 0.933333 [0.796276, 1.15417] (3/3) | 0.949219 [0.923828, 0.974609] (2/3); range [0.898438, 1] | 4.09999 [2.63849, 5.56149] (2/3) | unavailable [unavailable, unavailable] (0/3); range [unavailable, unavailable] | 1.27069 [1.01137, 6.59633] (22/27) | 0.736842 [0.141414, 1] (13/27); range [0.122881, 1.28205] | incomplete |
| 250 | control/base | 0.949219 [0.923828, 0.974609] (2/3) | 0.949219 [0.923828, 0.974609] (2/3); range [0.898438, 1] | 0.97561 [0.97561, 0.97561] (1/3) | 0.97561 [0.97561, 0.97561] (1/3); range [0.97561, 0.97561] | 0.928571 [0.143601, 1.00641] (16/27) | 0.928571 [0.143601, 1.00641] (16/27); range [0.122881, 1.28205] | incomplete |
| 250 | head/control | 1.09483 [0.877022, 1.31263] (2/3) | 0.949219 [0.923828, 0.974609] (2/3); range [0.898438, 1] | unavailable [unavailable, unavailable] (0/3) | unavailable [unavailable, unavailable] (0/3); range [unavailable, unavailable] | 8.86667 [0.780488, 9.44828] (13/27) | 0.736842 [0.141414, 1] (13/27); range [0.122881, 1.28205] | incomplete |
| 250 | head/pi | 1.42373 [1.17643, 1.48379] (3/3) | 0.949219 [0.923828, 0.974609] (2/3); range [0.898438, 1] | 9.67787 [9.42514, 9.9306] (2/3) | unavailable [unavailable, unavailable] (0/3); range [unavailable, unavailable] | 7.65909 [3.73818, 9.39524] (23/27) | 0.736842 [0.141414, 1] (13/27); range [0.122881, 1.28205] | incomplete |
| 250 | head/tardie | 1.04307 [0.892604, 1.19353] (2/3) | 1 [1, 1] (1/3); range [1, 1] | 1.02688 [0.901193, 1.15256] (2/3) | unavailable [unavailable, unavailable] (0/3); range [unavailable, unavailable] | 0.837061 [0.20202, 1.29119] (21/27) | 0.832707 [0.143601, 1.00641] (12/27); range [0.122881, 1.28205] | incomplete |
| 250 | pinned/pi | 1.30508 [1.30215, 1.39816] (3/3) | 0.949219 [0.923828, 0.974609] (2/3); range [0.898438, 1] | 9.1 [8.92903, 10.3259] (3/3) | 0.97561 [0.97561, 0.97561] (1/3); range [0.97561, 0.97561] | 9.2069 [8.62097, 10.3678] (26/27) | 0.928571 [0.142872, 1.01282] (15/27); range [0.122881, 1.28205] | incomplete |
| 250 | pinned/tardie | 1.13487 [1.0863, 1.18343] (2/3) | 1 [1, 1] (1/3); range [1, 1] | 1.14226 [1.05947, 1.23657] (3/3) | 0.97561 [0.97561, 0.97561] (1/3); range [0.97561, 0.97561] | 1.15966 [1.04855, 1.34083] (23/27) | 0.928571 [0.14433, 1.02564] (13/27); range [0.122881, 1.28205] | incomplete |
| 1000 | head/base | 0.987434 [0.972531, 1.00234] (2/3) | 1.13961 [1.05687, 1.22235] (2/3); range [0.974138, 1.30508] | 1.1 [0.6275, 3.99828] (3/3) | 1.03333 [0.589167, 5.6546] (3/3); range [0.145, 10.2759] | 1.05 [0.181892, 6.57543] (24/27) | 1.01667 [0.153348, 8.47763] (22/27); range [0.124444, 11.6786] | incomplete |
| 1000 | control/base | 0.974138 [0.912442, 1.13961] (3/3) | 0.974138 [0.912442, 1.13961] (3/3); range [0.850746, 1.30508] | 1.03333 [0.589167, 5.6546] (3/3) | 1.03333 [0.589167, 5.6546] (3/3); range [0.145, 10.2759] | 1.03333 [0.157895, 7.53659] (25/27) | 1.03333 [0.157895, 7.53659] (25/27); range [0.124444, 11.6786] | incomplete |
| 1000 | head/control | 0.889007 [0.811387, 0.966627] (2/3) | 1.13961 [1.05687, 1.22235] (2/3); range [0.974138, 1.30508] | 1.06452 [0.867829, 1.06674] (3/3) | 1.03333 [0.589167, 5.6546] (3/3); range [0.145, 10.2759] | 1.0431 [0.754505, 1.14597] (22/27) | 1.01667 [0.153348, 8.47763] (22/27); range [0.124444, 11.6786] | incomplete |
| 1000 | head/pi | 0.677068 [0.619628, 0.734507] (2/3) | 1.13961 [1.05687, 1.22235] (2/3); range [0.974138, 1.30508] | 1.1 [0.910465, 2.93095] (3/3) | 1.03333 [0.589167, 5.6546] (3/3); range [0.145, 10.2759] | 1.06667 [0.795966, 4.19362] (24/27) | 1.01667 [0.153348, 8.47763] (22/27); range [0.124444, 11.6786] | incomplete |
| 1000 | head/tardie | 0.832797 [0.778361, 0.887232] (2/3) | 1.13961 [1.05687, 1.22235] (2/3); range [0.974138, 1.30508] | 0.861111 [0.492123, 0.906746] (3/3) | 1.03333 [0.589167, 5.6546] (3/3); range [0.145, 10.2759] | 0.885714 [0.414389, 1.01918] (23/27) | 1.03333 [0.151832, 8.86207] (21/27); range [0.124444, 11.6786] | incomplete |
| 1000 | pinned/pi | 0.855721 [0.849736, 1.14934] (3/3) | 0.974138 [0.912442, 1.13961] (3/3); range [0.850746, 1.30508] | 1.53333 [1.25476, 3.92946] (3/3) | 1.03333 [0.589167, 5.6546] (3/3); range [0.145, 10.2759] | 1.51613 [1.02439, 6.10227] (27/27) | 1.03333 [0.157895, 7.53659] (25/27); range [0.124444, 11.6786] | incomplete |
| 1000 | pinned/tardie | 1.31902 [1.14454, 1.37618] (3/3) | 0.974138 [0.912442, 1.13961] (3/3); range [0.850746, 1.30508] | 0.195238 [0.18344, 3.8754] (3/3) | 1.03333 [0.589167, 5.6546] (3/3); range [0.145, 10.2759] | 0.18882 [0.16247, 7.44956] (26/27) | 1.03452 [0.156379, 7.86796] (24/27); range [0.124444, 11.6786] | incomplete |

## HTTP processing colo by Object

HTTP colo histograms use the three-letter CF-Ray response suffix from accepted workload turn receipts in samples.json, independently of DO telemetry. Each sample identifies one Object cluster; group totals aggregate its planned samples. Missing or unparseable headers remain unavailable, separately from missing accepted receipts. Reference RPCs, preparation, rejected requests and historical attempts are excluded. CF-Ray identifies the HTTP request-processing data center, not the Durable Object's physical location or host. Different codes across paired Objects expose a client-latency routing confound; matching codes do not establish identical paths or placement. These diagnostics do not change metric eligibility, matching or ratios. [Cloudflare CF-Ray documentation](https://developers.cloudflare.com/fundamentals/reference/http-headers/#cf-ray).

| Size | Role | Sample | CF-Ray colo histogram | Accepted receipts / planned turns | Accepted receipts without a usable colo |
|---:|---|---|---|---:|---:|
| 50 | base | 1 | MAD:10 | 10/10 | 0 |
| 50 | base | 2 | MAD:10 | 10/10 | 0 |
| 50 | base | 3 | MRS:10 | 10/10 | 0 |
| 50 | base | all | MAD:20, MRS:10 | 30/30 | 0 |
| 50 | head | 1 | MAD:10 | 10/10 | 0 |
| 50 | head | 2 | KIX:10 | 10/10 | 0 |
| 50 | head | 3 | MAD:10 | 10/10 | 0 |
| 50 | head | all | KIX:10, MAD:20 | 30/30 | 0 |
| 50 | control | 1 | MAD:10 | 10/10 | 0 |
| 50 | control | 2 | MAD:10 | 10/10 | 0 |
| 50 | control | 3 | MAD:10 | 10/10 | 0 |
| 50 | control | all | MAD:30 | 30/30 | 0 |
| 50 | pinned | 1 | MAD:10 | 10/10 | 0 |
| 50 | pinned | 2 | NRT:10 | 10/10 | 0 |
| 50 | pinned | 3 | PDX:10 | 10/10 | 0 |
| 50 | pinned | all | MAD:10, NRT:10, PDX:10 | 30/30 | 0 |
| 50 | pi | 1 | KIX:10 | 10/10 | 0 |
| 50 | pi | 2 | MAD:10 | 10/10 | 0 |
| 50 | pi | 3 | MAD:10 | 10/10 | 0 |
| 50 | pi | all | KIX:10, MAD:20 | 30/30 | 0 |
| 50 | tardie | 1 | MAD:10 | 10/10 | 0 |
| 50 | tardie | 2 | MRS:10 | 10/10 | 0 |
| 50 | tardie | 3 | KIX:10 | 10/10 | 0 |
| 50 | tardie | all | KIX:10, MAD:10, MRS:10 | 30/30 | 0 |
| 250 | base | 0 | KIX:10 | 10/10 | 0 |
| 250 | base | 1 | KIX:10 | 10/10 | 0 |
| 250 | base | 2 | MRS:10 | 10/10 | 0 |
| 250 | base | all | KIX:20, MRS:10 | 30/30 | 0 |
| 250 | head | 0 | KIX:10 | 10/10 | 0 |
| 250 | head | 1 | NRT:10 | 10/10 | 0 |
| 250 | head | 2 | MRS:10 | 10/10 | 0 |
| 250 | head | all | KIX:10, MRS:10, NRT:10 | 30/30 | 0 |
| 250 | control | 0 | MRS:10 | 10/10 | 0 |
| 250 | control | 1 | KIX:10 | 10/10 | 0 |
| 250 | control | 2 | KIX:10 | 10/10 | 0 |
| 250 | control | all | KIX:20, MRS:10 | 30/30 | 0 |
| 250 | pinned | 0 | KIX:10 | 10/10 | 0 |
| 250 | pinned | 1 | KIX:10 | 10/10 | 0 |
| 250 | pinned | 2 | MRS:10 | 10/10 | 0 |
| 250 | pinned | all | KIX:20, MRS:10 | 30/30 | 0 |
| 250 | pi | 0 | MAD:10 | 10/10 | 0 |
| 250 | pi | 1 | MRS:10 | 10/10 | 0 |
| 250 | pi | 2 | MAD:10 | 10/10 | 0 |
| 250 | pi | all | MAD:20, MRS:10 | 30/30 | 0 |
| 250 | tardie | 0 | MAD:10 | 10/10 | 0 |
| 250 | tardie | 1 | NRT:10 | 10/10 | 0 |
| 250 | tardie | 2 | MIA:10 | 10/10 | 0 |
| 250 | tardie | all | MAD:10, MIA:10, NRT:10 | 30/30 | 0 |
| 1000 | base | 0 | MRS:10 | 10/10 | 0 |
| 1000 | base | 1 | DFW:10 | 10/10 | 0 |
| 1000 | base | 2 | ATL:10 | 10/10 | 0 |
| 1000 | base | all | ATL:10, DFW:10, MRS:10 | 30/30 | 0 |
| 1000 | head | 0 | NRT:10 | 10/10 | 0 |
| 1000 | head | 1 | SEA:10 | 10/10 | 0 |
| 1000 | head | 2 | SJC:10 | 10/10 | 0 |
| 1000 | head | all | NRT:10, SEA:10, SJC:10 | 30/30 | 0 |
| 1000 | control | 0 | MIA:10 | 10/10 | 0 |
| 1000 | control | 1 | MIA:10 | 10/10 | 0 |
| 1000 | control | 2 | SJC:10 | 10/10 | 0 |
| 1000 | control | all | MIA:20, SJC:10 | 30/30 | 0 |
| 1000 | pinned | 0 | SEA:10 | 10/10 | 0 |
| 1000 | pinned | 1 | SJC:10 | 10/10 | 0 |
| 1000 | pinned | 2 | DFW:10 | 10/10 | 0 |
| 1000 | pinned | all | DFW:10, SEA:10, SJC:10 | 30/30 | 0 |
| 1000 | pi | 0 | DFW:10 | 10/10 | 0 |
| 1000 | pi | 1 | MRS:10 | 10/10 | 0 |
| 1000 | pi | 2 | BOS:10 | 10/10 | 0 |
| 1000 | pi | all | BOS:10, DFW:10, MRS:10 | 30/30 | 0 |
| 1000 | tardie | 0 | ATL:10 | 10/10 | 0 |
| 1000 | tardie | 1 | SJC:10 | 10/10 | 0 |
| 1000 | tardie | 2 | ATL:10 | 10/10 | 0 |
| 1000 | tardie | all | ATL:20, SJC:10 | 30/30 | 0 |

## Reference evolution per Object

Reference evolution lists cold, then warm1 through warm9, for each planned Object. These are repeated reference timings within at most three Object clusters per role/size, not ten independent calibration replicates. Tardie's Actor is listed separately but belongs to the Thread's cluster; its HTTP reference precedes the Thread HTTP reference. Client values use raw samples.json reference receipts independently of CF telemetry. Thread DO/CPU values use collector reference fields; Actor values require one matching successful reference invocation. Missing values remain unavailable. Warm reference median [p25, p75] is descriptive over the available references with exact n, without a completeness or calibration claim. Full Object IDs and per-phase reference runtime IDs are retained in summary.json; cold reference runtime IDs precede the final abort.

### Reference clientWallMs (ms)

| Size | Role | Sample | DO kind | Object ID prefix | cold | warm1 | warm2 | warm3 | warm4 | warm5 | warm6 | warm7 | warm8 | warm9 | Warm reference median [p25, p75] (n/9) | Observed refs / 10 |
|---:|---|---:|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|
| 50 | base | 1 | thread | bd41f95388cd | 542 | 648 | 709 | 640 | 649 | 704 | 699 | 697 | 703 | 628 | 697 [648, 703] (9/9) | 10/10; complete |
| 50 | head | 1 | thread | 19f6ebf81407 | 505 | 609 | 675 | 611 | 649 | 651 | 705 | 677 | 627 | 633 | 649 [627, 675] (9/9) | 10/10; complete |
| 50 | control | 1 | thread | 5238cc083391 | 474 | 587 | 604 | 605 | 594 | 590 | 617 | 666 | 611 | 574 | 604 [590, 611] (9/9) | 10/10; complete |
| 50 | pinned | 1 | thread | 38acb1d08a59 | 511 | 431 | 395 | 393 | 403 | 403 | 396 | 403 | 409 | 393 | 403 [395, 403] (9/9) | 10/10; complete |
| 50 | pi | 1 | thread | 6d0a388f3dab | 386 | 296 | 288 | 296 | 293 | 287 | 286 | 291 | 294 | 293 | 293 [288, 294] (9/9) | 10/10; complete |
| 50 | tardie | 1 | thread | ea229efd8406 | 488 | 573 | 612 | 563 | 590 | 560 | 589 | 594 | 615 | 587 | 589 [573, 594] (9/9) | 10/10; complete |
| 50 | tardie | 1 | actor | 3883454c1f61 | 454 | 362 | 387 | 359 | 378 | 2551 | 372 | 363 | 362 | 358 | 363 [362, 378] (9/9) | 10/10; complete |
| 50 | base | 2 | thread | 9125856d6795 | 480 | 554 | 591 | 575 | 624 | 561 | 599 | 548 | 603 | 556 | 575 [556, 599] (9/9) | 10/10; complete |
| 50 | head | 2 | thread | 946d862f7ee4 | 370 | 297 | 269 | 264 | 280 | 476 | 269 | 285 | 289 | 274 | 280 [269, 289] (9/9) | 10/10; complete |
| 50 | control | 2 | thread | 9e48938e033f | 454 | 630 | 547 | 554 | 539 | 541 | 563 | 550 | 548 | 536 | 548 [541, 554] (9/9) | 10/10; complete |
| 50 | pinned | 2 | thread | 9309d68116da | 414 | 552 | 523 | 512 | 490 | 534 | 559 | 546 | 606 | 536 | 536 [523, 552] (9/9) | 10/10; complete |
| 50 | pi | 2 | thread | 60bdb44d0ecf | 468 | 542 | 520 | 524 | 520 | 610 | 522 | 609 | 600 | 533 | 533 [522, 600] (9/9) | 10/10; complete |
| 50 | tardie | 2 | thread | f84e5b3ad985 | 480 | 2250 | 563 | 601 | 594 | 566 | 552 | 558 | 666 | 569 | 569 [563, 601] (9/9) | 10/10; complete |
| 50 | tardie | 2 | actor | 3594d9cc49ef | 528 | 693 | 635 | 685 | 684 | 677 | 656 | 565 | 534 | 408 | 656 [565, 684] (9/9) | 10/10; complete |
| 50 | base | 3 | thread | b84aa0a2df90 | 468 | 382 | 385 | 381 | 387 | 385 | 380 | 390 | 377 | 375 | 382 [380, 385] (9/9) | 10/10; complete |
| 50 | head | 3 | thread | e88c31788ae0 | 478 | 361 | 359 | 367 | 369 | 370 | 353 | 368 | 378 | 371 | 368 [361, 370] (9/9) | 10/10; complete |
| 50 | control | 3 | thread | 695c68d822e0 | 507 | 357 | 353 | 384 | 363 | 382 | 360 | 382 | 360 | 370 | 363 [360, 382] (9/9) | 10/10; complete |
| 50 | pinned | 3 | thread | f826b0248d3e | 296 | 470 | 322 | 328 | 304 | 288 | 396 | 296 | 406 | 311 | 322 [304, 396] (9/9) | 10/10; complete |
| 50 | pi | 3 | thread | 66b7ee897918 | 478 | 366 | 358 | 381 | 405 | 360 | 362 | 371 | 376 | 371 | 371 [362, 376] (9/9) | 10/10; complete |
| 50 | tardie | 3 | thread | c8c7a019ff76 | 355 | 320 | 267 | 311 | 277 | 267 | 299 | 266 | 315 | 276 | 277 [267, 311] (9/9) | 10/10; complete |
| 50 | tardie | 3 | actor | 6ad25681ac56 | 404 | 515 | 485 | 507 | 496 | 518 | 485 | 468 | 467 | 474 | 485 [474, 507] (9/9) | 10/10; complete |
| 250 | base | 0 | thread | 2b0541784f44 | 368 | 466 | 492 | 443 | 474 | 484 | 481 | 493 | 462 | 455 | 474 [462, 484] (9/9) | 10/10; complete |
| 250 | head | 0 | thread | d18de0334763 | 421 | 525 | 568 | 522 | 558 | 509 | 504 | 508 | 556 | 551 | 525 [509, 556] (9/9) | 10/10; complete |
| 250 | control | 0 | thread | 353f29bc74a9 | 427 | 341 | 343 | 338 | 345 | 338 | 358 | 337 | 339 | 349 | 341 [338, 345] (9/9) | 10/10; complete |
| 250 | pinned | 0 | thread | 15822c223280 | 417 | 602 | 600 | 515 | 555 | 599 | 577 | 592 | 541 | 587 | 587 [555, 599] (9/9) | 10/10; complete |
| 250 | pi | 0 | thread | 8f8c1ae95606 | 440 | 362 | 366 | 369 | 354 | 360 | 356 | 365 | 353 | 359 | 360 [356, 365] (9/9) | 10/10; complete |
| 250 | tardie | 0 | thread | f7d89ee938cc | 545 | 726 | 713 | 723 | 707 | 728 | 684 | 707 | 750 | 658 | 713 [707, 726] (9/9) | 10/10; complete |
| 250 | tardie | 0 | actor | 9ad27b1eab86 | 486 | 360 | 362 | 364 | 362 | 360 | 356 | 362 | 363 | 362 | 362 [360, 362] (9/9) | 10/10; complete |
| 250 | base | 1 | thread | 392dd11f3186 | 423 | 291 | 303 | 279 | 294 | 294 | 287 | 285 | 279 | 291 | 291 [285, 294] (9/9) | 10/10; complete |
| 250 | head | 1 | thread | a00657292941 | 396 | 518 | 524 | 513 | 581 | 583 | 526 | 537 | 688 | 573 | 537 [524, 581] (9/9) | 10/10; complete |
| 250 | control | 1 | thread | df189c7b4fd8 | 358 | 346 | 280 | 293 | 277 | 254 | 268 | 260 | 259 | 261 | 268 [260, 280] (9/9) | 10/10; complete |
| 250 | pinned | 1 | thread | eaae0ff44019 | 536 | 625 | 670 | 710 | 554 | 557 | 518 | 506 | 515 | 513 | 554 [515, 625] (9/9) | 10/10; complete |
| 250 | pi | 1 | thread | f33bfa036e8a | 457 | 364 | 367 | 355 | 367 | 382 | 349 | 360 | 373 | 357 | 364 [357, 367] (9/9) | 10/10; complete |
| 250 | tardie | 1 | thread | 4cbb3d5911be | 358 | 475 | 480 | 455 | 480 | 478 | 471 | 451 | 493 | 477 | 477 [471, 480] (9/9) | 10/10; complete |
| 250 | tardie | 1 | actor | 9a7135fd321f | 352 | 315 | 256 | 272 | 264 | 263 | 261 | 254 | 254 | 263 | 263 [256, 264] (9/9) | 10/10; complete |
| 250 | base | 2 | thread | 3667e287c639 | 522 | 392 | 475 | 382 | 384 | 384 | 638 | 601 | 546 | 376 | 392 [384, 546] (9/9) | 10/10; complete |
| 250 | head | 2 | thread | 07d60ba0e842 | 489 | 418 | 406 | 416 | 392 | 439 | 404 | 407 | 408 | 400 | 407 [404, 416] (9/9) | 10/10; complete |
| 250 | control | 2 | thread | 74ad5afc14f4 | 409 | 312 | 359 | 276 | 280 | 278 | 274 | 286 | 283 | 273 | 280 [276, 286] (9/9) | 10/10; complete |
| 250 | pinned | 2 | thread | c88ce5929ae8 | 546 | 655 | 638 | 675 | 713 | 617 | 630 | 637 | 621 | 695 | 638 [630, 675] (9/9) | 10/10; complete |
| 250 | pi | 2 | thread | e8a880eb3d1f | 457 | 372 | 369 | 363 | 372 | 368 | 367 | 389 | 416 | 365 | 369 [367, 372] (9/9) | 10/10; complete |
| 250 | tardie | 2 | thread | 5abb5cbe2287 | 362 | 387 | 432 | 450 | 414 | 419 | 426 | 418 | 388 | 393 | 418 [393, 426] (9/9) | 10/10; complete |
| 250 | tardie | 2 | actor | f3cee5d49a3c | 394 | 457 | 456 | 492 | 482 | 456 | 462 | 457 | 383 | 372 | 457 [456, 462] (9/9) | 10/10; complete |
| 1000 | base | 0 | thread | e269a4cd9c08 | 548 | 560 | 576 | 566 | 579 | 548 | 609 | 539 | 574 | 542 | 566 [548, 576] (9/9) | 10/10; complete |
| 1000 | head | 0 | thread | 4833972521e7 | 373 | 297 | 286 | 282 | 287 | 278 | 290 | 278 | 307 | 290 | 287 [282, 290] (9/9) | 10/10; complete |
| 1000 | control | 0 | thread | 0f8a843a606c | 252 | 170 | 167 | 170 | 181 | 165 | 168 | 161 | 163 | 176 | 168 [165, 170] (9/9) | 10/10; complete |
| 1000 | pinned | 0 | thread | 3ade32be2632 | 324 | 385 | 506 | 494 | 475 | 370 | 379 | 382 | 409 | 391 | 391 [382, 475] (9/9) | 10/10; complete |
| 1000 | pi | 0 | thread | 2499e47521f1 | 394 | 130 | 117 | 183 | 235 | 181 | 127 | 191 | 188 | 126 | 181 [127, 188] (9/9) | 10/10; complete |
| 1000 | tardie | 0 | thread | 45e8775b408f | 334 | 190 | 183 | 274 | 224 | 181 | 181 | 197 | 199 | 208 | 197 [183, 208] (9/9) | 10/10; complete |
| 1000 | tardie | 0 | actor | 6e3dd79ca215 | 401 | 196 | 218 | 191 | 247 | 199 | 203 | 214 | 219 | 257 | 214 [199, 219] (9/9) | 10/10; complete |
| 1000 | base | 1 | thread | 67ea61c7767b | 275 | 197 | 139 | 126 | 126 | 125 | 138 | 119 | 120 | 121 | 126 [121, 138] (9/9) | 10/10; complete |
| 1000 | head | 1 | thread | 85273887e2e2 | 293 | 441 | 312 | 326 | 463 | 462 | 325 | 314 | 317 | 301 | 325 [314, 441] (9/9) | 10/10; complete |
| 1000 | control | 1 | thread | 6a723a4d4dce | 345 | 488 | 473 | 523 | 455 | 469 | 514 | 509 | 502 | 462 | 488 [469, 509] (9/9) | 10/10; complete |
| 1000 | pinned | 1 | thread | 29ee4c899796 | 202 | 81 | 66 | 71 | 126 | 89 | 76 | 66 | 79 | 101 | 79 [71, 89] (9/9) | 10/10; complete |
| 1000 | pi | 1 | thread | 8d3235332deb | 551 | 406 | 446 | 393 | 424 | 410 | 409 | 423 | 444 | 408 | 410 [408, 424] (9/9) | 10/10; complete |
| 1000 | tardie | 1 | thread | 349ec998b1f0 | 170 | 259 | 277 | 267 | 329 | 253 | 315 | 327 | 334 | 254 | 277 [259, 327] (9/9) | 10/10; complete |
| 1000 | tardie | 1 | actor | 9649ae6cd150 | 234 | 91 | 93 | 95 | 93 | 85 | 103 | 91 | 109 | 98 | 93 [91, 98] (9/9) | 10/10; complete |
| 1000 | base | 2 | thread | cce5061590d5 | 228 | 136 | 138 | 146 | 137 | 130 | 141 | 131 | 128 | 139 | 137 [131, 139] (9/9) | 10/10; complete |
| 1000 | head | 2 | thread | 1d2eaf1dc9c0 | 162 | 101 | 73 | 66 | 51 | 55 | 58 | 55 | 66 | 61 | 61 [55, 66] (9/9) | 10/10; complete |
| 1000 | control | 2 | thread | 801831520349 | 191 | 105 | 122 | 114 | 107 | 129 | 101 | 108 | 109 | 114 | 109 [107, 114] (9/9) | 10/10; complete |
| 1000 | pinned | 2 | thread | ed2e35a2e421 | 377 | 191 | 192 | 153 | 142 | 157 | 149 | 188 | 145 | 144 | 153 [145, 188] (9/9) | 10/10; complete |
| 1000 | pi | 2 | thread | e5a71e9c0261 | 320 | 199 | 213 | 186 | 172 | 179 | 211 | 222 | 257 | 194 | 199 [186, 213] (9/9) | 10/10; complete |
| 1000 | tardie | 2 | thread | 997d42d272bf | 284 | 441 | 432 | 378 | 387 | 387 | 373 | 450 | 387 | 406 | 387 [387, 432] (9/9) | 10/10; complete |
| 1000 | tardie | 2 | actor | 861ab423bf3a | 299 | 401 | 427 | 399 | 429 | 386 | 418 | 419 | 415 | 364 | 415 [399, 419] (9/9) | 10/10; complete |

### Reference doWallMs (ms)

| Size | Role | Sample | DO kind | Object ID prefix | cold | warm1 | warm2 | warm3 | warm4 | warm5 | warm6 | warm7 | warm8 | warm9 | Warm reference median [p25, p75] (n/9) | Observed refs / 10 |
|---:|---|---:|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|
| 50 | base | 1 | thread | bd41f95388cd | 348 | 459 | 493 | 445 | 450 | 508 | 509 | 505 | 516 | 439 | 493 [450, 508] (9/9) | 10/10; complete |
| 50 | head | 1 | thread | 19f6ebf81407 | 305 | 412 | 479 | 414 | 453 | 453 | 506 | 482 | 432 | 438 | 453 [432, 479] (9/9) | 10/10; complete |
| 50 | control | 1 | thread | 5238cc083391 | 294 | 404 | 416 | 422 | 417 | 400 | 435 | 482 | 428 | 388 | 417 [404, 428] (9/9) | 10/10; complete |
| 50 | pinned | 1 | thread | 38acb1d08a59 | 324 | 214 | 210 | 208 | 208 | 212 | 213 | 216 | 207 | unavailable | 211 [208, 213.25] (8/9) | 9/10; incomplete |
| 50 | pi | 1 | thread | 6d0a388f3dab | 254 | 166 | 162 | 174 | 170 | 164 | 163 | 170 | 168 | 170 | 168 [164, 170] (9/9) | 10/10; complete |
| 50 | tardie | 1 | thread | ea229efd8406 | 306 | 391 | 408 | 384 | 413 | 385 | 410 | unavailable | 436 | 406 | 407 [389.5, 410.75] (8/9) | 9/10; incomplete |
| 50 | tardie | 1 | actor | 3883454c1f61 | 274 | 184 | 190 | 182 | 188 | 180 | 178 | 180 | 183 | 178 | 182 [180, 184] (9/9) | 10/10; complete |
| 50 | base | 2 | thread | 9125856d6795 | unavailable | 368 | 401 | 379 | 437 | 373 | 411 | 362 | 415 | 368 | 379 [368, 411] (9/9) | 9/10; incomplete |
| 50 | head | 2 | thread | 946d862f7ee4 | 233 | 152 | 142 | 142 | 149 | 151 | 144 | 155 | 147 | 139 | 147 [142, 151] (9/9) | 10/10; complete |
| 50 | control | 2 | thread | 9e48938e033f | 265 | 435 | 348 | 348 | 347 | 347 | 348 | 355 | 353 | 345 | 348 [347, 353] (9/9) | 10/10; complete |
| 50 | pinned | 2 | thread | 9309d68116da | 284 | 425 | 396 | 368 | 365 | 412 | 429 | unavailable | 469 | 406 | 409 [389, 426] (8/9) | 9/10; incomplete |
| 50 | pi | 2 | thread | 60bdb44d0ecf | 272 | 354 | unavailable | 341 | 337 | 428 | 335 | 424 | 416 | 349 | 351.5 [340, 418] (8/9) | 9/10; incomplete |
| 50 | tardie | 2 | thread | f84e5b3ad985 | 274 | 411 | 364 | 404 | 401 | 365 | 356 | 355 | 452 | 359 | 365 [359, 404] (9/9) | 10/10; complete |
| 50 | tardie | 2 | actor | 3594d9cc49ef | 327 | 472 | 434 | 481 | 476 | 476 | 457 | 367 | unavailable | 202 | 464.5 [417.25, 476] (8/9) | 9/10; incomplete |
| 50 | base | 3 | thread | b84aa0a2df90 | 273 | 192 | 180 | 181 | 199 | 189 | 188 | 182 | 188 | 184 | 188 [182, 189] (9/9) | 10/10; complete |
| 50 | head | 3 | thread | e88c31788ae0 | 297 | 182 | 179 | 181 | 185 | 190 | 175 | 188 | 197 | 193 | 185 [181, 190] (9/9) | 10/10; complete |
| 50 | control | 3 | thread | 695c68d822e0 | 308 | 183 | 181 | 200 | 183 | 201 | 185 | 195 | 185 | 190 | 185 [183, 195] (9/9) | 10/10; complete |
| 50 | pinned | 3 | thread | f826b0248d3e | 232 | 394 | 253 | 263 | 269 | 255 | 365 | 263 | 373 | 265 | 265 [263, 365] (9/9) | 10/10; complete |
| 50 | pi | 3 | thread | 66b7ee897918 | 281 | unavailable | 177 | 198 | 222 | unavailable | 178 | 182 | 192 | 193 | 192 [180, 195.5] (7/9) | 8/10; incomplete |
| 50 | tardie | 3 | thread | c8c7a019ff76 | 233 | 143 | 147 | 146 | 153 | unavailable | 136 | 149 | 149 | 154 | 148 [145.25, 150] (8/9) | 9/10; incomplete |
| 50 | tardie | 3 | actor | 6ad25681ac56 | 235 | 344 | 350 | unavailable | 367 | 356 | 321 | 349 | 345 | 353 | 349.5 [344.75, 353.75] (8/9) | 9/10; incomplete |
| 250 | base | 0 | thread | 2b0541784f44 | 245 | 343 | 367 | 313 | 346 | 355 | 343 | 353 | 326 | 319 | 343 [326, 353] (9/9) | 10/10; complete |
| 250 | head | 0 | thread | d18de0334763 | 288 | 387 | 424 | 390 | 419 | 381 | 381 | 382 | 422 | 420 | 390 [382, 420] (9/9) | 10/10; complete |
| 250 | control | 0 | thread | 353f29bc74a9 | 250 | 164 | unavailable | 162 | unavailable | 162 | unavailable | 156 | 161 | 162 | 162 [161.25, 162] (6/9) | 7/10; incomplete |
| 250 | pinned | 0 | thread | 15822c223280 | 284 | 450 | 464 | 391 | 429 | 472 | 452 | 461 | 415 | 462 | 452 [429, 462] (9/9) | 10/10; complete |
| 250 | pi | 0 | thread | 8f8c1ae95606 | 259 | 179 | 189 | 188 | 176 | 176 | 172 | 182 | 171 | 179 | 179 [176, 182] (9/9) | 10/10; complete |
| 250 | tardie | 0 | thread | f7d89ee938cc | unavailable | unavailable | unavailable | 540 | 525 | 547 | 499 | 524 | 473 | 477 | 524 [488, 532.5] (7/9) | 7/10; incomplete |
| 250 | tardie | 0 | actor | 9ad27b1eab86 | unavailable | unavailable | 178 | 178 | 174 | 179 | 172 | 180 | 178 | 180 | 178 [177, 179.25] (8/9) | 8/10; incomplete |
| 250 | base | 1 | thread | 392dd11f3186 | 295 | unavailable | 167 | 152 | 160 | 165 | 157 | 152 | 150 | 160 | 158.5 [152, 161.25] (8/9) | 9/10; incomplete |
| 250 | head | 1 | thread | a00657292941 | 275 | 395 | 399 | 395 | 453 | 465 | unavailable | 410 | 546 | 452 | 431 [398, 456] (8/9) | 9/10; incomplete |
| 250 | control | 1 | thread | df189c7b4fd8 | unavailable | unavailable | unavailable | unavailable | unavailable | unavailable | unavailable | unavailable | 126 | unavailable | 126 [126, 126] (1/9) | 1/10; incomplete |
| 250 | pinned | 1 | thread | eaae0ff44019 | 402 | 488 | 464 | 538 | 426 | 400 | 391 | 377 | 389 | 388 | 400 [389, 464] (9/9) | 10/10; complete |
| 250 | pi | 1 | thread | f33bfa036e8a | 252 | 165 | 169 | 155 | 162 | 183 | 155 | 162 | 178 | 160 | 162 [160, 169] (9/9) | 10/10; complete |
| 250 | tardie | 1 | thread | 4cbb3d5911be | 236 | 357 | 364 | 332 | 359 | 357 | 355 | 329 | 374 | 358 | 357 [355, 359] (9/9) | 10/10; complete |
| 250 | tardie | 1 | actor | 9a7135fd321f | 230 | 146 | 139 | 153 | 146 | 142 | 143 | 134 | 134 | 144 | 143 [139, 146] (9/9) | 10/10; complete |
| 250 | base | 2 | thread | 3667e287c639 | 338 | 210 | 198 | 203 | 205 | 202 | 215 | 203 | 212 | 197 | 203 [202, 210] (9/9) | 10/10; complete |
| 250 | head | 2 | thread | 07d60ba0e842 | 303 | 212 | 196 | 205 | 193 | 202 | unavailable | 218 | unavailable | unavailable | 203.5 [197.5, 210.25] (6/9) | 7/10; incomplete |
| 250 | control | 2 | thread | 74ad5afc14f4 | 287 | 157 | 161 | 153 | 152 | 151 | 149 | 158 | 161 | 151 | 153 [151, 158] (9/9) | 10/10; complete |
| 250 | pinned | 2 | thread | c88ce5929ae8 | 359 | unavailable | 449 | 497 | 530 | 429 | 441 | 452 | 432 | 497 | 450.5 [438.75, 497] (8/9) | 9/10; incomplete |
| 250 | pi | 2 | thread | e8a880eb3d1f | 274 | 180 | 178 | 176 | 184 | 182 | 178 | 176 | 179 | 176 | 178 [176, 180] (9/9) | 10/10; complete |
| 250 | tardie | 2 | thread | 5abb5cbe2287 | 263 | 293 | 294 | 326 | 323 | 324 | unavailable | 288 | 294 | 299 | 296.5 [293.75, 323.25] (8/9) | 9/10; incomplete |
| 250 | tardie | 2 | actor | f3cee5d49a3c | 248 | 361 | 361 | 365 | 376 | 359 | unavailable | 361 | 286 | 271 | 361 [340.75, 362] (8/9) | 9/10; incomplete |
| 1000 | base | 0 | thread | e269a4cd9c08 | 294 | 357 | 383 | 358 | 382 | 352 | 399 | 346 | 377 | 351 | 358 [352, 382] (9/9) | 10/10; complete |
| 1000 | head | 0 | thread | 4833972521e7 | unavailable | 154 | 151 | 159 | 152 | 154 | 154 | 155 | 174 | 165 | 154 [154, 159] (9/9) | 9/10; incomplete |
| 1000 | control | 0 | thread | 0f8a843a606c | 152 | unavailable | 60 | 66 | 78 | 63 | 63 | 60 | 60 | 73 | 63 [60, 67.75] (8/9) | 9/10; incomplete |
| 1000 | pinned | 0 | thread | 3ade32be2632 | 193 | 331 | 326 | 300 | 310 | 305 | 327 | 325 | 306 | 333 | 325 [306, 327] (9/9) | 10/10; complete |
| 1000 | pi | 0 | thread | 2499e47521f1 | 200 | 54 | 44 | 51 | 49 | 46 | 49 | 47 | 51 | 47 | 49 [47, 51] (9/9) | 10/10; complete |
| 1000 | tardie | 0 | thread | 45e8775b408f | 236 | 98 | 100 | 116 | 119 | 98 | 97 | 108 | 103 | 117 | 103 [98, 116] (9/9) | 10/10; complete |
| 1000 | tardie | 0 | actor | 6e3dd79ca215 | 310 | 109 | 109 | 107 | 167 | 109 | 110 | 110 | 133 | 161 | 110 [109, 133] (9/9) | 10/10; complete |
| 1000 | base | 1 | thread | 67ea61c7767b | 149 | 61 | 72 | 61 | 64 | 63 | 76 | 60 | 61 | 64 | 63 [61, 64] (9/9) | 10/10; complete |
| 1000 | head | 1 | thread | 85273887e2e2 | 165 | 372 | 258 | 269 | 363 | 367 | unavailable | 256 | 244 | 249 | 263.5 [254.25, 364] (8/9) | 9/10; incomplete |
| 1000 | control | 1 | thread | 6a723a4d4dce | 243 | 383 | 372 | 422 | 353 | 368 | 413 | 406 | 393 | unavailable | 388 [371, 407.75] (8/9) | 9/10; incomplete |
| 1000 | pinned | 1 | thread | 29ee4c899796 | 179 | 50 | 43 | 47 | 43 | 47 | 47 | 45 | 45 | 41 | 45 [43, 47] (9/9) | 10/10; complete |
| 1000 | pi | 1 | thread | 8d3235332deb | 366 | 214 | 233 | 207 | 231 | 210 | 213 | 231 | 209 | 226 | 214 [210, 231] (9/9) | 10/10; complete |
| 1000 | tardie | 1 | thread | 349ec998b1f0 | 144 | 236 | 245 | 240 | 305 | 230 | 280 | 299 | 237 | 230 | 240 [236, 280] (9/9) | 10/10; complete |
| 1000 | tardie | 1 | actor | 9649ae6cd150 | 203 | 66 | 66 | 64 | 69 | 64 | 75 | 67 | 71 | 69 | 67 [66, 69] (9/9) | 10/10; complete |
| 1000 | base | 2 | thread | cce5061590d5 | 147 | 54 | 57 | 71 | 61 | 52 | 58 | 52 | 53 | 57 | 57 [53, 58] (9/9) | 10/10; complete |
| 1000 | head | 2 | thread | 1d2eaf1dc9c0 | 122 | unavailable | unavailable | 34 | 33 | 33 | 48 | 37 | 45 | 41 | 37 [33.5, 43] (7/9) | 8/10; incomplete |
| 1000 | control | 2 | thread | 801831520349 | 163 | 81 | 89 | 83 | 79 | 99 | 78 | 83 | 81 | 93 | 83 [81, 89] (9/9) | 10/10; complete |
| 1000 | pinned | 2 | thread | ed2e35a2e421 | 249 | 86 | 83 | 88 | 81 | 71 | 80 | 74 | 85 | 83 | 83 [80, 85] (9/9) | 10/10; complete |
| 1000 | pi | 2 | thread | e5a71e9c0261 | 199 | 76 | 79 | 75 | 82 | 79 | 87 | 91 | 75 | 86 | 79 [76, 86] (9/9) | 10/10; complete |
| 1000 | tardie | 2 | thread | 997d42d272bf | 195 | 346 | 306 | unavailable | 299 | 297 | 284 | 343 | 296 | 317 | 302.5 [296.75, 323.5] (8/9) | 9/10; incomplete |
| 1000 | tardie | 2 | actor | 861ab423bf3a | 208 | 312 | 310 | 311 | unavailable | 296 | 323 | 326 | 326 | 280 | 311.5 [306.5, 323.75] (8/9) | 9/10; incomplete |

### Reference cpuTimeMs (ms)

| Size | Role | Sample | DO kind | Object ID prefix | cold | warm1 | warm2 | warm3 | warm4 | warm5 | warm6 | warm7 | warm8 | warm9 | Warm reference median [p25, p75] (n/9) | Observed refs / 10 |
|---:|---|---:|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|
| 50 | base | 1 | thread | bd41f95388cd | 184 | 279 | 325 | 276 | 268 | 338 | 334 | 326 | 344 | 262 | 325 [276, 334] (9/9) | 10/10; complete |
| 50 | head | 1 | thread | 19f6ebf81407 | 157 | 261 | 322 | 263 | 268 | 295 | 340 | 312 | 269 | 278 | 278 [268, 312] (9/9) | 10/10; complete |
| 50 | control | 1 | thread | 5238cc083391 | 155 | 259 | 266 | 263 | 277 | 264 | 298 | 316 | 281 | 248 | 266 [263, 281] (9/9) | 10/10; complete |
| 50 | pinned | 1 | thread | 38acb1d08a59 | 159 | 38 | 37 | 37 | 43 | 37 | 42 | 41 | 43 | unavailable | 39.5 [37, 42.25] (8/9) | 9/10; incomplete |
| 50 | pi | 1 | thread | 6d0a388f3dab | 118 | 31 | 29 | 42 | 38 | 28 | 29 | 30 | 29 | 39 | 30 [29, 38] (9/9) | 10/10; complete |
| 50 | tardie | 1 | thread | ea229efd8406 | 134 | 223 | 235 | 211 | 243 | 220 | 240 | unavailable | 260 | 238 | 236.5 [222.25, 240.75] (8/9) | 9/10; incomplete |
| 50 | tardie | 1 | actor | 3883454c1f61 | 122 | 32 | 35 | 31 | 34 | 31 | 31 | 30 | 38 | 35 | 32 [31, 35] (9/9) | 10/10; complete |
| 50 | base | 2 | thread | 9125856d6795 | unavailable | 207 | 240 | 211 | 263 | 215 | 250 | 210 | 245 | 209 | 215 [210, 245] (9/9) | 9/10; incomplete |
| 50 | head | 2 | thread | 946d862f7ee4 | 121 | 37 | 31 | 30 | 36 | 33 | 31 | 33 | 32 | 31 | 32 [31, 33] (9/9) | 10/10; complete |
| 50 | control | 2 | thread | 9e48938e033f | 118 | 281 | 196 | 199 | 199 | 195 | 195 | 198 | 198 | 188 | 198 [195, 199] (9/9) | 10/10; complete |
| 50 | pinned | 2 | thread | 9309d68116da | 167 | 321 | 277 | 255 | 266 | 313 | 323 | unavailable | 334 | 280 | 296.5 [274.25, 321.5] (8/9) | 9/10; incomplete |
| 50 | pi | 2 | thread | 60bdb44d0ecf | 123 | 203 | unavailable | 195 | 189 | 279 | 192 | 280 | 271 | 200 | 201.5 [194.25, 273] (8/9) | 9/10; incomplete |
| 50 | tardie | 2 | thread | f84e5b3ad985 | 114 | 247 | 190 | 243 | 195 | 195 | 186 | 192 | 284 | 191 | 195 [191, 243] (9/9) | 10/10; complete |
| 50 | tardie | 2 | actor | 3594d9cc49ef | 179 | 327 | 296 | 333 | 321 | 328 | 299 | 219 | unavailable | 48 | 310 [276.75, 327.25] (8/9) | 9/10; incomplete |
| 50 | base | 3 | thread | b84aa0a2df90 | 124 | 34 | 30 | 31 | 43 | 34 | 31 | 30 | 31 | 33 | 31 [31, 34] (9/9) | 10/10; complete |
| 50 | head | 3 | thread | e88c31788ae0 | 142 | 30 | 29 | 29 | 37 | 30 | 29 | 35 | 29 | 33 | 30 [29, 33] (9/9) | 10/10; complete |
| 50 | control | 3 | thread | 695c68d822e0 | 134 | 32 | 31 | 45 | 31 | 52 | 34 | 45 | 30 | 32 | 32 [31, 45] (9/9) | 10/10; complete |
| 50 | pinned | 3 | thread | f826b0248d3e | 170 | 323 | 190 | 196 | 204 | 195 | 294 | 201 | 304 | 202 | 202 [196, 294] (9/9) | 10/10; complete |
| 50 | pi | 3 | thread | 66b7ee897918 | 115 | unavailable | 29 | 47 | 55 | unavailable | 29 | 31 | 30 | 29 | 30 [29, 39] (7/9) | 8/10; incomplete |
| 50 | tardie | 3 | thread | c8c7a019ff76 | 119 | 35 | 33 | 32 | 32 | unavailable | 29 | 33 | 28 | 33 | 32.5 [31.25, 33] (8/9) | 9/10; incomplete |
| 50 | tardie | 3 | actor | 6ad25681ac56 | 119 | 219 | 239 | unavailable | 249 | 240 | 201 | 239 | 228 | 237 | 238 [225.75, 239.25] (8/9) | 9/10; incomplete |
| 250 | base | 0 | thread | 2b0541784f44 | 128 | 228 | 248 | 194 | 226 | 236 | 201 | 229 | 198 | 198 | 226 [198, 229] (9/9) | 10/10; complete |
| 250 | head | 0 | thread | d18de0334763 | 176 | 263 | 307 | 267 | 266 | 264 | 262 | 266 | 301 | 274 | 266 [264, 274] (9/9) | 10/10; complete |
| 250 | control | 0 | thread | 353f29bc74a9 | 115 | 29 | unavailable | 28 | unavailable | 29 | unavailable | 30 | 28 | 29 | 29 [28.25, 29] (6/9) | 7/10; incomplete |
| 250 | pinned | 0 | thread | 15822c223280 | 170 | 328 | 344 | 277 | 308 | 344 | 335 | 338 | 292 | 341 | 335 [308, 341] (9/9) | 10/10; complete |
| 250 | pi | 0 | thread | 8f8c1ae95606 | 114 | 29 | 38 | 40 | 28 | 29 | 28 | 43 | 29 | 38 | 29 [29, 38] (9/9) | 10/10; complete |
| 250 | tardie | 0 | thread | f7d89ee938cc | unavailable | unavailable | unavailable | 355 | 344 | 364 | 313 | 343 | 294 | 294 | 343 [303.5, 349.5] (7/9) | 7/10; incomplete |
| 250 | tardie | 0 | actor | 9ad27b1eab86 | unavailable | unavailable | 38 | 42 | 40 | 39 | 38 | 41 | 42 | 40 | 40 [38.75, 41.25] (8/9) | 8/10; incomplete |
| 250 | base | 1 | thread | 392dd11f3186 | 180 | unavailable | 44 | 40 | 41 | 48 | 45 | 43 | 38 | 45 | 43.5 [40.75, 45] (8/9) | 9/10; incomplete |
| 250 | head | 1 | thread | a00657292941 | 168 | 279 | 283 | 283 | 325 | 348 | unavailable | 286 | 337 | 333 | 305.5 [283, 334] (8/9) | 9/10; incomplete |
| 250 | control | 1 | thread | df189c7b4fd8 | unavailable | unavailable | unavailable | unavailable | unavailable | unavailable | unavailable | unavailable | 28 | unavailable | 28 [28, 28] (1/9) | 1/10; incomplete |
| 250 | pinned | 1 | thread | eaae0ff44019 | 154 | 273 | 295 | 310 | 301 | 276 | 269 | 264 | 268 | 265 | 273 [268, 295] (9/9) | 10/10; complete |
| 250 | pi | 1 | thread | f33bfa036e8a | 118 | 30 | 42 | 30 | 29 | 49 | 29 | 28 | 44 | 29 | 30 [29, 42] (9/9) | 10/10; complete |
| 250 | tardie | 1 | thread | 4cbb3d5911be | 125 | 243 | 245 | 212 | 243 | 238 | 237 | 211 | 261 | 239 | 239 [237, 243] (9/9) | 10/10; complete |
| 250 | tardie | 1 | actor | 9a7135fd321f | 126 | 33 | 31 | 36 | 37 | 32 | 32 | 31 | 31 | 35 | 32 [31, 35] (9/9) | 10/10; complete |
| 250 | base | 2 | thread | 3667e287c639 | 179 | 42 | 39 | 39 | 39 | 42 | 42 | 41 | 43 | 39 | 41 [39, 42] (9/9) | 10/10; complete |
| 250 | head | 2 | thread | 07d60ba0e842 | 118 | 32 | 35 | 28 | 29 | 39 | unavailable | 40 | unavailable | unavailable | 33.5 [29.75, 38] (6/9) | 7/10; incomplete |
| 250 | control | 2 | thread | 74ad5afc14f4 | 179 | 41 | 50 | 39 | 40 | 39 | 39 | 46 | 46 | 39 | 40 [39, 46] (9/9) | 10/10; complete |
| 250 | pinned | 2 | thread | c88ce5929ae8 | 165 | unavailable | 272 | 325 | 316 | 266 | 265 | 271 | 271 | 325 | 271.5 [269.75, 318.25] (8/9) | 9/10; incomplete |
| 250 | pi | 2 | thread | e8a880eb3d1f | 127 | 28 | 38 | 31 | 32 | 31 | 29 | 31 | 28 | 28 | 31 [28, 31] (9/9) | 10/10; complete |
| 250 | tardie | 2 | thread | 5abb5cbe2287 | 159 | 200 | 201 | 233 | 230 | 230 | unavailable | 198 | 204 | 204 | 204 [200.75, 230] (8/9) | 9/10; incomplete |
| 250 | tardie | 2 | actor | f3cee5d49a3c | 160 | 270 | 270 | 274 | 285 | 271 | unavailable | 271 | 193 | 181 | 270.5 [250.75, 271.75] (8/9) | 9/10; incomplete |
| 1000 | base | 0 | thread | e269a4cd9c08 | 134 | 200 | 225 | 196 | 224 | 190 | 232 | 191 | 222 | 189 | 200 [191, 224] (9/9) | 10/10; complete |
| 1000 | head | 0 | thread | 4833972521e7 | unavailable | 32 | 30 | 38 | 31 | 29 | 29 | 30 | 42 | 44 | 31 [30, 38] (9/9) | 9/10; incomplete |
| 1000 | control | 0 | thread | 0f8a843a606c | 114 | unavailable | 28 | 28 | 44 | 30 | 30 | 29 | 29 | 28 | 29 [28, 30] (8/9) | 9/10; incomplete |
| 1000 | pinned | 0 | thread | 3ade32be2632 | 162 | 300 | 294 | 268 | 266 | 270 | 272 | 282 | 271 | 298 | 272 [270, 294] (9/9) | 10/10; complete |
| 1000 | pi | 0 | thread | 2499e47521f1 | 192 | 46 | 39 | 45 | 44 | 38 | 43 | 39 | 44 | 40 | 43 [39, 44] (9/9) | 10/10; complete |
| 1000 | tardie | 0 | thread | 45e8775b408f | 167 | 36 | 38 | 34 | 35 | 39 | 36 | 36 | 38 | 35 | 36 [35, 38] (9/9) | 10/10; complete |
| 1000 | tardie | 0 | actor | 6e3dd79ca215 | 161 | 44 | 43 | 44 | 39 | 39 | 42 | 38 | 40 | 44 | 42 [39, 44] (9/9) | 10/10; complete |
| 1000 | base | 1 | thread | 67ea61c7767b | 118 | 30 | 37 | 28 | 29 | 29 | 41 | 28 | 28 | 29 | 29 [28, 30] (9/9) | 10/10; complete |
| 1000 | head | 1 | thread | 85273887e2e2 | 113 | 305 | 197 | 203 | 299 | 302 | unavailable | 189 | 192 | 189 | 200 [191.25, 299.75] (8/9) | 9/10; incomplete |
| 1000 | control | 1 | thread | 6a723a4d4dce | 154 | 290 | 271 | 327 | 257 | 279 | 309 | 314 | 306 | unavailable | 298 [277, 310.25] (8/9) | 9/10; incomplete |
| 1000 | pinned | 1 | thread | 29ee4c899796 | 172 | 45 | 37 | 42 | 37 | 42 | 42 | 40 | 41 | 37 | 41 [37, 42] (9/9) | 10/10; complete |
| 1000 | pi | 1 | thread | 8d3235332deb | 201 | 42 | 47 | 41 | 44 | 40 | 41 | 45 | 41 | 49 | 42 [41, 45] (9/9) | 10/10; complete |
| 1000 | tardie | 1 | thread | 349ec998b1f0 | 120 | 210 | 218 | 210 | 279 | 203 | 253 | 274 | 209 | 205 | 210 [209, 253] (9/9) | 10/10; complete |
| 1000 | tardie | 1 | actor | 9649ae6cd150 | 175 | 42 | 41 | 41 | 45 | 41 | 46 | 41 | 45 | 44 | 42 [41, 45] (9/9) | 10/10; complete |
| 1000 | base | 2 | thread | cce5061590d5 | 116 | 32 | 31 | 46 | 29 | 30 | 28 | 30 | 30 | 29 | 30 [29, 31] (9/9) | 10/10; complete |
| 1000 | head | 2 | thread | 1d2eaf1dc9c0 | 118 | unavailable | unavailable | 31 | 29 | 29 | 34 | 33 | 36 | 36 | 33 [30, 35] (7/9) | 8/10; incomplete |
| 1000 | control | 2 | thread | 801831520349 | 113 | 31 | 40 | 29 | 29 | 47 | 29 | 30 | 31 | 43 | 31 [29, 40] (9/9) | 10/10; complete |
| 1000 | pinned | 2 | thread | ed2e35a2e421 | 215 | 47 | 41 | 47 | 46 | 40 | 46 | 41 | 47 | 45 | 46 [41, 47] (9/9) | 10/10; complete |
| 1000 | pi | 2 | thread | e5a71e9c0261 | 149 | 31 | 30 | 30 | 29 | 30 | 30 | 41 | 30 | 30 | 30 [30, 30] (9/9) | 10/10; complete |
| 1000 | tardie | 2 | thread | 997d42d272bf | 163 | 314 | 271 | unavailable | 265 | 264 | 254 | 313 | 259 | 279 | 268 [262.75, 287.5] (8/9) | 9/10; incomplete |
| 1000 | tardie | 2 | actor | 861ab423bf3a | 176 | 281 | 285 | 288 | unavailable | 268 | 297 | 301 | 296 | 251 | 286.5 [277.75, 296.25] (8/9) | 9/10; incomplete |

## Observed alarm overlaps

observedAlarmOverlap and observedAlarmOverlaps retain the collector's DO telemetry interval-intersection evidence per turn. False means no overlap was observed in available telemetry, not proof that alarms were absent. Missing flags/evidence remain unavailable. Event entries may repeat across intersected turns and are not distinct alarm counts; overlap observations do not establish causation.

| Size | Role | Deployed build set | Phase | Observed overlap turns | No observed overlap turns | Unavailable flags | Expected turns | Observed event entries |
|---:|---|---|---|---:|---:|---:|---:|---:|
| 50 | base | base@4.0.1 (deabe970/289122cd); 3/3 deployments matched | cold | 2 | 0 | 1 | 3 | 6 (2/3; incomplete) |
| 50 | base | base@4.0.1 (deabe970/289122cd); 3/3 deployments matched | warm | 26 | 1 | 0 | 27 | 52 (27/27) |
| 50 | head | head@4.0.1 (fc50f45a/289122cd); 3/3 deployments matched | cold | 3 | 0 | 0 | 3 | 6 (3/3) |
| 50 | head | head@4.0.1 (fc50f45a/289122cd); 3/3 deployments matched | warm | 27 | 0 | 0 | 27 | 58 (27/27) |
| 50 | control | control@4.0.1 (deabe970/289122cd); 3/3 deployments matched | cold | 3 | 0 | 0 | 3 | 6 (3/3) |
| 50 | control | control@4.0.1 (deabe970/289122cd); 3/3 deployments matched | warm | 27 | 0 | 0 | 27 | 53 (27/27) |
| 50 | pinned | pinned@4.0.0 (c823d174/289122cd); 3/3 deployments matched | cold | 3 | 0 | 0 | 3 | 8 (3/3) |
| 50 | pinned | pinned@4.0.0 (c823d174/289122cd); 3/3 deployments matched | warm | 27 | 0 | 0 | 27 | 52 (27/27) |
| 50 | pi | pi 1.0.4 / no Effect (5526e585/866f1f7f); 3/3 deployments matched | cold | 0 | 3 | 0 | 3 | 0 (3/3) |
| 50 | pi | pi 1.0.4 / no Effect (5526e585/866f1f7f); 3/3 deployments matched | warm | 0 | 24 | 3 | 27 | 0 (24/27; incomplete) |
| 50 | tardie | tardie@4.0.0-rc.115 (c8fbbb72/8a0cb11b); 3/3 deployments matched | cold | 3 | 0 | 0 | 3 | 5 (3/3) |
| 50 | tardie | tardie@4.0.0-rc.115 (c8fbbb72/8a0cb11b); 3/3 deployments matched | warm | 27 | 0 | 0 | 27 | 61 (27/27) |
| 250 | base | base@4.0.1 (deabe970/289122cd); base@4.0.1 (deabe970/6b2bef15); 3/3 deployments matched | cold | 3 | 0 | 0 | 3 | 7 (3/3) |
| 250 | base | base@4.0.1 (deabe970/289122cd); base@4.0.1 (deabe970/6b2bef15); 3/3 deployments matched | warm | 27 | 0 | 0 | 27 | 54 (27/27) |
| 250 | head | head@4.0.1 (fc50f45a/289122cd); head@4.0.1 (fc50f45a/6b2bef15); 3/3 deployments matched | cold | 3 | 0 | 0 | 3 | 8 (3/3) |
| 250 | head | head@4.0.1 (fc50f45a/289122cd); head@4.0.1 (fc50f45a/6b2bef15); 3/3 deployments matched | warm | 26 | 0 | 1 | 27 | 50 (26/27; incomplete) |
| 250 | control | control@4.0.1 (deabe970/289122cd); control@4.0.1 (deabe970/6b2bef15); 3/3 deployments matched | cold | 2 | 0 | 1 | 3 | 6 (2/3; incomplete) |
| 250 | control | control@4.0.1 (deabe970/289122cd); control@4.0.1 (deabe970/6b2bef15); 3/3 deployments matched | warm | 19 | 0 | 8 | 27 | 34 (19/27; incomplete) |
| 250 | pinned | pinned@4.0.0 (c823d174/289122cd); pinned@4.0.0 (c823d174/6b2bef15); 3/3 deployments matched | cold | 3 | 0 | 0 | 3 | 6 (3/3) |
| 250 | pinned | pinned@4.0.0 (c823d174/289122cd); pinned@4.0.0 (c823d174/6b2bef15); 3/3 deployments matched | warm | 26 | 0 | 1 | 27 | 52 (26/27; incomplete) |
| 250 | pi | pi 1.0.4 / no Effect (5526e585/866f1f7f); pi 1.0.4 / no Effect (5526e585/f1947b8f); 3/3 deployments matched | cold | 0 | 3 | 0 | 3 | 0 (3/3) |
| 250 | pi | pi 1.0.4 / no Effect (5526e585/866f1f7f); pi 1.0.4 / no Effect (5526e585/f1947b8f); 3/3 deployments matched | warm | 0 | 27 | 0 | 27 | 0 (27/27) |
| 250 | tardie | tardie@4.0.0-rc.115 (c8fbbb72/8a0cb11b); tardie@4.0.0-rc.115 (c8fbbb72/9f22f5b1); 3/3 deployments matched | cold | 2 | 0 | 1 | 3 | 2 (2/3; incomplete) |
| 250 | tardie | tardie@4.0.0-rc.115 (c8fbbb72/8a0cb11b); tardie@4.0.0-rc.115 (c8fbbb72/9f22f5b1); 3/3 deployments matched | warm | 25 | 0 | 2 | 27 | 36 (25/27; incomplete) |
| 1000 | base | base@4.0.1 (deabe970/6b2bef15); 3/3 deployments matched | cold | 3 | 0 | 0 | 3 | 10 (3/3) |
| 1000 | base | base@4.0.1 (deabe970/6b2bef15); 3/3 deployments matched | warm | 27 | 0 | 0 | 27 | 58 (27/27) |
| 1000 | head | head@4.0.1 (fc50f45a/6b2bef15); 3/3 deployments matched | cold | 2 | 0 | 1 | 3 | 4 (2/3; incomplete) |
| 1000 | head | head@4.0.1 (fc50f45a/6b2bef15); 3/3 deployments matched | warm | 25 | 0 | 2 | 27 | 62 (25/27; incomplete) |
| 1000 | control | control@4.0.1 (deabe970/6b2bef15); 3/3 deployments matched | cold | 3 | 0 | 0 | 3 | 10 (3/3) |
| 1000 | control | control@4.0.1 (deabe970/6b2bef15); 3/3 deployments matched | warm | 26 | 0 | 1 | 27 | 57 (26/27; incomplete) |
| 1000 | pinned | pinned@4.0.0 (c823d174/6b2bef15); 3/3 deployments matched | cold | 3 | 0 | 0 | 3 | 10 (3/3) |
| 1000 | pinned | pinned@4.0.0 (c823d174/6b2bef15); 3/3 deployments matched | warm | 27 | 0 | 0 | 27 | 82 (27/27) |
| 1000 | pi | pi 1.0.4 / no Effect (5526e585/f1947b8f); 3/3 deployments matched | cold | 0 | 3 | 0 | 3 | 0 (3/3) |
| 1000 | pi | pi 1.0.4 / no Effect (5526e585/f1947b8f); 3/3 deployments matched | warm | 0 | 27 | 0 | 27 | 0 (27/27) |
| 1000 | tardie | tardie@4.0.0-rc.115 (c8fbbb72/9f22f5b1); 3/3 deployments matched | cold | 3 | 0 | 0 | 3 | 27 (3/3) |
| 1000 | tardie | tardie@4.0.0-rc.115 (c8fbbb72/9f22f5b1); 3/3 deployments matched | warm | 26 | 1 | 0 | 27 | 109 (27/27) |

## Startup and storage

| Size | Role | Startup p50 ms (n/3) | Seed bytes p50 (n/3) | Post bytes p50 (n/3) |
|---:|---|---:|---:|---:|
| 50 | base | 100 (3/3) | 2224130 (3/3) | 3121150 (3/3) |
| 50 | head | 101 (3/3) | 2220030 (3/3) | 3121150 (3/3) |
| 50 | control | 94 (3/3) | 2220030 (3/3) | 3121150 (3/3) |
| 50 | pinned | 90 (3/3) | 2224130 (3/3) | 3125250 (3/3) |
| 50 | pi | 7 (3/3) | 241664 (3/3) | 409600 (3/3) |
| 50 | tardie | 60 (3/3) | 430080 (3/3) | 847872 (3/3) |
| 250 | base | 86 (3/3) | 8237060 (3/3) | 9158660 (3/3) |
| 250 | head | 91 (3/3) | 8245250 (3/3) | 9166850 (3/3) |
| 250 | control | 103 (3/3) | 8241150 (3/3) | 9162750 (3/3) |
| 250 | pinned | 92 (3/3) | 8241150 (3/3) | 9162750 (3/3) |
| 250 | pi | 11 (3/3) | 757760 (3/3) | 929792 (3/3) |
| 250 | tardie | 71 (3/3) | 1937410 (3/3) | 2527230 (3/3) |
| 1000 | base | 107 (3/3) | 30601200 (3/3) | 31543300 (3/3) |
| 1000 | head | 105 (3/3) | 30601200 (3/3) | 31535100 (3/3) |
| 1000 | control | 156 (3/3) | 30601200 (3/3) | 31539200 (3/3) |
| 1000 | pinned | 97 (3/3) | 30588900 (3/3) | 31522800 (3/3) |
| 1000 | pi | 8 (3/3) | 2711550 (3/3) | 2883580 (3/3) |
| 1000 | tardie | 95 (3/3) | 7671810 (3/3) | 8265730 (3/3) |

## All-owned DO window-observed cohort CPU

All-owned DO totals are window-observed CPU, summed across available Object windows. The separate per-Object total/10 diagnostic requires ten completed RPC receipts, cohort telemetryComplete=true, complete ten-row collection status, empty missing[] lists in both cohort and collection records, and CPU values for every saved event. This conservative gate suppresses raw and normalized cohort means even for ingress-only missing[] entries when all ten primary DO RPCs matched; it does not mark completed workloads failed or discard available RPC metrics or observed totals. A passed gate describes the saved evidence, never proof of exhaustive invocation capture. Incomplete totals remain observed lower bounds and are not promoted to complete per-turn CPU. Alarms are never allocated to cold or warm RPC metrics.

All roles include observed native alarms. Invocation CPU totals cover saved events across all owned DOs, including the Tardie Actor, and all outcomes. Alarm means eventType=alarm; absent event types mark alarm coverage incomplete. Counts remain available when CPU is missing. Collector bounds start at the cold RPC event timestamp (workloadFrom fallback if absent) through the role's 35s+2s tail, excluding prep and audit traces. These window-observed totals never establish exhaustive capture, even when telemetryComplete is true. Partial event CPU sums retain event/Object and telemetry coverage. When telemetry is incomplete, extra-trace classification is provisional: it is relative to matched RPC traces and can include unmatched RPC work, so extra totals are not labeled lower bounds for non-RPC work.

Alarm columns overlap the all-DO/extra columns and must not be added again.

normalizedTenTurnMeanCpuRatio is supplied by the collector as sum(each owned DO window-observed cohort CPU / median of that DO's ten reference CPU values) / 10. It has the same conservative telemetry/ten-turn gate as the raw cohort mean. The reported value and referenceEvidence are retained separately even when that gate fails. Passing the gate does not prove exhaustive CPU capture.

| Size | Role | Observed cohort CPU Σ ms | Gated observed total/10 p50 ms (n/3) | Observed DO invocations Σ | Observed extra CPU Σ ms | Observed extra invocations Σ |
|---:|---|---:|---:|---:|---:|---:|
| 50 | base | 7785 lower bound (3/3; incomplete; telemetry 2/3) | 302.1 (2/3) | 84 lower bound (3/3; incomplete; telemetry 2/3) | 295 (3/3; incomplete; telemetry 2/3) | 55 (3/3; incomplete; telemetry 2/3) |
| 50 | head | 8233 (3/3; telemetry 3/3) | 214.8 (3/3) | 89 (3/3; telemetry 3/3) | 222 (3/3; telemetry 3/3) | 59 (3/3; telemetry 3/3) |
| 50 | control | 8195 lower bound (3/3; incomplete; telemetry 1/3) | 203.2 (1/3) | 86 lower bound (3/3; incomplete; telemetry 1/3) | 218 (3/3; incomplete; telemetry 1/3) | 56 (3/3; incomplete; telemetry 1/3) |
| 50 | pinned | 9852 lower bound (3/3; incomplete; telemetry 1/3) | 205.6 (1/3) | 85 lower bound (3/3; incomplete; telemetry 1/3) | 377 (3/3; incomplete; telemetry 1/3) | 55 (3/3; incomplete; telemetry 1/3) |
| 50 | pi | 3077 lower bound (3/3; incomplete; telemetry 1/3) | 114.3 (1/3) | 27 lower bound (3/3; incomplete; telemetry 1/3) | 0 (3/3; incomplete; telemetry 1/3) | 0 (3/3; incomplete; telemetry 1/3) |
| 50 | tardie | 12884 lower bound (3/3; incomplete; telemetry 0/3) | unavailable (0/3) | 136 lower bound (3/3; incomplete; telemetry 0/3) | 11936 (3/3; incomplete; telemetry 0/3) | 106 (3/3; incomplete; telemetry 0/3) |
| 250 | base | 14031 lower bound (3/3; incomplete; telemetry 2/3) | 411.6 (2/3) | 88 lower bound (3/3; incomplete; telemetry 2/3) | 256 (3/3; incomplete; telemetry 2/3) | 58 (3/3; incomplete; telemetry 2/3) |
| 250 | head | 15100 lower bound (3/3; incomplete; telemetry 1/3) | 644.7 (1/3) | 85 lower bound (3/3; incomplete; telemetry 1/3) | 302 (3/3; incomplete; telemetry 1/3) | 56 (3/3; incomplete; telemetry 1/3) |
| 250 | control | 8721 lower bound (3/3; incomplete; telemetry 1/3) | 530.9 (1/3) | 72 lower bound (3/3; incomplete; telemetry 1/3) | 673 (3/3; incomplete; telemetry 1/3) | 51 (3/3; incomplete; telemetry 1/3) |
| 250 | pinned | 16653 lower bound (3/3; incomplete; telemetry 2/3) | 570.6 (2/3) | 85 lower bound (3/3; incomplete; telemetry 2/3) | 285 (3/3; incomplete; telemetry 2/3) | 56 (3/3; incomplete; telemetry 2/3) |
| 250 | pi | 5406 lower bound (3/3; incomplete; telemetry 1/3) | 213.9 (1/3) | 30 lower bound (3/3; incomplete; telemetry 1/3) | 0 (3/3; incomplete; telemetry 1/3) | 0 (3/3; incomplete; telemetry 1/3) |
| 250 | tardie | 18991 lower bound (3/3; incomplete; telemetry 1/3) | 643.1 (1/3) | 120 lower bound (3/3; incomplete; telemetry 1/3) | 17602 (3/3; incomplete; telemetry 1/3) | 93 (3/3; incomplete; telemetry 1/3) |
| 1000 | base | 16785 (3/3; telemetry 3/3) | 561 (3/3) | 86 (3/3; telemetry 3/3) | 159 (3/3; telemetry 3/3) | 56 (3/3; telemetry 3/3) |
| 1000 | head | 15938 lower bound (3/3; incomplete; telemetry 0/3) | unavailable (0/3) | 79 lower bound (3/3; incomplete; telemetry 0/3) | 140 (3/3; incomplete; telemetry 0/3) | 52 (3/3; incomplete; telemetry 0/3) |
| 1000 | control | 18882 lower bound (3/3; incomplete; telemetry 1/3) | 510.7 (1/3) | 83 lower bound (3/3; incomplete; telemetry 1/3) | 167 (3/3; incomplete; telemetry 1/3) | 54 (3/3; incomplete; telemetry 1/3) |
| 1000 | pinned | 33574 (3/3; telemetry 3/3) | 1105 (3/3) | 87 (3/3; telemetry 3/3) | 275 (3/3; telemetry 3/3) | 57 (3/3; telemetry 3/3) |
| 1000 | pi | 22300 (3/3; telemetry 3/3) | 840.8 (3/3) | 30 (3/3; telemetry 3/3) | 0 (3/3; telemetry 3/3) | 0 (3/3; telemetry 3/3) |
| 1000 | tardie | 58474 lower bound (3/3; incomplete; telemetry 1/3) | 2182.6 (1/3) | 154 lower bound (3/3; incomplete; telemetry 1/3) | 49890 (3/3; incomplete; telemetry 1/3) | 124 (3/3; incomplete; telemetry 1/3) |

| Size | Role | Normalized cohort mean median [p25, p75] (ms CPU / ms reference CPU; Objects/3) |
|---:|---|---:|
| 50 | base | 3.95625 [2.62812, 5.28437] (2/3) |
| 50 | head | 6.55692 [4.00131, 6.85846] (3/3) |
| 50 | control | 6.15758 [6.15758, 6.15758] (1/3) |
| 50 | pinned | 1.02035 [1.02035, 1.02035] (1/3) |
| 50 | pi | 3.74754 [3.74754, 3.74754] (1/3) |
| 50 | tardie | unavailable [unavailable, unavailable] (0/3) |
| 250 | base | 6.77712 [4.14641, 9.40784] (2/3) |
| 250 | head | 2.42368 [2.42368, 2.42368] (1/3) |
| 250 | control | 13.1086 [13.1086, 13.1086] (1/3) |
| 250 | pinned | 1.92202 [1.78303, 2.06101] (2/3) |
| 250 | pi | 7.13 [7.13, 7.13] (1/3) |
| 250 | tardie | 2.70441 [2.70441, 2.70441] (1/3) |
| 1000 | base | 19.3448 [10.9916, 19.5907] (3/3) |
| 1000 | head | unavailable [unavailable, unavailable] (0/3) |
| 1000 | control | 16.4742 [16.4742, 16.4742] (1/3) |
| 1000 | pinned | 24.0217 [14.2398, 24.5663] (3/3) |
| 1000 | pi | 19.3287 [17.1944, 20.5644] (3/3) |
| 1000 | tardie | 60.6244 [60.6244, 60.6244] (1/3) |

### tenTurnMeanCpuTimeMs: paired cohort ratios

| Size | Pair | Ratio median [p25, p75] (Object pairs/3) | Control/base spread | Status |
|---:|---|---:|---:|---|
| 50 | head/base | 1.01113 [1.00913, 1.01313] (2/3) | 0.960302 [0.960302, 0.960302] (1/3); range [0.960302, 0.960302] | incomplete |
| 50 | control/base | 0.960302 [0.960302, 0.960302] (1/3) | 0.960302 [0.960302, 0.960302] (1/3); range [0.960302, 0.960302] | incomplete |
| 50 | head/control | 1.05709 [1.05709, 1.05709] (1/3) | 0.960302 [0.960302, 0.960302] (1/3); range [0.960302, 0.960302] | incomplete |
| 50 | head/pi | 3.45932 [3.45932, 3.45932] (1/3) | unavailable [unavailable, unavailable] (0/3); range [unavailable, unavailable] | incomplete |
| 50 | head/tardie | unavailable [unavailable, unavailable] (0/3) | unavailable [unavailable, unavailable] (0/3); range [unavailable, unavailable] | incomplete |
| 50 | pinned/pi | unavailable [unavailable, unavailable] (0/3) | unavailable [unavailable, unavailable] (0/3); range [unavailable, unavailable] | incomplete |
| 50 | pinned/tardie | unavailable [unavailable, unavailable] (0/3) | unavailable [unavailable, unavailable] (0/3); range [unavailable, unavailable] | incomplete |
| 250 | head/base | 1.99227 [1.99227, 1.99227] (1/3) | unavailable [unavailable, unavailable] (0/3); range [unavailable, unavailable] | incomplete |
| 250 | control/base | 1.06265 [1.06265, 1.06265] (1/3) | 1.06265 [1.06265, 1.06265] (1/3); range [1.06265, 1.06265] | incomplete |
| 250 | head/control | unavailable [unavailable, unavailable] (0/3) | unavailable [unavailable, unavailable] (0/3); range [unavailable, unavailable] | incomplete |
| 250 | head/pi | unavailable [unavailable, unavailable] (0/3) | unavailable [unavailable, unavailable] (0/3); range [unavailable, unavailable] | incomplete |
| 250 | head/tardie | unavailable [unavailable, unavailable] (0/3) | unavailable [unavailable, unavailable] (0/3); range [unavailable, unavailable] | incomplete |
| 250 | pinned/pi | 2.78728 [2.78728, 2.78728] (1/3) | unavailable [unavailable, unavailable] (0/3); range [unavailable, unavailable] | incomplete |
| 250 | pinned/tardie | 0.927072 [0.927072, 0.927072] (1/3) | unavailable [unavailable, unavailable] (0/3); range [unavailable, unavailable] | incomplete |
| 1000 | head/base | unavailable [unavailable, unavailable] (0/3) | unavailable [unavailable, unavailable] (0/3); range [unavailable, unavailable] | incomplete |
| 1000 | control/base | 0.858175 [0.858175, 0.858175] (1/3) | 0.858175 [0.858175, 0.858175] (1/3); range [0.858175, 0.858175] | incomplete |
| 1000 | head/control | unavailable [unavailable, unavailable] (0/3) | unavailable [unavailable, unavailable] (0/3); range [unavailable, unavailable] | incomplete |
| 1000 | head/pi | unavailable [unavailable, unavailable] (0/3) | unavailable [unavailable, unavailable] (0/3); range [unavailable, unavailable] | incomplete |
| 1000 | head/tardie | unavailable [unavailable, unavailable] (0/3) | unavailable [unavailable, unavailable] (0/3); range [unavailable, unavailable] | incomplete |
| 1000 | pinned/pi | 1.43946 [1.27558, 1.94262] (3/3) | 0.858175 [0.858175, 0.858175] (1/3); range [0.858175, 0.858175] | incomplete |
| 1000 | pinned/tardie | 0.554522 [0.554522, 0.554522] (1/3) | unavailable [unavailable, unavailable] (0/3); range [unavailable, unavailable] | incomplete |

### normalizedTenTurnMeanCpuRatio: paired cohort ratios

| Size | Pair | Ratio median [p25, p75] (Object pairs/3) | Control/base spread | Status |
|---:|---|---:|---:|---|
| 50 | head/base | 1.09744 [1.09012, 1.10476] (2/3) | 0.931202 [0.931202, 0.931202] (1/3); range [0.931202, 0.931202] | incomplete |
| 50 | control/base | 0.931202 [0.931202, 0.931202] (1/3) | 0.931202 [0.931202, 0.931202] (1/3); range [0.931202, 0.931202] | incomplete |
| 50 | head/control | 1.1628 [1.1628, 1.1628] (1/3) | 0.931202 [0.931202, 0.931202] (1/3); range [0.931202, 0.931202] | incomplete |
| 50 | head/pi | 0.385774 [0.385774, 0.385774] (1/3) | unavailable [unavailable, unavailable] (0/3); range [unavailable, unavailable] | incomplete |
| 50 | head/tardie | unavailable [unavailable, unavailable] (0/3) | unavailable [unavailable, unavailable] (0/3); range [unavailable, unavailable] | incomplete |
| 50 | pinned/pi | unavailable [unavailable, unavailable] (0/3) | unavailable [unavailable, unavailable] (0/3); range [unavailable, unavailable] | incomplete |
| 50 | pinned/tardie | unavailable [unavailable, unavailable] (0/3) | unavailable [unavailable, unavailable] (0/3); range [unavailable, unavailable] | incomplete |
| 250 | head/base | 1.59906 [1.59906, 1.59906] (1/3) | unavailable [unavailable, unavailable] (0/3); range [unavailable, unavailable] | incomplete |
| 250 | control/base | 1.08889 [1.08889, 1.08889] (1/3) | 1.08889 [1.08889, 1.08889] (1/3); range [1.08889, 1.08889] | incomplete |
| 250 | head/control | unavailable [unavailable, unavailable] (0/3) | unavailable [unavailable, unavailable] (0/3); range [unavailable, unavailable] | incomplete |
| 250 | head/pi | unavailable [unavailable, unavailable] (0/3) | unavailable [unavailable, unavailable] (0/3); range [unavailable, unavailable] | incomplete |
| 250 | head/tardie | unavailable [unavailable, unavailable] (0/3) | unavailable [unavailable, unavailable] (0/3); range [unavailable, unavailable] | incomplete |
| 250 | pinned/pi | 0.308555 [0.308555, 0.308555] (1/3) | unavailable [unavailable, unavailable] (0/3); range [unavailable, unavailable] | incomplete |
| 250 | pinned/tardie | 0.813486 [0.813486, 0.813486] (1/3) | unavailable [unavailable, unavailable] (0/3); range [unavailable, unavailable] | incomplete |
| 1000 | head/base | unavailable [unavailable, unavailable] (0/3) | unavailable [unavailable, unavailable] (0/3); range [unavailable, unavailable] | incomplete |
| 1000 | control/base | 0.830492 [0.830492, 0.830492] (1/3) | 0.830492 [0.830492, 0.830492] (1/3); range [0.830492, 0.830492] | incomplete |
| 1000 | head/control | unavailable [unavailable, unavailable] (0/3) | unavailable [unavailable, unavailable] (0/3); range [unavailable, unavailable] | incomplete |
| 1000 | head/pi | unavailable [unavailable, unavailable] (0/3) | unavailable [unavailable, unavailable] (0/3); range [unavailable, unavailable] | incomplete |
| 1000 | head/tardie | unavailable [unavailable, unavailable] (0/3) | unavailable [unavailable, unavailable] (0/3); range [unavailable, unavailable] | incomplete |
| 1000 | pinned/pi | 1.15187 [0.691253, 1.37347] (3/3) | 0.830492 [0.830492, 0.830492] (1/3); range [0.830492, 0.830492] | incomplete |
| 1000 | pinned/tardie | 0.0735319 [0.0735319, 0.0735319] (1/3) | unavailable [unavailable, unavailable] (0/3); range [unavailable, unavailable] | incomplete |

Event CPU sums retain available values when other events lack CPU telemetry. Coverage is events with CPU / saved events, plus Objects with event lists / expected Objects and complete telemetry / expected Objects; missing event lists are not interpreted as zero work. With partial telemetry, extra traces can include unmatched RPC work and remain provisional observed totals.

| Size | Role | Observed all-DO event CPU Σ ms | Observed extra event CPU Σ ms | Observed alarms Σ count | Observed alarm CPU Σ ms | Observed extra alarms Σ count | Observed extra alarm CPU Σ ms |
|---:|---|---:|---:|---:|---:|---:|---:|
| 50 | base | 7785 lower bound (84/84; incomplete; telemetry 2/3) events; 3/3 Objects | 295 (55/55; incomplete; telemetry 2/3) events; 3/3 Objects | 55 lower bound (3/3; incomplete; telemetry 2/3) | 295 lower bound (55/55; incomplete; telemetry 2/3) events; 3/3 Objects | 55 (3/3; incomplete; telemetry 2/3) | 295 (55/55; incomplete; telemetry 2/3) events; 3/3 Objects |
| 50 | head | 8233 (89/89; telemetry 3/3) events; 3/3 Objects | 222 (59/59; telemetry 3/3) events; 3/3 Objects | 59 (3/3; telemetry 3/3) | 222 (59/59; telemetry 3/3) events; 3/3 Objects | 59 (3/3; telemetry 3/3) | 222 (59/59; telemetry 3/3) events; 3/3 Objects |
| 50 | control | 8195 lower bound (86/86; incomplete; telemetry 1/3) events; 3/3 Objects | 218 (56/56; incomplete; telemetry 1/3) events; 3/3 Objects | 56 lower bound (3/3; incomplete; telemetry 1/3) | 218 lower bound (56/56; incomplete; telemetry 1/3) events; 3/3 Objects | 56 (3/3; incomplete; telemetry 1/3) | 218 (56/56; incomplete; telemetry 1/3) events; 3/3 Objects |
| 50 | pinned | 9852 lower bound (85/85; incomplete; telemetry 1/3) events; 3/3 Objects | 377 (55/55; incomplete; telemetry 1/3) events; 3/3 Objects | 55 lower bound (3/3; incomplete; telemetry 1/3) | 377 lower bound (55/55; incomplete; telemetry 1/3) events; 3/3 Objects | 55 (3/3; incomplete; telemetry 1/3) | 377 (55/55; incomplete; telemetry 1/3) events; 3/3 Objects |
| 50 | pi | 3077 lower bound (27/27; incomplete; telemetry 1/3) events; 3/3 Objects | 0 (0/0; incomplete; telemetry 1/3) events; 3/3 Objects | 0 lower bound (3/3; incomplete; telemetry 1/3) | 0 lower bound (0/0; incomplete; telemetry 1/3) events; 3/3 Objects | 0 (3/3; incomplete; telemetry 1/3) | 0 (0/0; incomplete; telemetry 1/3) events; 3/3 Objects |
| 50 | tardie | 12884 lower bound (136/136; incomplete; telemetry 0/3) events; 3/3 Objects | 11936 (106/106; incomplete; telemetry 0/3) events; 3/3 Objects | 84 lower bound (3/3; incomplete; telemetry 0/3) | 11843 lower bound (84/84; incomplete; telemetry 0/3) events; 3/3 Objects | 84 (3/3; incomplete; telemetry 0/3) | 11843 (84/84; incomplete; telemetry 0/3) events; 3/3 Objects |
| 250 | base | 14031 lower bound (88/88; incomplete; telemetry 2/3) events; 3/3 Objects | 256 (58/58; incomplete; telemetry 2/3) events; 3/3 Objects | 58 lower bound (3/3; incomplete; telemetry 2/3) | 256 lower bound (58/58; incomplete; telemetry 2/3) events; 3/3 Objects | 58 (3/3; incomplete; telemetry 2/3) | 256 (58/58; incomplete; telemetry 2/3) events; 3/3 Objects |
| 250 | head | 15100 lower bound (85/85; incomplete; telemetry 1/3) events; 3/3 Objects | 302 (56/56; incomplete; telemetry 1/3) events; 3/3 Objects | 56 lower bound (3/3; incomplete; telemetry 1/3) | 302 lower bound (56/56; incomplete; telemetry 1/3) events; 3/3 Objects | 56 (3/3; incomplete; telemetry 1/3) | 302 (56/56; incomplete; telemetry 1/3) events; 3/3 Objects |
| 250 | control | 8721 lower bound (72/72; incomplete; telemetry 1/3) events; 3/3 Objects | 673 (51/51; incomplete; telemetry 1/3) events; 3/3 Objects | 50 lower bound (3/3; incomplete; telemetry 1/3) | 168 lower bound (50/50; incomplete; telemetry 1/3) events; 3/3 Objects | 50 (3/3; incomplete; telemetry 1/3) | 168 (50/50; incomplete; telemetry 1/3) events; 3/3 Objects |
| 250 | pinned | 16653 lower bound (85/85; incomplete; telemetry 2/3) events; 3/3 Objects | 285 (56/56; incomplete; telemetry 2/3) events; 3/3 Objects | 56 lower bound (3/3; incomplete; telemetry 2/3) | 285 lower bound (56/56; incomplete; telemetry 2/3) events; 3/3 Objects | 56 (3/3; incomplete; telemetry 2/3) | 285 (56/56; incomplete; telemetry 2/3) events; 3/3 Objects |
| 250 | pi | 5406 lower bound (30/30; incomplete; telemetry 1/3) events; 3/3 Objects | 0 (0/0; incomplete; telemetry 1/3) events; 3/3 Objects | 0 lower bound (3/3; incomplete; telemetry 1/3) | 0 lower bound (0/0; incomplete; telemetry 1/3) events; 3/3 Objects | 0 (3/3; incomplete; telemetry 1/3) | 0 (0/0; incomplete; telemetry 1/3) events; 3/3 Objects |
| 250 | tardie | 18991 lower bound (120/120; incomplete; telemetry 1/3) events; 3/3 Objects | 17602 (93/93; incomplete; telemetry 1/3) events; 3/3 Objects | 66 lower bound (3/3; incomplete; telemetry 1/3) | 17451 lower bound (66/66; incomplete; telemetry 1/3) events; 3/3 Objects | 66 (3/3; incomplete; telemetry 1/3) | 17451 (66/66; incomplete; telemetry 1/3) events; 3/3 Objects |
| 1000 | base | 16785 (86/86; telemetry 3/3) events; 3/3 Objects | 159 (56/56; telemetry 3/3) events; 3/3 Objects | 56 (3/3; telemetry 3/3) | 159 (56/56; telemetry 3/3) events; 3/3 Objects | 56 (3/3; telemetry 3/3) | 159 (56/56; telemetry 3/3) events; 3/3 Objects |
| 1000 | head | 15938 lower bound (79/79; incomplete; telemetry 0/3) events; 3/3 Objects | 140 (52/52; incomplete; telemetry 0/3) events; 3/3 Objects | 52 lower bound (3/3; incomplete; telemetry 0/3) | 140 lower bound (52/52; incomplete; telemetry 0/3) events; 3/3 Objects | 52 (3/3; incomplete; telemetry 0/3) | 140 (52/52; incomplete; telemetry 0/3) events; 3/3 Objects |
| 1000 | control | 18882 lower bound (83/83; incomplete; telemetry 1/3) events; 3/3 Objects | 167 (54/54; incomplete; telemetry 1/3) events; 3/3 Objects | 54 lower bound (3/3; incomplete; telemetry 1/3) | 167 lower bound (54/54; incomplete; telemetry 1/3) events; 3/3 Objects | 54 (3/3; incomplete; telemetry 1/3) | 167 (54/54; incomplete; telemetry 1/3) events; 3/3 Objects |
| 1000 | pinned | 33574 (87/87; telemetry 3/3) events; 3/3 Objects | 275 (57/57; telemetry 3/3) events; 3/3 Objects | 57 (3/3; telemetry 3/3) | 275 (57/57; telemetry 3/3) events; 3/3 Objects | 57 (3/3; telemetry 3/3) | 275 (57/57; telemetry 3/3) events; 3/3 Objects |
| 1000 | pi | 22300 (30/30; telemetry 3/3) events; 3/3 Objects | 0 (0/0; telemetry 3/3) events; 3/3 Objects | 0 (3/3; telemetry 3/3) | 0 (0/0; telemetry 3/3) events; 3/3 Objects | 0 (3/3; telemetry 3/3) | 0 (0/0; telemetry 3/3) events; 3/3 Objects |
| 1000 | tardie | 58474 lower bound (154/154; incomplete; telemetry 1/3) events; 3/3 Objects | 49890 (124/124; incomplete; telemetry 1/3) events; 3/3 Objects | 97 lower bound (3/3; incomplete; telemetry 1/3) | 49872 lower bound (97/97; incomplete; telemetry 1/3) events; 3/3 Objects | 97 (3/3; incomplete; telemetry 1/3) | 49872 (97/97; incomplete; telemetry 1/3) events; 3/3 Objects |

## Collection coverage

Expected/matched counts below come from the collector; the design requires 30 RPC rows per role/size. The summary's complete-collection count also requires an empty missing[] list, so ingress-only gaps remain incomplete even with ten matched primary DO RPCs and collector complete=true. Missing evidence lists can also be empty when collection is incomplete (for example an unstable final poll). Neither case changes completed workload receipts, and complete collection is not proof of exhaustive alarm capture.

| Size | Role | Complete collections (n/3) | Collector expected RPC Σ | Matched RPC Σ | Missing evidence rows Σ |
|---:|---|---:|---:|---:|---:|
| 50 | base | 2/3 | 30 (3/3) | 29 (3/3) | 1 (3/3) |
| 50 | head | 3/3 | 30 (3/3) | 30 (3/3) | 0 (3/3) |
| 50 | control | 1/3 | 30 (3/3) | 30 (3/3) | 3 (3/3) |
| 50 | pinned | 1/3 | 30 (3/3) | 30 (3/3) | 2 (3/3) |
| 50 | pi | 1/3 | 30 (3/3) | 27 (3/3) | 3 (3/3) |
| 50 | tardie | 0/3 | 30 (3/3) | 30 (3/3) | 5 (3/3) |
| 250 | base | 2/3 | 30 (3/3) | 30 (3/3) | 2 (3/3) |
| 250 | head | 1/3 | 30 (3/3) | 29 (3/3) | 4 (3/3) |
| 250 | control | 1/3 | 30 (3/3) | 21 (3/3) | 13 (3/3) |
| 250 | pinned | 2/3 | 30 (3/3) | 29 (3/3) | 1 (3/3) |
| 250 | pi | 1/3 | 30 (3/3) | 30 (3/3) | 4 (3/3) |
| 250 | tardie | 1/3 | 30 (3/3) | 27 (3/3) | 5 (3/3) |
| 1000 | base | 3/3 | 30 (3/3) | 30 (3/3) | 0 (3/3) |
| 1000 | head | 0/3 | 30 (3/3) | 27 (3/3) | 6 (3/3) |
| 1000 | control | 1/3 | 30 (3/3) | 29 (3/3) | 2 (3/3) |
| 1000 | pinned | 3/3 | 30 (3/3) | 30 (3/3) | 0 (3/3) |
| 1000 | pi | 3/3 | 30 (3/3) | 30 (3/3) | 0 (3/3) |
| 1000 | tardie | 1/3 | 30 (3/3) | 30 (3/3) | 3 (3/3) |

## Resolved deployment manifests

Only manifests resolved for saved deployments appear below. The full candidate inventory and unresolved per-Object provenance remain in summary.json; current and archive copies can identify the same deployed bytes.

| Role | Target runtime | Bench/wrapper SHA prefixes | Bench raw/gzip bytes | Wrapper raw/gzip bytes | Manifest |
|---|---|---|---:|---:|---|
| base | base@4.0.1 | deabe970/6b2bef15 | 3442269/658275 | 12853/3216 | ../builds/base.json |
| base | base@4.0.1 | deabe970/289122cd | 3442269/658275 | 11443/2743 | ../builds/wrapper-v5/base/manifest.json |
| base | base@4.0.1 | deabe970/6b2bef15 | 3442269/658275 | 12853/3216 | ../builds/wrapper-v6/base/manifest.json |
| base | base@4.0.1 | deabe970/6b2bef15 | 3442269/658275 | 12853/3216 | ../builds/wrapper-v7/base/manifest.json |
| head | head@4.0.1 | fc50f45a/6b2bef15 | 3455241/660855 | 12853/3216 | ../builds/head.json |
| head | head@4.0.1 | fc50f45a/289122cd | 3455241/660855 | 11443/2743 | ../builds/wrapper-v5/head/manifest.json |
| head | head@4.0.1 | fc50f45a/6b2bef15 | 3455241/660855 | 12853/3216 | ../builds/wrapper-v6/head/manifest.json |
| head | head@4.0.1 | fc50f45a/6b2bef15 | 3455241/660855 | 12853/3216 | ../builds/wrapper-v7/head/manifest.json |
| control | control@4.0.1 | deabe970/6b2bef15 | 3442269/658275 | 12853/3216 | ../builds/control.json |
| control | control@4.0.1 | deabe970/289122cd | 3442269/658275 | 11443/2743 | ../builds/wrapper-v5/control/manifest.json |
| control | control@4.0.1 | deabe970/6b2bef15 | 3442269/658275 | 12853/3216 | ../builds/wrapper-v6/control/manifest.json |
| control | control@4.0.1 | deabe970/6b2bef15 | 3442269/658275 | 12853/3216 | ../builds/wrapper-v7/control/manifest.json |
| pinned | pinned@4.0.0 | c823d174/6b2bef15 | 3432935/655834 | 12853/3216 | ../builds/pinned.json |
| pinned | pinned@4.0.0 | c823d174/289122cd | 3432935/655834 | 11443/2743 | ../builds/wrapper-v5/pinned/manifest.json |
| pinned | pinned@4.0.0 | c823d174/6b2bef15 | 3432935/655834 | 12853/3216 | ../builds/wrapper-v6/pinned/manifest.json |
| pinned | pinned@4.0.0 | c823d174/6b2bef15 | 3432935/655834 | 12853/3216 | ../builds/wrapper-v7/pinned/manifest.json |
| pi | pi 1.0.4 / no Effect | 5526e585/f1947b8f | 879497/158081 | 11537/2762 | ../builds/pi.json |
| pi | pi 1.0.4 / no Effect | 5526e585/866f1f7f | 879497/158081 | 11428/2733 | ../builds/wrapper-v5/pi/manifest.json |
| pi | pi 1.0.4 / no Effect | 5526e585/f1947b8f | 879497/158081 | 11537/2762 | ../builds/wrapper-v6/pi/manifest.json |
| pi | pi 1.0.4 / no Effect | 5526e585/f1947b8f | 879497/158081 | 11537/2762 | ../builds/wrapper-v7/pi/manifest.json |
| tardie | tardie@4.0.0-rc.115 | c8fbbb72/9f22f5b1 | 1689120/342158 | 18124/3832 | ../builds/tardie.json |
| tardie | tardie@4.0.0-rc.115 | c8fbbb72/8a0cb11b | 1689120/342158 | 15694/3098 | ../builds/wrapper-v5/tardie/manifest.json |
| tardie | tardie@4.0.0-rc.115 | c8fbbb72/9f22f5b1 | 1689120/342158 | 18124/3832 | ../builds/wrapper-v7/tardie/manifest.json |

## Completeness

| Issue | Count |
|---|---:|
| telemetry_incomplete | 27 |

Recorded measurement failures: 0. Row status counts: {"complete":503,"incomplete":37}.

Per-Object values, ratio operands, input status, missing/failed row reasons, invocation outcomes, and all quantiles are retained in summary.json.
