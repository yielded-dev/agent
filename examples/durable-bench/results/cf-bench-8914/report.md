# Deployed durable-bench and Effect #8914

**Effect #8914 does not demonstrate a deployed turn-latency win at 50, 250 or 1,000 historical turns.** Its paired warm client ratios against merge-base remain within the identical-code control spread, and cold results do not establish a consistent improvement. The CPU breakdown also does not establish a PR reduction in either warm or cold turns. This is an unresolved effect, not proof that the PR has no benefit.

At 50 and 250 turns, #8914's warm client medians are **709 ms and 938 ms**, versus pi's **462 ms and 567 ms**. At 1,000 turns, #8914 is numerically lower at **899 ms** versus pi's **1,142 ms**, but merge-base is already **884 ms**, the ordering reverses in one of the three Object pairs, and the control variation is too large to establish a win. Pinned Yielded 4.0.0 has warm medians of **741 / 941 / 1,378 ms**. Tardie has the largest warm client median at all three sizes: **2,241 / 1,769 / 3,024 ms**.

The local three-way ranking is therefore not a reliable guide to deployed latency. A whole-framework **CPU** ranking is also not established here: Tardie moves substantial work into separate alarm invocations, and the retained invocation exports have gaps. Its small Thread RPC CPU is not its whole-turn compute cost. The tables expose the requested invocation breakdown and bounded, observed CPU totals without conflating them.

These whole-turn results do not justify prioritising work to land #8914 as Yielded's latency optimisation. A focused deployed experiment on the large DO wall-minus-CPU component is the next useful step before choosing an upstream change or local restructuring. Storage durability gates, alarm queueing and scheduling remain hypotheses; this run does not identify the cause of that time. The PR's isolated Effect improvements may still be useful, but they do not establish the payoff for this workload.

<!-- RESULTS -->

## Turn latency and CPU breakdown

All times are milliseconds, median [Q1–Q3]. These descriptive warm tables pool the nine warm turns from each of three Objects. Cold has one turn per Object. The client column retains all valid receipts; DO columns use matched telemetry. `n` gives client / DO turn counts. **Residuals are calculated for each paired invocation before taking quantiles: do not subtract or add the table medians.** Different coverage can also make the marginal DO median exceed the client median.

Tardie CPU below is the main Thread RPC only. Much of its computation runs in separate alarm/Actor invocations. Its small RPC CPU number is not its whole-turn compute cost, and its wall-minus-CPU residual can include time while those other invocations compute.

### Warm: turns 2–10

| Historical turns | Target / Effect build | Client wall | DO wall | DO CPU | DO wall − CPU | Client wall − DO wall | n: client / DO |
|---:|---|---:|---:|---:|---:|---:|---:|
| 50 | Yielded 4.0.0 | 741 [483–796.5] | 591 [412–620.5] | 302 [232–331] | 272 [178–302.5] | 128 [72–187] | 27 / 27 |
| 50 | Yielded merge-base | 724 [692–795] | 537 [505.5–600.5] | 199 [170–288.5] | 328 [283.5–335.5] | 191 [188–196.5] | 27 / 27 |
| 50 | Yielded #8914 | 709 [643.5–812.5] | 530 [485–613.5] | 211 [184.5–288] | 307 [294.5–322] | 182 [137–196.5] | 27 / 27 |
| 50 | Yielded identical control | 705 [672–823.5] | 524 [478.5–629] | 180 [157–310.5] | 330 [318–344.5] | 187 [180.5–196] | 27 / 27 |
| 50 | pi-durable 1.0.4 | 462 [444.5–481] | 290 [273.5–320] | 99.5 [93.8–114] | 183.5 [179–190] | 184 [124.8–187.2] | 27 / 24 |
| 50 | tardie 0.44.0 | 2,241 [2,113–2,444] | 2,075 [1,931.5–2,252] | 9 [7–10.5] | 2,066 [1,924.5–2,242.5] | 182 [167–200.5] | 27 / 27 |
| 250 | Yielded 4.0.0 | 941 [870.5–1,057] | 784.5 [733.2–865.2] | 515 [447–575.2] | 240.5 [223.5–353.8] | 143 [128–183] | 27 / 26 |
| 250 | Yielded merge-base | 892 [771.5–1,019] | 753 [642–842] | 429 [333–531.5] | 311 [242–371.5] | 133 [128–183.5] | 27 / 27 |
| 250 | Yielded #8914 | 938 [915–1,037] | 792 [717–854.8] | 466.5 [302.2–568.2] | 348 [245–411.8] | 129.5 [124.2–196.5] | 27 / 26 |
| 250 | Yielded identical control | 707 [670.5–761.5] | 588 [512.5–659] | 313 [219–434.5] | 254 [223.5–285] | 142 [126–178] | 27 / 19 |
| 250 | pi-durable 1.0.4 | 567 [527.5–600.5] | 355 [334.5–387.5] | 157 [143.5–188.5] | 194 [188.5–199.5] | 195 [188–197.5] | 27 / 27 |
| 250 | tardie 0.44.0 | 1,769 [1,343–2,307] | 1,928 [1,237–2,219] | 16 [14–24] | 1,902 [1,220–2,205] | 122 [119–183] | 27 / 25 |
| 1,000 | Yielded 4.0.0 | 1,378 [1,228–1,531] | 1,334 [1,148–1,479.5] | 1,058 [928.5–1,211.5] | 237 [201.5–267.5] | 62 [43.5–99.5] | 27 / 27 |
| 1,000 | Yielded merge-base | 884 [782.5–988.5] | 742 [677–839] | 466 [444.5–552.5] | 258 [236–282] | 90 [79–193.5] | 27 / 27 |
| 1,000 | Yielded #8914 | 899 [852.5–995.5] | 836 [768–865] | 513 [480–621] | 306 [236–313] | 97 [22–136] | 27 / 25 |
| 1,000 | Yielded identical control | 920 [829–1,206.5] | 815.5 [774.8–1,084.2] | 485 [454.8–816.5] | 299.5 [272–341] | 104 [31.5–108.8] | 27 / 26 |
| 1,000 | pi-durable 1.0.4 | 1,142 [760–1,361.5] | 912 [605.5–1,169.5] | 754 [462–887] | 155 [137.5–296.5] | 131 [93.5–189] | 27 / 27 |
| 1,000 | tardie 0.44.0 | 3,024 [2,468–4,176.5] | 2,973 [2,365–4,114] | 48 [37.5–56.5] | 2,890 [2,284–4,069.5] | 89 [48–94] | 27 / 27 |

### Cold: open plus first turn

| Historical turns | Target / Effect build | Client wall | DO wall | DO CPU | DO wall − CPU | Client wall − DO wall | n: client / DO |
|---:|---|---:|---:|---:|---:|---:|---:|
| 50 | Yielded 4.0.0 | 1,114 [854.5–1,385] | 924 [729.5–1,225.5] | 614 [466.5–778.5] | 310 [263–447] | 129 [94.5–159.5] | 3 / 3 |
| 50 | Yielded merge-base | 1,224 [1,090.5–1,255] | 1,063 [1,048.5–1,077.5] | 587.5 [533.2–641.8] | 475.5 [406.8–544.2] | 192 [191–193] | 3 / 2 |
| 50 | Yielded #8914 | 935 [905–1,164] | 751 [744–972.5] | 376 [369.5–598] | 374 [367.5–381] | 184 [161–191.5] | 3 / 3 |
| 50 | Yielded identical control | 1,177 [1,049–1,220] | 985 [863.5–1,032.5] | 582 [480.5–639.5] | 383 [373–393] | 183 [181–187.5] | 3 / 3 |
| 50 | pi-durable 1.0.4 | 657 [654.5–669] | 471 [459–511] | 131 [130–145] | 340 [314–381] | 186 [158–195.5] | 3 / 3 |
| 50 | tardie 0.44.0 | 2,462 [2,446.5–2,685] | 2,299 [2,259.5–2,512] | 205 [183.5–272] | 2,094 [2,076–2,240] | 183 [173–197] | 3 / 3 |
| 250 | Yielded 4.0.0 | 1,525 [1,421–1,568.5] | 1,335 [1,253.5–1,379] | 907 [901–998] | 334 [305.5–381] | 189 [167–189.5] | 3 / 3 |
| 250 | Yielded merge-base | 1,422 [1,207.5–1,439.5] | 1,237 [1,053.5–1,280.5] | 785 [649–866.5] | 376 [366.5–414] | 133 [128–159] | 3 / 3 |
| 250 | Yielded #8914 | 1,596 [1,490–1,609.5] | 1,456 [1,317.5–1,465.5] | 1,079 [882.5–1,126.5] | 396 [339–444.5] | 167 [144–186] | 3 / 3 |
| 250 | Yielded identical control | 1,079 [987–1,310] | 1,062 [890–1,234] | 786.5 [611.8–961.2] | 275.5 [272.8–278.2] | 156 [145.5–166.5] | 3 / 2 |
| 250 | pi-durable 1.0.4 | 891 [800.5–1,189] | 693 [606.5–998] | 224 [207.5–230] | 469 [376.5–790.5] | 190 [187–194] | 3 / 3 |
| 250 | tardie 0.44.0 | 2,496 [1,997.5–2,780] | 2,170.5 [1,784.8–2,556.2] | 465 [426–504] | 1,705.5 [1,358.8–2,052.2] | 111 [105.5–116.5] | 3 / 2 |
| 1,000 | Yielded 4.0.0 | 1,936 [1,873–2,014] | 1,847 [1,777.5–1,871.5] | 1,470 [1,452.5–1,515.5] | 335 [286.5–373.5] | 102 [71–173.5] | 3 / 3 |
| 1,000 | Yielded merge-base | 1,411 [1,378.5–1,416.5] | 1,265 [1,243–1,280] | 872 [870–885] | 393 [358–410] | 116 [98.5–158.5] | 3 / 3 |
| 1,000 | Yielded #8914 | 1,363 [1,249.5–1,410.5] | 1,155.5 [1,098.2–1,212.8] | 816 [765.5–866.5] | 339.5 [332.8–346.2] | 94 [93.5–94.5] | 3 / 2 |
| 1,000 | Yielded identical control | 1,520 [1,463–1,559] | 1,460 [1,344.5–1,470] | 929 [873.5–1,057.5] | 411 [342.5–481] | 138 [89–157.5] | 3 / 3 |
| 1,000 | pi-durable 1.0.4 | 1,541 [1,244–1,645] | 1,462 [1,146–1,511] | 1,075 [815.5–1,078.5] | 380 [327–432.5] | 117 [98–153] | 3 / 3 |
| 1,000 | tardie 0.44.0 | 8,904 [6,798.5–8,988.5] | 8,803 [6,730.5–8,890.5] | 2,347 [1,824–2,911] | 5,328 [4,342.5–5,979.5] | 95 [65–98] | 3 / 3 |

## Paired comparisons and controls

Warm ratios use paired **Object medians**, not the pooled medians above; cold ratios pair individual cold turns. Ratios below 1 favour the numerator. Every ratio carries the matched identical-code control. The observed envelope includes the full control/base range and its reciprocal around 1, so a biased control median counts as variation. It is descriptive, not a confidence interval or a guaranteed bound on Cloudflare variability. `n` is the number of eligible Object pairs.

### #8914 / merge-base

| Historical turns | Metric | Warm head / base | Warm control / base and envelope | Cold head / base | Cold control / base and envelope |
|---:|---|---|---|---|---|
| 50 | Client wall | 1.017 [0.962–1.026]; n=3 | 0.979 [0.966–1.044]; observed envelope 0.901–1.11; n=3 | 0.914 [0.821–1.026]; n=3 | 1.032 [0.874–1.131]; observed envelope 0.716–1.396; n=3 |
| 50 | DO wall | 1.008 [0.969–1.032]; n=3 | 0.956 [0.951–1.053]; observed envelope 0.87–1.15; n=3 | 0.921 [0.804–1.038]; n=2 | 0.862 [0.771–0.953]; observed envelope 0.679–1.472; n=2 |
| 50 | DO CPU | 1.045 [0.998–1.097]; n=3 | 1.006 [0.913–1.066]; observed envelope 0.819–1.221; n=3 | 0.968 [0.863–1.073]; n=2 | 0.896 [0.844–0.949]; observed envelope 0.791–1.264; n=2 |
| 50 | Client wall / own reference | 1.077 [1.07–1.427]; n=3 | 1.007 [0.997–1.155]; observed envelope 0.768–1.302; n=3 | 1.186 [0.949–1.204]; n=3 | 1.18 [0.92–1.24]; observed envelope 0.661–1.513; n=3 |
| 250 | Client wall | 1.143 [1.02–1.229]; n=3 | 0.756 [0.752–0.86]; observed envelope 0.747–1.338; n=3 | 1.095 [1.034–1.365]; n=3 | 0.901 [0.821–0.992]; observed envelope 0.741–1.35; n=3 |
| 250 | DO wall | 1.134 [1.001–1.254]; n=3 | 0.82 [0.803–0.837]; observed envelope 0.786–1.273; n=2 | 1.114 [1.034–1.394]; n=3 | 0.981 [0.903–1.059]; observed envelope 0.825–1.212; n=2 |
| 250 | DO CPU | 0.969 [0.771–1.525]; n=3 | 0.853 [0.811–0.895]; observed envelope 0.768–1.301; n=2 | 1.138 [1.006–1.713]; n=3 | 1.149 [1.001–1.298]; observed envelope 0.691–1.447; n=2 |
| 250 | Client wall / own reference | 0.942 [0.781–1.043]; n=3 | 1.153 [0.981–1.232]; observed envelope 0.763–1.31; n=3 | 1.17 [1.105–1.299]; n=3 | 0.875 [0.826–1.129]; observed envelope 0.723–1.383; n=3 |
| 1,000 | Client wall | 1.01 [1.005–1.062]; n=3 | 1.036 [1–1.226]; observed envelope 0.706–1.416; n=3 | 1.013 [0.909–1.019]; n=3 | 1.129 [1.059–1.131]; observed envelope 0.883–1.133; n=3 |
| 1,000 | DO wall | 1.165 [1.122–1.219]; n=3 | 1.133 [1.114–1.305]; observed envelope 0.677–1.477; n=3 | 0.904 [0.854–0.954]; n=2 | 1.149 [1.138–1.159]; observed envelope 0.855–1.17; n=2 |
| 1,000 | DO CPU | 1.218 [1.147–1.271]; n=3 | 1.03 [1.018–1.387]; observed envelope 0.574–1.743; n=3 | 0.938 [0.881–0.995]; n=2 | 1.216 [1.141–1.291]; observed envelope 0.732–1.366; n=2 |
| 1,000 | Client wall / own reference | 1.959 [1.2–2.098]; n=3 | 1.191 [0.802–2.19]; observed envelope 0.314–3.189; n=3 | 1.425 [1.09–1.466]; n=3 | 1.348 [1.125–1.749]; observed envelope 0.465–2.15; n=3 |

The reference-normalised row is diagnostic. Reference execution and HTTP routing differ across Objects, so it cannot supply a universally corrected ranking or override an unresolved raw-latency result.

### Yielded / other targets: client latency

| Historical turns | Comparison | Warm ratio | Warm control / base and envelope | Cold ratio | Cold control / base and envelope |
|---:|---|---|---|---|---|
| 50 | Yielded #8914 / pi-durable 1.0.4 | 1.442 [1.417–1.714]; n=3 | 0.979 [0.966–1.044]; observed envelope 0.901–1.11; n=3 | 1.434 [1.383–1.74]; n=3 | 1.032 [0.874–1.131]; observed envelope 0.716–1.396; n=3 |
| 50 | Yielded #8914 / tardie 0.44.0 | 0.32 [0.313–0.325]; n=3 | 0.979 [0.966–1.044]; observed envelope 0.901–1.11; n=3 | 0.38 [0.37–0.429]; n=3 | 1.032 [0.874–1.131]; observed envelope 0.716–1.396; n=3 |
| 50 | Yielded 4.0.0 / pi-durable 1.0.4 | 1.656 [1.231–1.787]; n=3 | 0.979 [0.966–1.044]; observed envelope 0.901–1.11; n=3 | 1.636 [1.274–2.078]; n=3 | 1.032 [0.874–1.131]; observed envelope 0.716–1.396; n=3 |
| 50 | Yielded 4.0.0 / tardie 0.44.0 | 0.31 [0.247–0.336]; n=3 | 0.979 [0.966–1.044]; observed envelope 0.901–1.11; n=3 | 0.383 [0.312–0.532]; n=3 | 1.032 [0.874–1.131]; observed envelope 0.716–1.396; n=3 |
| 250 | Yielded #8914 / pi-durable 1.0.4 | 1.748 [1.716–1.766]; n=3 | 0.756 [0.752–0.86]; observed envelope 0.747–1.338; n=3 | 1.791 [1.441–1.87]; n=3 | 0.901 [0.821–0.992]; observed envelope 0.741–1.35; n=3 |
| 250 | Yielded #8914 / tardie 0.44.0 | 0.531 [0.475–0.615]; n=3 | 0.756 [0.752–0.86]; observed envelope 0.747–1.338; n=3 | 0.65 [0.586–0.787]; n=3 | 0.901 [0.821–0.992]; observed envelope 0.741–1.35; n=3 |
| 250 | Yielded 4.0.0 / pi-durable 1.0.4 | 1.655 [1.624–1.78]; n=3 | 0.756 [0.752–0.86]; observed envelope 0.747–1.338; n=3 | 1.809 [1.347–1.979]; n=3 | 0.901 [0.821–0.992]; observed envelope 0.741–1.35; n=3 |
| 250 | Yielded 4.0.0 / tardie 0.44.0 | 0.493 [0.438–0.642]; n=3 | 0.756 [0.752–0.86]; observed envelope 0.747–1.338; n=3 | 0.528 [0.527–0.772]; n=3 | 0.901 [0.821–0.992]; observed envelope 0.741–1.35; n=3 |
| 1,000 | Yielded #8914 / pi-durable 1.0.4 | 0.835 [0.756–1.001]; n=3 | 1.036 [1–1.226]; observed envelope 0.706–1.416; n=3 | 0.946 [0.798–1.193]; n=3 | 1.129 [1.059–1.131]; observed envelope 0.883–1.133; n=3 |
| 1,000 | Yielded #8914 / tardie 0.44.0 | 0.303 [0.269–0.313]; n=3 | 1.036 [1–1.226]; observed envelope 0.706–1.416; n=3 | 0.161 [0.157–0.201]; n=3 | 1.129 [1.059–1.131]; observed envelope 0.883–1.133; n=3 |
| 1,000 | Yielded 4.0.0 / pi-durable 1.0.4 | 1.338 [1.111–1.614]; n=3 | 1.036 [1–1.226]; observed envelope 0.706–1.416; n=3 | 1.358 [1.232–1.634]; n=3 | 1.129 [1.059–1.131]; observed envelope 0.883–1.133; n=3 |
| 1,000 | Yielded 4.0.0 / tardie 0.44.0 | 0.396 [0.386–0.46]; n=3 | 1.036 [1–1.226]; observed envelope 0.706–1.416; n=3 | 0.231 [0.217–0.322]; n=3 | 1.129 [1.059–1.131]; observed envelope 0.883–1.133; n=3 |

Full per-Object results, all normalised metrics, eligibility counts, HTTP colo histograms and observed alarm CPU are in [tables.md](run/tables.md) and [summary.json](run/summary.json).

## Storage, upload startup and bundles

Storage is the original bench’s allocated SQLite `databaseSize`, in bytes; Tardie includes its Actor directory database. Growth is `(post − seed) / 10`, summarised across three Objects. **Growth is not bytes written, and row/batch counts are not committed transaction counts.** Startup is Cloudflare’s Worker upload startup metric, separately measured from the cold turn.

| Historical turns | Target / Effect build | Seeded SQLite bytes | Net growth per turn, bytes | Worker startup, ms | Objects / uploads |
|---:|---|---:|---:|---:|---:|
| 50 | Yielded 4.0.0 | 2,224,128 [2,224,128–2,224,128] | 90,112 [89,907.2–90,112] | 90 [88.5–113.5] | 3 / 3 |
| 50 | Yielded merge-base | 2,224,128 [2,222,080–2,226,176] | 89,702.4 [89,702.4–89,702.4] | 100 [94.5–104] | 3 / 3 |
| 50 | Yielded #8914 | 2,220,032 [2,220,032–2,222,080] | 89,702.4 [89,497.6–89,907.2] | 101 [93.5–145.5] | 3 / 3 |
| 50 | Yielded identical control | 2,220,032 [2,220,032–2,222,080] | 90,112 [89,907.2–90,112] | 94 [90–99.5] | 3 / 3 |
| 50 | pi-durable 1.0.4 | 241,664 [241,664–241,664] | 16,793.6 [16,793.6–16,793.6] | 7 [6.5–8] | 3 / 3 |
| 50 | tardie 0.44.0 | 430,080 [430,080–430,080] | 41,779.2 [41,779.2–41,779.2] | 60 [59.5–79] | 3 / 3 |
| 250 | Yielded 4.0.0 | 8,241,152 [8,241,152–8,243,200] | 92,160 [91,955.2–92,160] | 92 [91–93.5] | 3 / 3 |
| 250 | Yielded merge-base | 8,237,056 [8,237,056–8,241,152] | 92,160 [91,750.4–92,364.8] | 86 [84.5–92] | 3 / 3 |
| 250 | Yielded #8914 | 8,245,248 [8,243,200–8,247,296] | 92,160 [92,160–92,364.8] | 91 [89.5–168.5] | 3 / 3 |
| 250 | Yielded identical control | 8,241,152 [8,241,152–8,245,248] | 92,160 [91,955.2–92,160] | 103 [96.5–141] | 3 / 3 |
| 250 | pi-durable 1.0.4 | 757,760 [757,760–757,760] | 17,203.2 [17,203.2–17,203.2] | 11 [8.5–12] | 3 / 3 |
| 250 | tardie 0.44.0 | 1,937,408 [1,937,408–1,937,408] | 58,982.4 [58,982.4–58,982.4] | 71 [68–108] | 3 / 3 |
| 1,000 | Yielded 4.0.0 | 30,588,928 [30,586,880–30,593,024] | 93,388.8 [93,388.8–93,593.6] | 97 [95.5–98.5] | 3 / 3 |
| 1,000 | Yielded merge-base | 30,601,216 [30,599,168–30,605,312] | 93,388.8 [92,979.2–94,412.8] | 107 [105–108] | 3 / 3 |
| 1,000 | Yielded #8914 | 30,601,216 [30,597,120–30,603,264] | 93,388.8 [92,979.2–93,593.6] | 105 [103–138] | 3 / 3 |
| 1,000 | Yielded identical control | 30,601,216 [30,597,120–30,603,264] | 93,388.8 [92,979.2–94,003.2] | 156 [125–175] | 3 / 3 |
| 1,000 | pi-durable 1.0.4 | 2,711,552 [2,711,552–2,711,552] | 17,203.2 [17,203.2–17,203.2] | 8 [8–10.5] | 3 / 3 |
| 1,000 | tardie 0.44.0 | 7,671,808 [7,671,808–7,671,808] | 59,392 [58,982.4–59,392] | 95 [82–108] | 3 / 3 |

Each original `bench.mjs` is uploaded unchanged with a separate wrapper. Wrapper revisions add preparation/inspection helpers; the turn implementation remains the same. Full wrapper hashes and exact bytes for each upload are in [builds](builds/) and the per-Object deployment identities. The baseline and control use identical module bytes.

| Target / Effect build | Original benchmark bytes: raw / gzip | Original benchmark SHA-256 |
|---|---:|---|
| Yielded 4.0.0 | 3,432,935 / 655,834 | `c823d1745273673543621a777765ef3531a3392682dbc3bddea3981c5000defb` |
| Yielded merge-base | 3,442,269 / 658,275 | `deabe97056af77fd01a5b8799d9bece4f1ce3ae68ee509aa45f70630849269db` |
| Yielded #8914 | 3,455,241 / 660,855 | `fc50f45a421a0792e153e3c91a344f38a51a9569cfd36b4d54b0a479ee2b23c8` |
| Yielded identical control | 3,442,269 / 658,275 | `deabe97056af77fd01a5b8799d9bece4f1ce3ae68ee509aa45f70630849269db` |
| pi-durable 1.0.4 | 879,497 / 158,081 | `5526e585eb8bccfdc9b3eb4e68cf58cb57c97697abce9a27951fc7486efb3985` |
| tardie 0.44.0 | 1,689,120 / 342,158 | `c8fbbb722e67090856e46048ab8197f49f197b64900aa4513c48adba4a909484` |

## Observed Object CPU and alarm overlap

The following CPU totals cover each ten-turn Object window and its bounded alarm tail, excluding the reference/preparation/audit RPCs. They include cold and warm together and are **window-observed totals, not complete CPU costs or per-turn warm means**. Missing main, reference or ingress logs are recorded separately in the raw collection status; even complete main-RPC matching does not prove exhaustive alarm capture. Temporal overlap does not establish a delay or its cause.

The final column retains the in-Object `Date.now()` interval, warm / cold median [Q1–Q3]. Its zero values are clock behaviour, not zero execution time.

| Historical turns | Target / Effect build | Observed owned-DO CPU by sample, ms | Warm turns with observed alarm overlap / matched DO rows | Cold overlap / matched DO rows | In-Object clock: warm / cold, ms |
|---:|---|---|---:|---:|---|
| 50 | Yielded 4.0.0 | 1: 3,543; 2: 4,253; 3: 2,056 | 27 / 27 | 3 / 3 | 0 [0–0] / 0 [0–0] |
| 50 | Yielded merge-base | 1: 3,926; 2: 1,743; 3: 2,116 | 26 / 27 | 2 / 2 | 0 [0–0] / 0 [0–0] |
| 50 | Yielded #8914 | 1: 3,954; 2: 2,131; 3: 2,148 | 27 / 27 | 3 / 3 | 0 [0–0] / 0 [0–0] |
| 50 | Yielded identical control | 1: 4,061; 2: 2,102; 3: 2,032 | 27 / 27 | 3 / 3 | 0 [0–0] / 0 [0–0] |
| 50 | pi-durable 1.0.4 | 1: 1,143; 2: 927; 3: 1,007 | 0 / 24 | 0 / 3 | 0 [0–0] / 0 [0–0] |
| 50 | tardie 0.44.0 | 1: 5,588; 2: 3,678; 3: 3,618 | 27 / 27 | 3 / 3 | 1,871 [1,754.5–2,062] / 1,829 [1,809.5–2,004] |
| 250 | Yielded 4.0.0 | 0: 5,450; 1: 5,962; 2: 5,241 | 26 / 26 | 3 / 3 | 0 [0–0] / 0 [0–0] |
| 250 | Yielded merge-base | 0: 3,236; 1: 5,799; 2: 4,996 | 27 / 27 | 3 / 3 | 0 [0–0] / 0 [0–0] |
| 250 | Yielded #8914 | 0: 6,447; 1: 5,463; 2: 3,190 | 26 / 26 | 3 / 3 | 0 [0–0] / 0 [0–0] |
| 250 | Yielded identical control | 0: 2,064; 1: 1,348; 2: 5,309 | 19 / 19 | 2 / 2 | 0 [0–0] / 0 [0–0] |
| 250 | pi-durable 1.0.4 | 0: 1,673; 1: 2,139; 2: 1,594 | 0 / 27 | 0 / 3 | 0 [0–0] / 0 [0–0] |
| 250 | tardie 0.44.0 | 0: 6,890; 1: 6,431; 2: 5,670 | 25 / 25 | 2 / 2 | 1,045 [863–2,149] / 1,125 [966–1,811.5] |
| 1,000 | Yielded 4.0.0 | 0: 12,103; 1: 10,421; 2: 11,050 | 27 / 27 | 3 / 3 | 0 [0–0] / 0 [0–0] |
| 1,000 | Yielded merge-base | 0: 5,224; 1: 5,610; 2: 5,951 | 27 / 27 | 3 / 3 | 0 [0–0] / 0 [0–0] |
| 1,000 | Yielded #8914 | 0: 4,436; 1: 6,125; 2: 5,377 | 25 / 25 | 2 / 2 | 0 [0–0] / 0 [0–0] |
| 1,000 | Yielded identical control | 0: 5,232; 1: 8,543; 2: 5,107 | 26 / 26 | 3 / 3 | 0 [0–0] / 0 [0–0] |
| 1,000 | pi-durable 1.0.4 | 0: 8,408; 1: 9,374; 2: 4,518 | 0 / 27 | 0 / 3 | 0 [0–0] / 0 [0–0] |
| 1,000 | tardie 0.44.0 | 0: 21,826; 1: 14,821; 2: 21,827 | 26 / 27 | 3 / 3 | 3,011 [2,208–4,054.5] / 8,055 [6,161.5–8,257] |

## Method

Latency is primary: client-observed time through receipt of the complete JSON response, then Cloudflare's Durable Object invocation `wallTimeMs`. CPU is the breakdown. Each matched turn also records `DO wall − DO CPU` and `client wall − DO wall`. These differences preserve their signs and are boundary measurements, not a causal allocation to storage, queueing, or the network. Cloudflare invocation wall time describes the lifetime of the invocation's JavaScript context and can include work after the response; it is not inherently response latency. [Cloudflare timing definition](https://developers.cloudflare.com/workers/observability/metrics-and-analytics/#wall-time-per-execution).

The placement problem was incorporated before the accepted measurement. The two sibling reports found large differences between identical Workers: [effect-eval-cost](https://github.com/yielded-dev/agent/blob/8e6cf8407589e1a91ba967971ede73a285e66022/examples/durable-bench/results/effect-eval-cost/report.md) and [sync-do-append](https://github.com/yielded-dev/agent/blob/c1bffcc33b1caad56fb6d7413b64c081043f5e3c/examples/durable-bench/results/sync-do-append/report.md). I chose the same fixed JavaScript reference RPC before every turn, together with three distinct Objects per role and size. This retains the original separately bundled runtimes and avoids changing their storage, recovery, or alarm implementations to fit two runtimes into one Object.

The reference runs ten million iterations of the same storage-free integer loop on the same Object, returns a checked checksum (`623056721`), and has its own telemetry and client receipt. For Tardie, the Actor is referenced separately first, then the Thread receives the same single-Object reference RPC used by the other targets. CPU, DO wall, and client wall ratios each divide the turn measurement by its own immediately preceding reference measurement. The cold reference precedes the final reset. Reference work and rollout preparation are outside the measured turn. The reference changes the spacing between warm turns; these are turns under that common measurement protocol.

Normalisation is a diagnostic, not a guarantee that placement, JIT behaviour, garbage collection, storage waits, or ingress paths have been corrected. Reference execution itself differs substantially among roles. Raw latency, per-Object results, reference evolution and identical-code controls therefore remain visible. A smaller normalised ratio alone does not establish that one framework is faster.

The client request path was not geographically fixed. Response `CF-Ray` suffixes include `MAD`, `MRS`, `KIX`, `NRT` and `PDX`, and differ between some paired cohorts. They identify the Cloudflare data centre processing the HTTP request, not the Durable Object's physical placement. Consequently, client latency describes this experiment's observed route and is not a minimum achievable with a nearby client. The Object wall measurement and client-minus-Object residual remain separate; normalisation does not erase this limitation. [Cloudflare's `CF-Ray` definition](https://developers.cloudflare.com/fundamentals/reference/http-headers/#cf-ray).

Each Object runs the README workload once: ten turns, eight readonly `lookup` calls and nine scripted model calls per turn. Cold is open plus the first turn; warm is turns 2–10. Historical inputs use the unmodified `[1,1,0]` tool-call seed plan and the original 256-byte / occasional 8-KiB payloads. All models are scripted; there are no model-provider requests.

Each sample is ten consecutive workload turns with reference RPCs between them. Cohort order alternates baseline/head/control and control/head/baseline, with the other targets also reordered. The identical control has byte-identical baseline modules. Paired ratios compare matching sample IDs; warm inference uses the Object's median, not 27 supposedly independent turns. The README summary pools the nine warm turns from each of three Objects. Quartiles use linear interpolation. All ratio tables carry the corresponding identical-control spread. Three Objects provide descriptive replication, not a precise confidence interval. Resolution checks consider the full control/base range and its reciprocal around 1, including a biased control median; IQR width alone is not a noise bound.

A narrow control envelope among three Objects does not override the siblings' evidence of broader deployment variation. This experiment does not establish sub-30% effects merely because one observed control range is narrower. Normalisation and replication improve the evidence available for checking a claim; they did not guarantee that a small PR effect would become resolvable.

Cold means the first workload RPC in a fresh Durable Object incarnation over the seeded database after `ctx.abort()`. Readiness checks and a reference prime happen first; both Tardie's Thread and Actor are reset. The receipt must carry a new runtime ID, and matched telemetry must contain the constructor log on the cold RPC trace. Module/isolate freshness is not guaranteed. Cold includes the runtime's open/recovery work and first eight-tool turn; it excludes seed time, deployment/upload/startup validation, readiness, the reference RPC and the explicit reset. This definition is identical across targets. Worker upload startup is reported separately. [Durable Object lifecycle API](https://developers.cloudflare.com/durable-objects/api/state/).

All 54 measured Thread Objects retain their module/runtime/version identities through their nine warm turns. Tardie's supporting Actor also keeps the same Object ID and version, but its runtime and module identities change during `warm6` in cohort 50/2 and during `warm7` in cohort 250/2. The original README's turns 2–10 remain in the warm results. They therefore include those native Actor reactivations; they are not a guarantee that every supporting Object stays warm. The audit does not attribute the latency gap to these events. [Receipt and incarnation audit](checks/workload-audit.json).

The in-Object `Date.now()` interval is retained as an additional diagnostic. Workers' clocks advance across I/O rather than measuring synchronous CPU, so zero there is not zero execution time. CPU comes only from Cloudflare invocation telemetry. [Workers clocks](https://developers.cloudflare.com/workers/runtime-apis/performance/).

All deployments use Alchemy stacks, SQLite namespaces, `bundle: false`, invocation logs, `headSamplingRate: 1`, a `wnam` location hint for every Thread and Actor, and a 300,000-ms CPU limit. `wnam` does not pin Objects to one physical machine. The original `bench/targets.ts` `prepare()` output is uploaded unchanged as `bench.mjs`; a separate routing/identity/reference wrapper is uploaded alongside it. Every upload is downloaded through the Cloudflare API and checked against its local SHA-256. No measured runtime disables alarms, drops durability checks, changes history, or changes tool execution semantics.

Native alarms remain enabled. The collector retains all observed invocation outcomes and alarms on the owned Objects through 35 seconds after each ten-turn sample, plus two seconds of timestamp tolerance. Temporal overlap means overlapping reported event-time / wall-time intervals; it does not prove that an alarm delayed the turn. The tail is a bounded observation window, not proof that all future alarm work has finished. Tardie's Thread RPC CPU excludes much of the CPU in its separate Actor and alarm invocations, so main-RPC CPU must not be used by itself to rank whole-framework compute. Additional all-owned-Object CPU totals are reported separately, without assigning alarm CPU to arbitrary individual turns.

An output gate waiting for durable writes, alarm queueing, and runtime scheduling remain hypotheses for the non-CPU gap. The fixed reference performs no application storage writes yet also has a non-CPU wall component. This experiment does not isolate the causes. Storage size and net size growth are recorded. They are not bytes written. Comparable per-turn write-byte and committed-transaction counts would require additional instrumentation across the Thread, Actor and alarms; those counts were not inferred from page growth or SQL row counts.

## Builds and correctness

Framework source is repository commit `8c05714de84d68961b14e5ab7a3b7d809599563f`. The original workload files and every bundle have recorded hashes in [builds](builds/). Effect is restored to the repository's patched pinned 4.0.0 installation after preparing the candidate bundles; the root lockfile is unchanged.

This checkout is newer than the supplied local reference at `32f8a078`: it includes `d449cd5e` (#816), which reuses durable decoders and SQL work. The benchmark fixture and third-party versions are unchanged, but Yielded's runtime is not byte-identical to that older reference. The local numbers are orientation only; a changed ordering cannot be attributed solely to the hosting environment. All Effect variants here use the same current framework source.

The PR head is exactly `01c6222ccf74390848595633ef23410cbfa6983b`. Its actual `git merge-base` with fetched main is `757821fe99b7179f907d6d1a34a4e86de4173112`, not current main. Both source revisions report 4.0.1 and receive the same equivalent `ai/LanguageModel` patch. The separate clone, package preparation, matched build pipeline, applied patches, per-file hashes and reproduction script are retained in [effect-builds](effect-builds/README.md). Both variants use TypeScript 6.0.3's compiler API and the same Babel pass; this is not the upstream TypeScript 7 CLI release build. The pinned 4.0.0 comparison consequently includes other version/build differences and is not the causal PR comparison.

Affected consumer packages type-check and the original benchmark bundles build with pinned, base and head. No API incompatibility was observed in the APIs this benchmark uses. [Consumer checks and installation restoration](checks/install-swap.json). Pi is pi-durable 1.0.4 and has no Effect runtime; the Effect field in its build manifest describes the build CLI only. Tardie is 0.44.0 with its own bundled Effect 4.0.0-rc.115; #8914 does not apply to it.

Evaluation and primitive-allocation counts are skipped under the task's comparability exception. The copied KOM-433 harness wraps constructors and evaluation sites changed by the PR: new specialised primitives, queue operations and inlined continuation dispatch would be missed or mislabelled by the original hooks. Applying the old hooks would create an artificial reduction. The exact incompatibilities and the preserved original harness are documented in [the count assessment](effect-builds/README.md#kom-433-count-harness-assessment) and [source/harness-corrected](source/harness-corrected/).

Seeded transcript fingerprints must match the supplied references before measurement: 50 `b017b487524e44a4`, 250 `dcea9f30b0917245`, 1,000 `ac520308146f2a8f`. Post-run fingerprints are also compared across all six roles. Failed preparation attempts remain in the raw artifacts. The consumed 50/0 rollout pilot is quarantined; accepted size-50 samples use distinct Objects 1, 2 and 3. Completed benchmark turns are never rerun to repair telemetry. Only an exact ingress HTTP 412 generation rejection, emitted before Object access, may be retried; those rejected requests are recorded outside accepted turn latency.

Seeding retains the original bench's restart interval of 50 historical turns. Tardie's native alarm work requires a completed per-turn RPC and a quiescence check before admitting the next seed input; an ingress-only helper serialises up to ten such original RPCs. This helper is never used for measured turns. Preparation encountered storage-operation timeouts, native alarm delays and lost acknowledgements. Failed batches are retained. A continuation after a lost acknowledgement requires read-only persisted input and successful terminal records proving the exact completed prefix, plus quiescence and the final full transcript fingerprint; no ambiguous input is replayed.

Tardie encountered the 128-MB isolate memory limit while seeding the second 1,000-turn Object. The client received a failed seed batch after the last acknowledged prefix of 990 turns. Cloudflare recorded `exceededMemory` on a `seed` RPC and an overlapping `alarm` invocation for that same Object and timestamp; these are two invocation records for one observed failure episode, not two independent failures. Read-only persisted evidence subsequently proved 998 completed historical inputs. Only the two remaining new inputs were admitted, and the final 1,000-turn transcript matched `ac520308146f2a8f`. This recovery does not erase the capacity failure or establish its cause. [Incident, telemetry and recovery proof](run/resource-incidents/tardie-1000-1-memory.json).

The final [resource-outcome export](run/resource-outcomes.json) contains only those two memory-limit records. No `exceededCpu` outcome or additional `exceededMemory` outcome was observed. All 540 accepted benchmark turns completed successfully, including the recovered Tardie Object's ten measured turns. Local workerd RSS was not used as a measurement of deployed isolate memory.

## Evidence coverage and reproduction

Raw receipts, reference receipts, seed audits, invocation records, deployment/module verification, and collection status live in [run](run/). [summary.json](run/summary.json) retains every quantile, numerator/denominator and coverage count; [tables.md](run/tables.md) expands the raw and normalised metrics. Workload completion and telemetry completeness are separate. A missing log is not a failed client turn or zero CPU. Client medians require all nine warm receipts; an Object's DO/CPU median requires at least seven of nine observed warm rows, a cutoff fixed before collecting the accepted cohorts. Missing cold constructor evidence excludes the corresponding telemetry cold result. Every table states its actual count.

Client coverage is **540/540** turns. Main DO wall/CPU coverage is **518/540**: 176/180 at 50 turns, 166/180 at 250, and 176/180 at 1,000. Own-reference DO normalisation is available for 503 turns. Additional ingress gaps keep some collection statuses incomplete even when the main DO record exists. No completed turn was repeated to fill a telemetry gap.

At 250 turns, only two control Objects qualify for raw warm DO/CPU comparisons. Some normalised warm DO/CPU comparisons have two eligible target pairs but no matching eligible control pairs; they cannot establish a gain. The incomplete control sample's window-observed CPU total is likewise unsuitable for an efficiency claim. Supplementary tables separate alarm counts and alarm CPU from the all-owned-Object totals shown above.

All timing evidence comes from deployed Cloudflare requests. The retained KOM-433 source report contains its historical local results, but they are not new measurements or timing evidence for this task. Local work here is build/typecheck, deterministic validation and analysis of hosted receipts.

The completed workload covers three distinct Thread Objects per role at each of 50, 250 and 1,000 historical turns: 54 measured Thread Objects, nine supporting Tardie Actor Objects and 540 measured turns. The optional 3,500-turn size was not provisioned or measured; completing the 1,000-turn seed set already required failure recovery. These results make no claim about 3,500-turn latency or capacity.

From this directory's `harness/`, use `vp run hosted --help` for deployment/seed/measure/collect/cleanup actions and `vp run summarize --output-dir <absolute-run-directory>` to regenerate statistics without executing workloads. Use `vp run check` here for the harness's explicit file checks. `vp install` and the original example's `vendor` task prepare dependencies; the existing build CLI calls the original `prepare()` function and copies its output. Alchemy state and authentication stay in a private mode-700 directory outside the repository. No state or credentials belong in committed evidence.

## Cleanup and validation

**Cleanup is verified.** Alchemy destroyed all six task Workers. Independent Cloudflare API checks returned HTTP 404 for each Worker and found none of the 142 known namespace IDs: 70 from the final deployments plus 72 retired during preparation. The private Alchemy state/auth directory was removed and its absence checked. [Cleanup receipt](cleanup.json) includes every covered namespace ID and retirement receipt.

All three Effect variants built and passed the affected consumer checks. The final harness `vp run check` passed formatting, lint and type checks for all 11 files. The task's allowed affected-package validation gate was used; no full `vp run ready` pass is claimed. [Harness check](checks/harness-final.log), [consumer checks and restoration](checks/install-swap.json).

The saved evidence was checked for Cloudflare credentials, the private benchmark token and accidentally included Alchemy state/auth files. [Artifact audit](checks/artifact-scan.json), [audit before private-state removal](checks/artifact-scan-before-cleanup.json).
