Phase: main. Object median [Q1, Q3]; min…max across Objects. All durations ms. — means missing or inapplicable.

Headline driver latency: three independent Objects per target

| Cell | Yielded production | pi native | Tardie native | Yielded/pi |
| --- | --- | --- | --- | --- |
| h50/d0/cold | 1292.0 [1265.0, 1715.0]; 1238.0…2138.0 (n=3) | 967.0 [893.5, 1077.0]; 820.0…1187.0 (n=3) | 2616.0 [2490.0, 2631.0]; 2364.0…2646.0 (n=3) | 1.336× |
| h50/d0/warm | 759.0 [733.0, 1083.5]; 707.0…1408.0 (n=3) | 748.0 [670.5, 752.5]; 593.0…757.0 (n=3) | 1976.0 [1944.0, 2011.5]; 1912.0…2047.0 (n=3) | 1.015× |
| h50/d400/cold | 5553.0 [5431.5, 5594.5]; 5310.0…5636.0 (n=3) | 5127.0 [4967.0, 5259.5]; 4807.0…5392.0 (n=3) | 6371.0 [6354.0, 6853.5]; 6337.0…7336.0 (n=3) | 1.083× |
| h50/d400/warm | 4972.0 [4950.5, 4996.5]; 4929.0…5021.0 (n=3) | 4796.0 [4722.5, 4895.0]; 4649.0…4994.0 (n=3) | 6006.0 [5734.5, 6335.0]; 5463.0…6664.0 (n=3) | 1.037× |
| h250/d0/cold | 1535.0 [1530.0, 1922.0]; 1525.0…2309.0 (n=3) | 898.0 [893.0, 906.5]; 888.0…915.0 (n=3) | 2777.0 [2596.0, 3452.0]; 2415.0…4127.0 (n=3) | 1.709× |
| h250/d0/warm | 1069.0 [999.0, 1192.5]; 929.0…1316.0 (n=3) | 686.0 [669.5, 778.5]; 653.0…871.0 (n=3) | 2238.0 [1861.0, 2689.0]; 1484.0…3140.0 (n=3) | 1.558× |
| h250/d400/cold | 5768.0 [5624.5, 5922.5]; 5481.0…6077.0 (n=3) | 5167.0 [5093.0, 5255.5]; 5019.0…5344.0 (n=3) | 6911.0 [6448.5, 7019.5]; 5986.0…7128.0 (n=3) | 1.116× |
| h250/d400/warm | 5093.0 [5024.5, 5144.5]; 4956.0…5196.0 (n=3) | 4860.0 [4857.5, 4909.0]; 4855.0…4958.0 (n=3) | 5808.0 [5695.5, 5954.5]; 5583.0…6101.0 (n=3) | 1.048× |

Driver and provider timing

| Cohort | Objects | Driver | Laptop including diagnostics | Admission | Submit → first provider bounds* | Receipt → first provider bounds* | Last provider → client bounds* | Gap median* |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| tardie/inline/h250/d0/cold | 3/3 | 2777.0 [2596.0, 3452.0]; 2415.0…4127.0 (n=3) | 2880.4 [2704.9, 3567.9]; 2529.4…4255.4 (n=3) | — | 1512.5…1519.0 (n=2) | — | 121.5…128.0 (n=2) | 186.5 [167.5, 212.0]; 148.5…237.5 (n=3) |
| pi/inline/h250/d0/cold | 3/3 | 898.0 [893.0, 906.5]; 888.0…915.0 (n=3) | 1019.5 [1013.0, 1088.1]; 1006.5…1156.7 (n=3) | — | 192.0…198.0 (n=1) | — | 52.0…58.0 (n=1) | 66.5 [65.5, 69.3]; 64.5…72.0 (n=3) |
| yielded/production/h250/d0/cold | 3/3 | 1535.0 [1530.0, 1922.0]; 1525.0…2309.0 (n=3) | 1619.9 [1597.2, 2008.3]; 1574.4…2396.8 (n=3) | 715.0 [670.5, 921.5]; 626.0…1128.0 (n=3) | 766.0…770.0 (n=3) | 51.0…55.0 (n=3) | 122.0…123.0 (n=3) | 81.0 [79.0, 92.0]; 77.0…103.0 (n=3) |
| yielded/production/h250/d0/settling | 3/3 | 1114.0 [1028.0, 1216.0]; 942.0…1318.0 (n=3) | 1176.8 [1083.6, 1277.4]; 990.4…1378.0 (n=3) | 305.0 [302.0, 359.0]; 299.0…413.0 (n=3) | 330.0…336.0 (n=3) | 26.0…31.0 (n=3) | 107.0…110.0 (n=3) | 72.5 [68.8, 80.5]; 65.0…88.5 (n=3) |
| pi/inline/h250/d0/settling | 3/3 | 691.0 [678.5, 759.0]; 666.0…827.0 (n=3) | 745.6 [744.3, 810.6]; 743.0…875.6 (n=3) | — | 60.0…67.0 (n=1) | — | 50.0…57.0 (n=1) | 70.0 [68.5, 70.3]; 67.0…70.5 (n=3) |
| tardie/inline/h250/d0/settling | 3/3 | 2257.0 [2038.5, 2973.0]; 1820.0…3689.0 (n=3) | 2371.9 [2152.2, 3121.9]; 1932.5…3871.9 (n=3) | — | 689.0…695.5 (n=2) | — | 124.0…130.5 (n=2) | 199.5 [172.3, 238.3]; 145.0…277.0 (n=3) |
| yielded/production/h250/d0/warm | 3/3 | 1069.0 [999.0, 1192.5]; 929.0…1316.0 (n=3) | 1140.6 [1056.7, 1316.6]; 972.7…1492.6 (n=3) | 266.0 [257.0, 306.0]; 248.0…346.0 (n=3) | 288.0…293.0 (n=3) | 25.0…31.0 (n=3) | 79.0…83.0 (n=3) | 72.0 [71.0, 78.3]; 70.0…84.5 (n=3) |
| pi/inline/h250/d0/warm | 3/3 | 686.0 [669.5, 778.5]; 653.0…871.0 (n=3) | 738.3 [724.6, 845.1]; 710.8…952.0 (n=3) | — | 58.0…63.0 (n=1) | — | 51.0…56.0 (n=1) | 66.5 [65.8, 71.8]; 65.0…77.0 (n=3) |
| tardie/inline/h250/d0/warm | 3/3 | 2238.0 [1861.0, 2689.0]; 1484.0…3140.0 (n=3) | 2344.0 [1970.8, 2779.7]; 1597.6…3215.4 (n=3) | — | 601.5…609.0 (n=2) | — | 114.0…119.0 (n=2) | 195.0 [166.8, 223.0]; 138.5…251.0 (n=3) |
| yielded/production/h50/d0/cold | 3/3 | 1292.0 [1265.0, 1715.0]; 1238.0…2138.0 (n=3) | 1347.9 [1331.5, 1806.6]; 1315.0…2265.3 (n=3) | 253.0 [215.0, 342.5]; 177.0…432.0 (n=3) | 725.0…729.5 (n=2) | 510.0…514.5 (n=2) | 102.0…106.5 (n=2) | 80.5 [77.8, 113.5]; 75.0…146.5 (n=3) |
| pi/inline/h50/d0/cold | 3/3 | 967.0 [893.5, 1077.0]; 820.0…1187.0 (n=3) | 1053.0 [972.4, 1139.6]; 891.8…1226.1 (n=3) | — | 387.0…392.0 (n=1) | — | 57.0…62.0 (n=1) | 75.5 [69.3, 80.3]; 63.0…85.0 (n=3) |
| tardie/inline/h50/d0/cold | 3/3 | 2616.0 [2490.0, 2631.0]; 2364.0…2646.0 (n=3) | 2759.8 [2594.7, 2767.4]; 2429.6…2775.0 (n=3) | — | 779.5…788.5 (n=2) | — | 166.0…175.0 (n=2) | 188.5 [188.5, 193.0]; 188.5…197.5 (n=3) |
| tardie/inline/h50/d0/settling | 3/3 | 1935.0 [1910.5, 1949.0]; 1886.0…1963.0 (n=3) | 2004.6 [1976.5, 2058.0]; 1948.4…2111.4 (n=3) | — | 326.5…332.5 (n=2) | — | 101.5…107.5 (n=2) | 181.5 [180.8, 182.8]; 180.0…184.0 (n=3) |
| yielded/production/h50/d0/settling | 3/3 | 800.0 [772.5, 1212.0]; 745.0…1624.0 (n=3) | 866.8 [860.0, 1276.3]; 853.3…1685.7 (n=3) | 152.0 [104.0, 259.5]; 56.0…367.0 (n=3) | 283.0…288.0 (n=2) | 71.5…76.5 (n=2) | 71.5…76.5 (n=2) | 66.0 [63.0, 105.5]; 60.0…145.0 (n=3) |
| pi/inline/h50/d0/settling | 3/3 | 645.0 [612.0, 702.0]; 579.0…759.0 (n=3) | 711.1 [675.0, 760.6]; 638.9…810.1 (n=3) | — | 73.0…79.0 (n=1) | — | 44.0…50.0 (n=1) | 63.5 [60.5, 72.3]; 57.5…81.0 (n=3) |
| pi/inline/h50/d0/warm | 3/3 | 748.0 [670.5, 752.5]; 593.0…757.0 (n=3) | 804.9 [730.7, 809.3]; 656.4…813.7 (n=3) | — | 57.0…64.0 (n=1) | — | 49.0…56.0 (n=1) | 65.5 [61.3, 72.8]; 57.0…80.0 (n=3) |
| tardie/inline/h50/d0/warm | 3/3 | 1976.0 [1944.0, 2011.5]; 1912.0…2047.0 (n=3) | 2113.3 [2104.6, 2170.5]; 2095.8…2227.8 (n=3) | — | 255.0…263.0 (n=2) | — | 98.0…105.0 (n=2) | 183.5 [182.5, 184.3]; 181.5…185.0 (n=3) |
| yielded/production/h50/d0/warm | 3/3 | 759.0 [733.0, 1083.5]; 707.0…1408.0 (n=3) | 828.9 [798.9, 1153.6]; 768.9…1478.2 (n=3) | 155.0 [108.5, 277.0]; 62.0…399.0 (n=3) | 319.0…324.0 (n=2) | 86.0…90.5 (n=2) | 73.5…79.0 (n=2) | 63.0 [59.3, 81.0]; 55.5…99.0 (n=3) |
| pi/inline/h250/d400/cold | 3/3 | 5167.0 [5093.0, 5255.5]; 5019.0…5344.0 (n=3) | 5324.5 [5237.5, 5463.1]; 5150.5…5601.8 (n=3) | — | 294.5…299.5 (n=2) | — | 44.0…49.0 (n=2) | 77.5 [75.8, 78.5]; 74.0…79.5 (n=3) |
| yielded/production/h250/d400/cold | 3/3 | 5768.0 [5624.5, 5922.5]; 5481.0…6077.0 (n=3) | 5826.0 [5719.4, 5982.4]; 5612.9…6138.8 (n=3) | 223.0 [223.0, 487.0]; 223.0…751.0 (n=3) | 1083.0…1086.5 (n=2) | 860.0…863.5 (n=2) | 104.0…107.5 (n=2) | 65.5 [59.5, 69.8]; 53.5…74.0 (n=3) |
| tardie/inline/h250/d400/cold | 3/3 | 6911.0 [6448.5, 7019.5]; 5986.0…7128.0 (n=3) | 7011.2 [6543.2, 7096.8]; 6075.2…7182.4 (n=3) | — | 1130.5…1136.5 (n=2) | — | 73.5…79.5 (n=2) | 185.0 [148.3, 186.8]; 111.5…188.5 (n=3) |
| pi/inline/h250/d400/settling | 3/3 | 4855.0 [4835.0, 4878.0]; 4815.0…4901.0 (n=3) | 4901.8 [4881.0, 4945.6]; 4860.1…4989.4 (n=3) | — | 62.5…68.0 (n=2) | — | 43.0…48.5 (n=2) | 68.0 [65.8, 68.5]; 63.5…69.0 (n=3) |
| yielded/production/h250/d400/settling | 3/3 | 5310.0 [5215.0, 5326.5]; 5120.0…5343.0 (n=3) | 5388.3 [5312.5, 5390.0]; 5236.7…5391.7 (n=3) | 68.0 [60.0, 95.5]; 52.0…123.0 (n=3) | 555.0…559.0 (n=2) | 495.0…499.0 (n=2) | 77.5…81.5 (n=2) | 61.0 [55.0, 65.5]; 49.0…70.0 (n=3) |
| tardie/inline/h250/d400/settling | 3/3 | 5998.0 [5751.5, 6254.5]; 5505.0…6511.0 (n=3) | 6054.0 [5817.0, 6330.8]; 5580.0…6607.5 (n=3) | — | 340.5…347.5 (n=2) | — | 60.5…67.5 (n=2) | 176.5 [141.8, 181.5]; 107.0…186.5 (n=3) |
| tardie/inline/h250/d400/warm | 3/3 | 5808.0 [5695.5, 5954.5]; 5583.0…6101.0 (n=3) | 5863.0 [5753.2, 6030.5]; 5643.3…6198.0 (n=3) | — | 365.5…370.5 (n=2) | — | 66.0…71.5 (n=2) | 173.0 [138.8, 175.8]; 104.5…178.5 (n=3) |
| yielded/production/h250/d400/warm | 3/3 | 5093.0 [5024.5, 5144.5]; 4956.0…5196.0 (n=3) | 5147.6 [5111.9, 5203.2]; 5076.2…5258.8 (n=3) | 75.0 [70.5, 95.0]; 66.0…115.0 (n=3) | 370.0…376.0 (n=2) | 310.0…315.5 (n=2) | 71.5…77.0 (n=2) | 59.5 [54.0, 60.5]; 48.5…61.5 (n=3) |
| pi/inline/h250/d400/warm | 3/3 | 4860.0 [4857.5, 4909.0]; 4855.0…4958.0 (n=3) | 4918.0 [4912.7, 4964.8]; 4907.3…5011.6 (n=3) | — | 112.0…118.5 (n=2) | — | 40.0…47.0 (n=2) | 72.0 [71.8, 76.3]; 71.5…80.5 (n=3) |
| tardie/inline/h50/d400/cold | 3/3 | 6371.0 [6354.0, 6853.5]; 6337.0…7336.0 (n=3) | 6530.0 [6501.2, 6999.6]; 6472.3…7469.3 (n=3) | — | — | — | — | 173.0 [166.0, 188.3]; 159.0…203.5 (n=3) |
| pi/inline/h50/d400/cold | 3/3 | 5127.0 [4967.0, 5259.5]; 4807.0…5392.0 (n=3) | 5241.1 [5078.7, 5425.5]; 4916.3…5609.9 (n=3) | — | 192.0…197.0 (n=1) | — | 27.0…32.0 (n=1) | 72.0 [61.5, 80.3]; 51.0…88.5 (n=3) |
| yielded/production/h50/d400/cold | 3/3 | 5553.0 [5431.5, 5594.5]; 5310.0…5636.0 (n=3) | 5612.1 [5514.3, 5730.0]; 5416.6…5848.0 (n=3) | 231.0 [218.0, 334.5]; 205.0…438.0 (n=3) | 763.5…768.0 (n=2) | 545.5…550.0 (n=2) | 87.0…91.5 (n=2) | 72.5 [65.5, 74.3]; 58.5…76.0 (n=3) |
| yielded/production/h50/d400/settling | 3/3 | 5023.0 [4921.5, 5133.5]; 4820.0…5244.0 (n=3) | 5166.2 [5021.3, 5232.3]; 4876.3…5298.4 (n=3) | 86.0 [72.5, 160.5]; 59.0…235.0 (n=3) | 273.0…278.0 (n=2) | 200.5…205.5 (n=2) | 59.5…64.5 (n=2) | 66.0 [56.8, 72.8]; 47.5…79.5 (n=3) |
| pi/inline/h50/d400/settling | 3/3 | 4805.0 [4706.5, 4882.5]; 4608.0…4960.0 (n=3) | 4902.2 [4779.9, 4980.5]; 4657.6…5058.8 (n=3) | — | 39.0…45.0 (n=1) | — | 26.0…32.0 (n=1) | 63.5 [57.5, 71.8]; 51.5…80.0 (n=3) |
| tardie/inline/h50/d400/settling | 3/3 | 5959.0 [5785.0, 6119.5]; 5611.0…6280.0 (n=3) | 6112.5 [5909.2, 6239.7]; 5705.8…6366.9 (n=3) | — | — | — | — | 166.0 [158.0, 177.5]; 150.0…189.0 (n=3) |
| pi/inline/h50/d400/warm | 3/3 | 4796.0 [4722.5, 4895.0]; 4649.0…4994.0 (n=3) | 4908.6 [4853.7, 4995.7]; 4798.7…5082.8 (n=3) | — | 41.0…46.0 (n=1) | — | 28.0…33.0 (n=1) | 65.0 [60.0, 72.5]; 55.0…80.0 (n=3) |
| yielded/production/h50/d400/warm | 3/3 | 4972.0 [4950.5, 4996.5]; 4929.0…5021.0 (n=3) | 5059.7 [5040.8, 5069.7]; 5021.9…5079.7 (n=3) | 70.0 [67.0, 134.0]; 64.0…198.0 (n=3) | 314.5…319.5 (n=2) | 246.5…251.5 (n=2) | 66.0…71.5 (n=2) | 61.5 [55.3, 62.5]; 49.0…63.5 (n=3) |
| tardie/inline/h50/d400/warm | 3/3 | 6006.0 [5734.5, 6335.0]; 5463.0…6664.0 (n=3) | 6162.4 [5895.7, 6455.3]; 5629.0…6748.1 (n=3) | — | — | — | — | 168.5 [150.5, 182.5]; 132.5…196.5 (n=3) |

I/O-clock span and waiter diagnostics (not wall decomposition)

| Cohort | Ledger claim† | Run claim† | Recovery† | Waiter reads | Read interval† |
| --- | --- | --- | --- | --- | --- |
| tardie/inline/h250/d0/cold | — | — | — | 0.0 [0.0, 0.0]; 0.0…0.0 (n=3) | — |
| pi/inline/h250/d0/cold | — | — | — | 0.0 [0.0, 0.0]; 0.0…0.0 (n=3) | — |
| yielded/production/h250/d0/cold | — | 0.0 [0.0, 0.0]; 0.0…0.0 (n=3) | 0.0 [0.0, 0.0]; 0.0…0.0 (n=3) | 0.0 [0.0, 0.0]; 0.0…0.0 (n=3) | — |
| yielded/production/h250/d0/settling | — | 0.0 [0.0, 0.0]; 0.0…0.0 (n=3) | 0.0 [0.0, 0.0]; 0.0…0.0 (n=3) | 0.0 [0.0, 0.0]; 0.0…0.0 (n=3) | — |
| pi/inline/h250/d0/settling | — | — | — | 0.0 [0.0, 0.0]; 0.0…0.0 (n=3) | — |
| tardie/inline/h250/d0/settling | — | — | — | 0.0 [0.0, 0.0]; 0.0…0.0 (n=3) | — |
| yielded/production/h250/d0/warm | — | 0.0 [0.0, 0.0]; 0.0…0.0 (n=3) | 0.0 [0.0, 0.0]; 0.0…0.0 (n=3) | 0.0 [0.0, 0.0]; 0.0…0.0 (n=3) | — |
| pi/inline/h250/d0/warm | — | — | — | 0.0 [0.0, 0.0]; 0.0…0.0 (n=3) | — |
| tardie/inline/h250/d0/warm | — | — | — | 0.0 [0.0, 0.0]; 0.0…0.0 (n=3) | — |
| yielded/production/h50/d0/cold | — | 0.0 [0.0, 0.0]; 0.0…0.0 (n=3) | 0.0 [0.0, 0.0]; 0.0…0.0 (n=3) | 0.0 [0.0, 0.0]; 0.0…0.0 (n=3) | — |
| pi/inline/h50/d0/cold | — | — | — | 0.0 [0.0, 0.0]; 0.0…0.0 (n=3) | — |
| tardie/inline/h50/d0/cold | — | — | — | 0.0 [0.0, 0.0]; 0.0…0.0 (n=3) | — |
| tardie/inline/h50/d0/settling | — | — | — | 0.0 [0.0, 0.0]; 0.0…0.0 (n=3) | — |
| yielded/production/h50/d0/settling | — | 0.0 [0.0, 0.0]; 0.0…0.0 (n=3) | 0.0 [0.0, 0.0]; 0.0…0.0 (n=3) | 0.0 [0.0, 0.0]; 0.0…0.0 (n=3) | — |
| pi/inline/h50/d0/settling | — | — | — | 0.0 [0.0, 0.0]; 0.0…0.0 (n=3) | — |
| pi/inline/h50/d0/warm | — | — | — | 0.0 [0.0, 0.0]; 0.0…0.0 (n=3) | — |
| tardie/inline/h50/d0/warm | — | — | — | 0.0 [0.0, 0.0]; 0.0…0.0 (n=3) | — |
| yielded/production/h50/d0/warm | — | 0.0 [0.0, 0.0]; 0.0…0.0 (n=3) | 0.0 [0.0, 0.0]; 0.0…0.0 (n=3) | 0.0 [0.0, 0.0]; 0.0…0.0 (n=3) | — |
| pi/inline/h250/d400/cold | — | — | — | 0.0 [0.0, 0.0]; 0.0…0.0 (n=3) | — |
| yielded/production/h250/d400/cold | — | 0.0 [0.0, 0.0]; 0.0…0.0 (n=3) | 0.0 [0.0, 0.0]; 0.0…0.0 (n=3) | 0.0 [0.0, 0.0]; 0.0…0.0 (n=3) | — |
| tardie/inline/h250/d400/cold | — | — | — | 0.0 [0.0, 0.0]; 0.0…0.0 (n=3) | — |
| pi/inline/h250/d400/settling | — | — | — | 0.0 [0.0, 0.0]; 0.0…0.0 (n=3) | — |
| yielded/production/h250/d400/settling | — | 0.0 [0.0, 0.0]; 0.0…0.0 (n=3) | 0.0 [0.0, 0.0]; 0.0…0.0 (n=3) | 0.0 [0.0, 0.0]; 0.0…0.0 (n=3) | — |
| tardie/inline/h250/d400/settling | — | — | — | 0.0 [0.0, 0.0]; 0.0…0.0 (n=3) | — |
| tardie/inline/h250/d400/warm | — | — | — | 0.0 [0.0, 0.0]; 0.0…0.0 (n=3) | — |
| yielded/production/h250/d400/warm | — | 0.0 [0.0, 0.0]; 0.0…0.0 (n=3) | 0.0 [0.0, 0.0]; 0.0…0.0 (n=3) | 0.0 [0.0, 0.0]; 0.0…0.0 (n=3) | — |
| pi/inline/h250/d400/warm | — | — | — | 0.0 [0.0, 0.0]; 0.0…0.0 (n=3) | — |
| tardie/inline/h50/d400/cold | — | — | — | 0.0 [0.0, 0.0]; 0.0…0.0 (n=3) | — |
| pi/inline/h50/d400/cold | — | — | — | 0.0 [0.0, 0.0]; 0.0…0.0 (n=3) | — |
| yielded/production/h50/d400/cold | — | 0.0 [0.0, 0.0]; 0.0…0.0 (n=3) | 0.0 [0.0, 0.0]; 0.0…0.0 (n=3) | 0.0 [0.0, 0.0]; 0.0…0.0 (n=3) | — |
| yielded/production/h50/d400/settling | — | 0.0 [0.0, 0.0]; 0.0…0.0 (n=3) | 0.0 [0.0, 0.0]; 0.0…0.0 (n=3) | 0.0 [0.0, 0.0]; 0.0…0.0 (n=3) | — |
| pi/inline/h50/d400/settling | — | — | — | 0.0 [0.0, 0.0]; 0.0…0.0 (n=3) | — |
| tardie/inline/h50/d400/settling | — | — | — | 0.0 [0.0, 0.0]; 0.0…0.0 (n=3) | — |
| pi/inline/h50/d400/warm | — | — | — | 0.0 [0.0, 0.0]; 0.0…0.0 (n=3) | — |
| yielded/production/h50/d400/warm | — | 0.0 [0.0, 0.0]; 0.0…0.0 (n=3) | 0.0 [0.0, 0.0]; 0.0…0.0 (n=3) | 0.0 [0.0, 0.0]; 0.0…0.0 (n=3) | — |
| tardie/inline/h50/d400/warm | — | — | — | 0.0 [0.0, 0.0]; 0.0…0.0 (n=3) | — |

Invocation CPU (Cloudflare attribution, not semantic phase cost)

| Cohort | Submit | Await | Alarm sum‡ | Alarm pass median‡ | Alarm pass max‡ | Native Thread | Tardie Actor native observed | Tardie Actor alarms (unscoped) | Tardie Actor total (unknown) | Observed alarms | Model contexts inline/alarm/missing | Mixed-context turns |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| tardie/inline/h250/d0/cold | — | — | 311.0 [217.0, 322.5]; 123.0…334.0 (n=3) | 311.0 [217.0, 322.5]; 123.0…334.0 (n=3) | 311.0 [217.0, 322.5]; 123.0…334.0 (n=3) | 543.0 [460.0, 816.0]; 377.0…1089.0 (n=3) | 67.0 [51.5, 107.0]; 36.0…147.0 (n=3) | — | — | 1.0 [1.0, 1.0]; 1.0…1.0 (n=3) | 0/0/27 | 0 |
| pi/inline/h250/d0/cold | — | — | — | — | — | 330.0 [329.0, 338.5]; 328.0…347.0 (n=3) | — | — | — | 0.0 [0.0, 0.0]; 0.0…0.0 (n=3) | 0/0/27 | 0 |
| yielded/production/h250/d0/cold | 60.0 [59.5, 104.0]; 59.0…148.0 (n=3) | 324.0 [318.5, 461.5]; 313.0…599.0 (n=3) | 474.0 [426.0, 649.0]; 378.0…824.0 (n=3) | 474.0 [426.0, 649.0]; 378.0…824.0 (n=3) | 474.0 [426.0, 649.0]; 378.0…824.0 (n=3) | — | — | — | — | 1.0 [1.0, 1.0]; 1.0…1.0 (n=3) | 0/27/0 | 0 |
| yielded/production/h250/d0/settling | 12.0 [10.5, 16.5]; 9.0…21.0 (n=3) | 248.0 [229.5, 308.0]; 211.0…368.0 (n=3) | 217.0 [215.0, 260.0]; 213.0…303.0 (n=3) | 217.0 [215.0, 260.0]; 213.0…303.0 (n=3) | 217.0 [215.0, 260.0]; 213.0…303.0 (n=3) | — | — | — | — | 1.0 [1.0, 1.0]; 1.0…1.0 (n=3) | 0/27/0 | 0 |
| pi/inline/h250/d0/settling | — | — | — | — | — | 249.0 [241.5, 253.5]; 234.0…258.0 (n=3) | — | — | — | 0.0 [0.0, 0.0]; 0.0…0.0 (n=3) | 0/0/27 | 0 |
| tardie/inline/h250/d0/settling | — | — | 754.0 [722.5, 1191.5]; 691.0…1629.0 (n=3) | 754.0 [722.5, 1191.5]; 691.0…1629.0 (n=3) | 754.0 [722.5, 1191.5]; 691.0…1629.0 (n=3) | 19.0 [18.0, 23.5]; 17.0…28.0 (n=3) | 1.0 [0.5, 1.0]; 0.0…1.0 (n=3) | — | — | 1.0 [1.0, 1.0]; 1.0…1.0 (n=3) | 0/0/27 | 0 |
| yielded/production/h250/d0/warm | 9.0 [8.5, 12.5]; 8.0…16.0 (n=3) | 221.0 [220.0, 299.5]; 219.0…378.0 (n=3) | 166.0 [160.0, 196.0]; 154.0…226.0 (n=3) | 166.0 [160.0, 196.0]; 154.0…226.0 (n=3) | 166.0 [160.0, 196.0]; 154.0…226.0 (n=3) | — | — | — | — | 1.0 [1.0, 1.0]; 1.0…1.0 (n=3) | 0/81/0 | 0 |
| pi/inline/h250/d0/warm | — | — | — | — | — | 262.0 [257.5, 282.0]; 253.0…302.0 (n=3) | — | — | — | 0.0 [0.0, 0.0]; 0.0…0.0 (n=3) | 0/0/81 | 0 |
| tardie/inline/h250/d0/warm | — | — | 444.0 [257.0, 883.0]; 70.0…1322.0 (n=3) | 35.0 [32.8, 678.5]; 30.5…1322.0 (n=3) | 355.0 [199.0, 838.5]; 43.0…1322.0 (n=3) | 18.0 [16.0, 25.5]; 14.0…33.0 (n=3) | 1.0 [0.5, 1.0]; 0.0…1.0 (n=3) | — | — | 2.0 [1.5, 3.0]; 1.0…4.0 (n=3) | 0/0/81 | 0 |
| yielded/production/h50/d0/cold | 55.0 [53.5, 75.5]; 52.0…96.0 (n=3) | 331.0 [316.5, 529.5]; 302.0…728.0 (n=3) | 260.0 [242.5, 406.5]; 225.0…553.0 (n=3) | 260.0 [242.5, 406.5]; 225.0…553.0 (n=3) | 260.0 [242.5, 406.5]; 225.0…553.0 (n=3) | — | — | — | — | 1.0 [1.0, 1.0]; 1.0…1.0 (n=3) | 0/27/0 | 0 |
| pi/inline/h50/d0/cold | — | — | — | — | — | 437.0 [350.5, 452.5]; 264.0…468.0 (n=3) | — | — | — | 0.0 [0.0, 0.0]; 0.0…0.0 (n=3) | 0/0/27 | 0 |
| tardie/inline/h50/d0/cold | — | — | 404.0 [327.5, 480.5]; 251.0…557.0 (n=2) | 404.0 [327.5, 480.5]; 251.0…557.0 (n=2) | 404.0 [327.5, 480.5]; 251.0…557.0 (n=2) | 223.0 [221.5, 224.5]; 220.0…226.0 (n=2) | 57.0 [53.5, 60.5]; 50.0…64.0 (n=2) | — | — | 1.0 [1.0, 1.5]; 1.0…2.0 (n=3) | 0/0/27 | 0 |
| tardie/inline/h50/d0/settling | — | — | 468.0 [456.5, 558.5]; 445.0…649.0 (n=3) | 468.0 [456.5, 558.5]; 445.0…649.0 (n=3) | 468.0 [456.5, 558.5]; 445.0…649.0 (n=3) | 10.0 [10.0, 10.5]; 10.0…11.0 (n=3) | 1.0 [0.5, 35.5]; 0.0…70.0 (n=3) | — | — | 1.0 [1.0, 1.0]; 1.0…1.0 (n=3) | 0/0/27 | 0 |
| yielded/production/h50/d0/settling | 9.0 [8.5, 16.0]; 8.0…23.0 (n=3) | 367.0 [286.0, 448.0]; 205.0…529.0 (n=2) | 149.0 [114.0, 184.0]; 79.0…219.0 (n=2) | 149.0 [114.0, 184.0]; 79.0…219.0 (n=2) | 149.0 [114.0, 184.0]; 79.0…219.0 (n=2) | — | — | — | — | 1.0 [1.0, 1.0]; 1.0…1.0 (n=3) | 0/27/0 | 0 |
| pi/inline/h50/d0/settling | — | — | — | — | — | 259.0 [212.0, 283.0]; 165.0…307.0 (n=3) | — | — | — | 0.0 [0.0, 0.0]; 0.0…0.0 (n=3) | 0/0/27 | 0 |
| pi/inline/h50/d0/warm | — | — | — | — | — | 259.0 [216.5, 285.5]; 174.0…312.0 (n=3) | — | — | — | 0.0 [0.0, 0.0]; 0.0…0.0 (n=3) | 0/0/81 | 0 |
| tardie/inline/h50/d0/warm | — | — | 538.5 [506.8, 570.3]; 475.0…602.0 (n=2) | 538.5 [506.8, 570.3]; 475.0…602.0 (n=2) | 538.5 [506.8, 570.3]; 475.0…602.0 (n=2) | 11.0 [10.5, 12.5]; 10.0…14.0 (n=3) | 0.5 [0.3, 0.8]; 0.0…1.0 (n=2) | — | — | 1.0 [1.0, 2.5]; 1.0…4.0 (n=3) | 0/0/81 | 0 |
| yielded/production/h50/d0/warm | 7.0 [7.0, 11.0]; 7.0…15.0 (n=3) | 282.0 [235.5, 365.5]; 189.0…449.0 (n=3) | 64.0 [34.5, 132.5]; 5.0…201.0 (n=3) | 64.0 [34.5, 132.5]; 5.0…201.0 (n=3) | 64.0 [34.5, 132.5]; 5.0…201.0 (n=3) | — | — | — | — | 1.0 [1.0, 1.0]; 1.0…1.0 (n=3) | 0/81/0 | 0 |
| pi/inline/h250/d400/cold | — | — | — | — | — | 523.0 [454.5, 622.5]; 386.0…722.0 (n=3) | — | — | — | 0.0 [0.0, 0.0]; 0.0…0.0 (n=3) | 0/0/27 | 0 |
| yielded/production/h250/d400/cold | 92.0 [73.0, 96.0]; 54.0…100.0 (n=3) | 581.0 [455.5, 615.5]; 330.0…650.0 (n=3) | 562.5 [454.3, 670.8]; 346.0…779.0 (n=2) | 562.5 [454.3, 670.8]; 346.0…779.0 (n=2) | 562.5 [454.3, 670.8]; 346.0…779.0 (n=2) | — | — | — | — | 1.0 [1.0, 1.0]; 1.0…1.0 (n=3) | 0/27/0 | 0 |
| tardie/inline/h250/d400/cold | — | — | 122.0 [115.5, 338.5]; 109.0…555.0 (n=3) | 122.0 [115.5, 199.8]; 109.0…277.5 (n=3) | 122.0 [115.5, 307.5]; 109.0…493.0 (n=3) | 741.0 [667.0, 889.0]; 593.0…1037.0 (n=3) | 74.0 [64.0, 84.0]; 54.0…94.0 (n=2) | — | — | 1.0 [1.0, 1.5]; 1.0…2.0 (n=3) | 0/0/27 | 0 |
| pi/inline/h250/d400/settling | — | — | — | — | — | 391.0 [346.0, 450.5]; 301.0…510.0 (n=3) | — | — | — | 0.0 [0.0, 0.0]; 0.0…0.0 (n=3) | 0/0/27 | 0 |
| yielded/production/h250/d400/settling | 14.0 [14.0, 17.0]; 14.0…20.0 (n=3) | 764.0 [505.0, 813.5]; 246.0…863.0 (n=3) | 9.0 [6.0, 184.0]; 3.0…359.0 (n=3) | 9.0 [6.0, 184.0]; 3.0…359.0 (n=3) | 9.0 [6.0, 184.0]; 3.0…359.0 (n=3) | — | — | — | — | 1.0 [1.0, 1.0]; 1.0…1.0 (n=3) | 0/27/0 | 0 |
| tardie/inline/h250/d400/settling | — | — | 355.0 [207.5, 469.0]; 60.0…583.0 (n=3) | 355.0 [192.5, 469.0]; 30.0…583.0 (n=3) | 355.0 [197.5, 469.0]; 40.0…583.0 (n=3) | 30.0 [27.5, 32.0]; 25.0…34.0 (n=3) | 42.5 [21.3, 63.8]; 0.0…85.0 (n=2) | — | — | 1.0 [1.0, 1.5]; 1.0…2.0 (n=3) | 0/0/27 | 0 |
| tardie/inline/h250/d400/warm | — | — | 395.0 [227.0, 498.0]; 59.0…601.0 (n=3) | 335.0 [197.0, 367.8]; 59.0…400.5 (n=3) | 373.0 [216.0, 473.0]; 59.0…573.0 (n=3) | 28.0 [21.5, 28.0]; 15.0…28.0 (n=3) | — | — | — | 2.0 [1.5, 2.0]; 1.0…2.0 (n=3) | 0/0/81 | 0 |
| yielded/production/h250/d400/warm | 15.0 [11.5, 16.5]; 8.0…18.0 (n=3) | 524.5 [392.8, 656.3]; 261.0…788.0 (n=2) | 78.0 [43.5, 112.5]; 9.0…147.0 (n=2) | 78.0 [43.5, 112.5]; 9.0…147.0 (n=2) | 78.0 [43.5, 112.5]; 9.0…147.0 (n=2) | — | — | — | — | 1.0 [1.0, 1.0]; 1.0…1.0 (n=3) | 0/81/0 | 0 |
| pi/inline/h250/d400/warm | — | — | — | — | — | 408.0 [355.0, 457.0]; 302.0…506.0 (n=3) | — | — | — | 0.0 [0.0, 0.0]; 0.0…0.0 (n=3) | 0/0/81 | 0 |
| tardie/inline/h50/d400/cold | — | — | 243.0 [231.0, 293.5]; 219.0…344.0 (n=3) | 243.0 [176.3, 293.5]; 109.5…344.0 (n=3) | 243.0 [185.0, 293.5]; 127.0…344.0 (n=3) | 448.0 [315.0, 467.5]; 182.0…487.0 (n=3) | 41.5 [36.3, 46.8]; 31.0…52.0 (n=2) | — | — | 1.0 [1.0, 1.5]; 1.0…2.0 (n=3) | 0/0/27 | 0 |
| pi/inline/h50/d400/cold | — | — | — | — | — | 344.0 [307.5, 401.0]; 271.0…458.0 (n=3) | — | — | — | 0.0 [0.0, 0.0]; 0.0…0.0 (n=3) | 0/0/27 | 0 |
| yielded/production/h50/d400/cold | 90.0 [70.0, 102.0]; 50.0…114.0 (n=3) | 558.0 [453.5, 562.5]; 349.0…567.0 (n=3) | 412.0 [313.0, 416.0]; 214.0…420.0 (n=3) | 412.0 [313.0, 416.0]; 214.0…420.0 (n=3) | 412.0 [313.0, 416.0]; 214.0…420.0 (n=3) | — | — | — | — | 1.0 [1.0, 1.0]; 1.0…1.0 (n=3) | 0/27/0 | 0 |
| yielded/production/h50/d400/settling | 12.0 [9.0, 12.5]; 6.0…13.0 (n=3) | 411.0 [337.0, 485.0]; 263.0…559.0 (n=2) | 136.0 [119.5, 152.5]; 103.0…169.0 (n=2) | 136.0 [119.5, 152.5]; 103.0…169.0 (n=2) | 136.0 [119.5, 152.5]; 103.0…169.0 (n=2) | — | — | — | — | 1.0 [1.0, 1.0]; 1.0…1.0 (n=3) | 0/27/0 | 0 |
| pi/inline/h50/d400/settling | — | — | — | — | — | 259.0 [213.5, 278.0]; 168.0…297.0 (n=3) | — | — | — | 0.0 [0.0, 0.0]; 0.0…0.0 (n=3) | 0/0/27 | 0 |
| tardie/inline/h50/d400/settling | — | — | 227.0 [171.0, 518.5]; 115.0…810.0 (n=3) | 227.0 [171.0, 316.0]; 115.0…405.0 (n=3) | 227.0 [171.0, 496.5]; 115.0…766.0 (n=3) | 19.0 [17.0, 21.0]; 15.0…23.0 (n=3) | 0.5 [0.3, 0.8]; 0.0…1.0 (n=2) | — | — | 1.0 [1.0, 1.5]; 1.0…2.0 (n=3) | 0/0/27 | 0 |
| pi/inline/h50/d400/warm | — | — | — | — | — | 242.0 [212.0, 281.0]; 182.0…320.0 (n=3) | — | — | — | 0.0 [0.0, 0.0]; 0.0…0.0 (n=3) | 0/0/81 | 0 |
| yielded/production/h50/d400/warm | 10.0 [8.5, 10.0]; 7.0…10.0 (n=3) | 524.0 [372.0, 525.5]; 220.0…527.0 (n=3) | 10.0 [7.5, 40.5]; 5.0…71.0 (n=3) | 10.0 [7.5, 40.5]; 5.0…71.0 (n=3) | 10.0 [7.5, 40.5]; 5.0…71.0 (n=3) | — | — | — | — | 1.0 [1.0, 1.0]; 1.0…1.0 (n=3) | 0/81/0 | 0 |
| tardie/inline/h50/d400/warm | — | — | 187.0 [121.5, 267.0]; 56.0…347.0 (n=3) | 187.0 [121.5, 267.0]; 56.0…347.0 (n=3) | 187.0 [121.5, 267.0]; 56.0…347.0 (n=3) | 23.0 [23.0, 23.0]; 23.0…23.0 (n=2) | 0.0 [0.0, 0.0]; 0.0…0.0 (n=1) | — | — | 1.0 [1.0, 1.0]; 1.0…1.0 (n=3) | 0/0/81 | 0 |

Repeated-turn spread within each Object

| Cohort | Objects | Absolute first-to-last driver drift | Driver range |
| --- | --- | --- | --- |
| yielded/production/h250/d0/warm | 3 | 223.0 [222.0, 367.0]; 221.0…511.0 (n=3) | 287.0 [266.0, 399.0]; 245.0…511.0 (n=3) |
| pi/inline/h250/d0/warm | 3 | 46.0 [40.0, 118.0]; 34.0…190.0 (n=3) | 46.0 [40.0, 118.0]; 34.0…190.0 (n=3) |
| tardie/inline/h250/d0/warm | 3 | 271.0 [157.0, 328.5]; 43.0…386.0 (n=3) | 615.0 [584.5, 770.0]; 554.0…925.0 (n=3) |
| pi/inline/h50/d0/warm | 3 | 156.0 [87.5, 163.5]; 19.0…171.0 (n=3) | 156.0 [91.5, 163.5]; 27.0…171.0 (n=3) |
| tardie/inline/h50/d0/warm | 3 | 278.0 [199.5, 295.5]; 121.0…313.0 (n=3) | 313.0 [251.5, 382.5]; 190.0…452.0 (n=3) |
| yielded/production/h50/d0/warm | 3 | 122.0 [79.0, 346.5]; 36.0…571.0 (n=3) | 122.0 [79.0, 346.5]; 36.0…571.0 (n=3) |
| tardie/inline/h250/d400/warm | 3 | 424.0 [227.5, 473.5]; 31.0…523.0 (n=3) | 523.0 [395.5, 532.5]; 268.0…542.0 (n=3) |
| yielded/production/h250/d400/warm | 3 | 178.0 [119.5, 226.0]; 61.0…274.0 (n=3) | 255.0 [183.0, 264.5]; 111.0…274.0 (n=3) |
| pi/inline/h250/d400/warm | 3 | 90.0 [46.0, 127.5]; 2.0…165.0 (n=3) | 98.0 [94.0, 131.5]; 90.0…165.0 (n=3) |
| pi/inline/h50/d400/warm | 3 | 162.0 [101.0, 201.0]; 40.0…240.0 (n=3) | 175.0 [129.0, 217.5]; 83.0…260.0 (n=3) |
| yielded/production/h50/d400/warm | 3 | 18.0 [16.5, 21.0]; 15.0…24.0 (n=3) | 69.0 [49.0, 97.5]; 29.0…126.0 (n=3) |
| tardie/inline/h50/d400/warm | 3 | 78.0 [45.5, 491.0]; 13.0…904.0 (n=3) | 590.0 [350.5, 750.0]; 111.0…910.0 (n=3) |

All captured invocation outcomes (all phases)

| Worker / model / type / outcome | Count |
| --- | --- |
| rebench-54128957-primary/durableObject/jsrpc/ok | 1695 |
| rebench-54128957-primary/durableObject/alarm/ok | 3117 |
| rebench-54128957-primary/stateless/fetch/ok | 1625 |
| rebench-54128957-primary/durableObject/alarm/canceled | 1078 |
| rebench-54128957-primary/durableObject/fetch/ok | 1753 |
| rebench-54128957-primary/durableObject/jsrpc/aborted | 44 |
| rebench-54128957-primary/durableObject/fetch/aborted | 130 |
| rebench-54128957-primary/durableObject/fetch/exceededMemory | 1 |
| rebench-54128957-primary/durableObject/alarm/exceededMemory | 1 |
| rebench-54128957-primary/stateless/fetch/canceled | 2 |
| rebench-54128957-primary/durableObject/alarm/aborted | 1 |
| rebench-54128957-primary/durableObject/jsrpc/canceled | 1 |
| rebench-54128957-provider/stateless/fetch/ok | 2305 |

Coverage: 180/180 planned turns in complete cohorts; 9 failed controller requests; 1258 non-ok invocation outcomes; 161 join/observation issues. Full inventory: failed-outcomes.json.
Retired evidence: 11 turns across 1 excluded Object groups; 0 warm-incarnation proof failures retained.

* Bounds show lower…upper limits for the Object-level median, conditional on a stable provider/driver offset consistent with both echo probes. Their full Q1/Q3/range and per-Object intervals are in summary.json. Gaps use provider clocks. Negative values are retained. These residuals do not isolate alarm dispatch, processing entry or storage notification; invalid DO cross-clock differences remain only in turns.jsonl diagnosticClock.
† I/O-clock duration, never CPU; zero does not establish zero work. Missing waiterReads remains missing.
‡ Whole observed alarm invocations can extend beyond client completion. START-only identity joins retain conflicting END contexts separately. Partial or ambiguous start joins remain missing; per-invocation and boundary details are in turns.jsonl.

- Primary driverTotalMs is response.turnMs from the deployed driver; controller clientWallMs includes routing, clock probes and diagnostics and is not the primary latency.
- Object is the unit: median within each Object, then median/Q1/Q3/min/max/range across Objects (type-7 quantiles: linear interpolation at (n - 1) × p). Missing any repeated metric leaves that Object's metric null. Complete planned cohorts alone enter groups; partial turns remain in turns.jsonl.
- Each target has three independent Objects per history/TTFT condition in the same primary Worker. m0 follows an acknowledged cold abort, m1 settles, and m2/m3/m4 are three warm repeats. Cold proves a new Object incarnation, not a fresh isolate. Pi and Tardie use their native paths (variant inline in raw data); Yielded uses production only. Tardie cold proof requires Thread and Actor abort/entry evidence. No Actor.begin RPC precedes the timed turn: when native lookup/allocate uses the Actor, firstNativeEntry is captured before the unchanged implementation and read afterward. If nativeEntryObserved is false, the Actor was unused by the native turn; its post-timer diagnostic entry proves fresh/reset/no-prior-alarm state, but Actor cold hydration was neither required by that path nor charged to the turn. The directory receipt retains nativeEntryObserved and its observation string.
- Date.now and Effect span timestamps are frozen I/O clocks, not CPU clocks. Zero span duration means zero observable I/O-clock advance, not zero ownership or recovery cost. Nested/overlapping spans are not additive component costs.
- DO-to-driver timestamp differences are invalid for wall decomposition: the pilot demonstrates a lagging, frozen DO clock. diagnosticClock retains these raw differences, including negatives; they do not prove overlap, alarm dispatch, ownership cost, or notification delay and are excluded from comparison summaries.
- Provider/driver brackets intersect before/after echo offset intervals [provider arrival minus driver after, provider arrival minus driver before]. They are conditional on a stable offset across the same-colo provider invocations, not a guarantee of synchronized clocks. Missing, different-colo or inconsistent probes yield null bounds. Raw values are retained and never clipped.
- Receipt-to-first-provider is a combined alarm/setup/transport residual, not exact dispatch or processing latency. Last-provider-end to client includes finalization, storage gates and RPC return; it cannot isolate storage notification. Provider-to-provider gaps remain provider I/O-clock observations across invocations.
- settlementWrites records a logical SQL statement at dispatch, before SQL completion; it does not confirm replication. Publication span end and settlement.settledAt are separate observations, neither a replication acknowledgement.
- CPU comes only from cf-worker-event telemetry. Each generated RPC/alarm ID's START log must join to one Worker-scoped requestId and one matching invocation. Cloudflare can attach an alarm END to an await RPC; end-context disagreements are inventoried without overriding the start join. Missing, ambiguous, non-ok, or incomplete CPU is null. Sampling can omit whole invocations.
- CPU partitions are Cloudflare invocation attribution, not semantic phase costs: await CPU can include concurrent alarm work. Generic telemetry timestamp is not treated as invocation start. Only an explicit eventTimestamp can supply a platform start; otherwise start and start-plus-wallTime remain unresolved.
- Native model calls can carry inline/alarm or missing AsyncLocalStorage contexts. modelInvocationKinds counts these observations; neither ALS context nor native invocation CPU establishes logical Effect fiber ownership or Run ownership transfer. Yielded production requires every model call to carry an alarm context.
- Cloudflare invocation wall time is not caller wall time. Whole-invocation CPU and wall values may extend beyond driver completion; boundary alarms are identified and prevent a turn-attributed CPU total. No sum is a critical-path decomposition.
- Metrics are read after driver completion. Observed alarms, SQL counters, spans, waiterReads and event acquisitions can include follow-up work. A missing alarm end in the receipt is not proof of failure.
- Waiter-read intervals are optional SQL-read observations on the Object I/O clock. Empty waiterReads means no reads matched the observer, not proof of no waiting or polling. Repeated intervals near 500 ms can describe polling cadence, but are not proof of which notification or fallback caused a wake.
- Runtime is cached per Object incarnation: a stable incarnation implies zero new runtime initializations between passes. eventAcquisitions counts event-layer acquisition, not runtime construction; runRecovery spans separately describe observed recovery work.
- SSE provider receipts are preferred; a unique matching provider log is an explicit fallback. Fallbacks, missing logs, conflicting logs, telemetry poll metadata, and all non-ok outcomes remain inventoried.
- The eight headline cells are history 50/250 × TTFT 0/400 × cold/warm. Yielded/pi is the ratio of the two across-Object driver medians, not a paired same-Object effect: matching base names belong to separate namespaces. Ratios require all three Objects for both targets. Settling remains diagnostic only.
- Across-Object spread uses Object medians; repeatSpread measures first-to-last drift and range within each Object. Stable repeats on a slow Object remain in the across-Object distribution. Retired Object groups remain excluded evidence, including failures. No significance or causal improvement claim follows from this small run.
- Tardie driver timing surrounds the Thread fetch. Thread metrics and the directory Actor identity/entry receipt are read afterward; directoryStart copies that same post-timer receipt, not a pre-timer RPC. Provider calls remain in metrics.calls. Actor native lookup/allocate CPU requires an exact trace from a uniquely joined Thread fetch or alarm invocation for the turn and matching Actor identity/version. An Actor invocation claimed by multiple turns has null attributed CPU/wall in every claimant; raw observations remain retained. Actor alarms and counters are not scoped to turns: actorAlarmCpuMs, actorCpuMs and actorInvocationWallMs remain null, never inferred zero. Sampling can omit native RPCs, so their observed CPU is not a complete Actor or Tardie total. Thread alarm instrumentation remains separate and intact; all captured Actor events remain in the all-phase inventory.
