# 📊 ClickHouse Vectorized NUMA TPC-H Report

- **Scale Factor**: 30.0 GB
- **Memory Policy Applied**: `ddr-only`
- **Execution Mode**: `throughput`

## 🎛️ Real-time NUMA Physical Node Allocation
```text

Per-node numastat info (in MBs):
                  Node 0   Node 1 Node 2 Node 3    Total
                -------- -------- ------ ------ --------
Numa_Hit        22386694 13356244 670262 667044 37080244
Numa_Miss           1365        0    394   1248     3007
Numa_Foreign         394     2613      0      0     3007
Interleave_Hit   3500229  3525950 538396 542421  8106996
Local_Node      21841784 12835583      0      0 34677368
Other_Node        546275   520661 670656 668291  2405883

```

## 📥 Parallel Data Loading Performance
- **Total Loading Duration**: **37.11 seconds**

## 📈 Concurrent Streams Results (Throughput Test)
- **Number of Streams**: 6
- **Total Elapsed Time ($T_s$)**: **46.62 seconds**
- **🏆 TPC-H Throughput Metric ($Qth@Size$)**: **10192.72 Qth/Hour**

### ⏱️ Individual Streams Query Latency Breakdowns (seconds)
| Query ID | Stream 1 | Stream 2 | Stream 3 | Stream 4 | Stream 5 | Stream 6 |
| :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| Q01 | 0.664 | 0.746 | 0.492 | 0.674 | 0.622 | 0.693 |
| Q02 | 0.635 | 0.734 | 0.699 | 0.799 | 0.844 | 0.658 |
| Q03 | 2.664 | 1.476 | 1.882 | 1.557 | 1.936 | 1.457 |
| Q04 | 1.339 | 0.949 | 1.293 | 1.522 | 1.727 | 1.269 |
| Q05 | 2.656 | 2.560 | 2.423 | 2.528 | 3.500 | 3.453 |
| Q06 | 0.362 | 0.291 | 0.218 | 0.376 | 0.219 | 0.202 |
| Q07 | 1.415 | 1.927 | 2.080 | 1.473 | 1.718 | 1.306 |
| Q08 | 3.109 | 3.114 | 3.108 | 3.392 | 3.804 | 3.154 |
| Q09 | 6.961 | 5.580 | 5.924 | 5.243 | 5.935 | 5.931 |
| Q10 | 3.240 | 2.976 | 3.177 | 3.324 | 2.881 | 3.498 |
| Q11 | 0.483 | 0.483 | 0.460 | 0.705 | 0.417 | 0.258 |
| Q12 | 0.293 | 0.252 | 0.336 | 0.441 | 0.292 | 0.472 |
| Q13 | 5.774 | 6.474 | 6.487 | 6.887 | 6.048 | 6.160 |
| Q14 | 0.543 | 0.617 | 0.808 | 0.893 | 0.499 | 0.535 |
| Q15 | 0.369 | 0.435 | 0.266 | 0.319 | 0.273 | 0.309 |
| Q16 | 0.567 | 0.426 | 0.554 | 0.520 | 0.625 | 0.349 |
| Q17 | 1.016 | 1.272 | 1.247 | 1.251 | 1.082 | 1.232 |
| Q18 | 7.103 | 5.004 | 4.817 | 5.491 | 5.369 | 5.058 |
| Q19 | 0.515 | 0.514 | 0.386 | 0.472 | 0.399 | 0.383 |
| Q20 | 1.075 | 1.333 | 1.178 | 2.281 | 1.931 | 1.176 |
| Q21 | 5.096 | 7.743 | 6.041 | 4.558 | 4.554 | 7.610 |
| Q22 | 0.741 | 0.770 | 0.577 | 0.467 | 0.537 | 0.565 |

## 💡 Architectural Insights
- **Vectorized Layout**: ClickHouse utilizes continuous columnar memory vectors. It forces CPU registers to execute packed SIMD (AVX/AVX-512) loops directly.
- **Socket Isolation**: Strictly binds Shards to local Socket DDR or remote CXL structures to clearly measure asymmetric bus bottlenecks.
