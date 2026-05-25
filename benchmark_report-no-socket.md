# 📊 ClickHouse Vectorized NUMA TPC-H Report

- **Scale Factor**: 30.0 GB
- **Memory Policy Applied**: `weighted`
- **Execution Mode**: `throughput`

## 🎛️ Real-time NUMA Physical Node Allocation
```text

Per-node numastat info (in MBs):
                  Node 0   Node 1 Node 2 Node 3    Total
                -------- -------- ------ ------ --------
Numa_Hit        21569080 12748495 650820 646848 35615243
Numa_Miss           1365        0    394   1248     3007
Numa_Foreign         394     2613      0      0     3007
Interleave_Hit   3373862  3394687 518954 522227  7809731
Local_Node      21024838 12227937      0      0 33252775
Other_Node        545607   520558 651214 648096  2365475

```

## 📥 Parallel Data Loading Performance
- **Total Loading Duration**: **37.22 seconds**

## 📈 Concurrent Streams Results (Throughput Test)
- **Number of Streams**: 6
- **Total Elapsed Time ($T_s$)**: **57.96 seconds**
- **🏆 TPC-H Throughput Metric ($Qth@Size$)**: **8199.23 Qth/Hour**

### ⏱️ Individual Streams Query Latency Breakdowns (seconds)
| Query ID | Stream 1 | Stream 2 | Stream 3 | Stream 4 | Stream 5 | Stream 6 |
| :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| Q01 | 0.651 | 0.609 | 0.497 | 0.551 | 0.747 | 0.978 |
| Q02 | 0.628 | 1.195 | 1.077 | 0.687 | 0.605 | 0.991 |
| Q03 | 3.086 | 1.635 | 1.872 | 1.728 | 1.846 | 1.406 |
| Q04 | 1.760 | 2.686 | 1.992 | 1.235 | 1.789 | 1.912 |
| Q05 | 6.273 | 2.720 | 4.647 | 3.225 | 2.865 | 3.002 |
| Q06 | 0.271 | 0.284 | 0.355 | 0.281 | 0.285 | 0.252 |
| Q07 | 1.386 | 1.607 | 2.361 | 1.406 | 2.496 | 2.751 |
| Q08 | 3.070 | 3.388 | 3.618 | 3.638 | 7.206 | 3.426 |
| Q09 | 5.794 | 6.668 | 11.226 | 6.284 | 5.958 | 6.871 |
| Q10 | 3.427 | 3.905 | 2.940 | 3.488 | 4.388 | 4.998 |
| Q11 | 0.666 | 0.423 | 0.612 | 0.357 | 0.616 | 0.595 |
| Q12 | 0.543 | 0.325 | 0.739 | 0.862 | 0.327 | 0.506 |
| Q13 | 7.829 | 7.153 | 6.210 | 8.061 | 6.232 | 6.413 |
| Q14 | 0.614 | 0.715 | 0.550 | 0.730 | 0.707 | 1.206 |
| Q15 | 0.329 | 0.283 | 0.244 | 0.235 | 0.352 | 0.288 |
| Q16 | 0.765 | 0.439 | 0.502 | 0.700 | 0.724 | 0.625 |
| Q17 | 1.682 | 1.054 | 1.310 | 2.475 | 1.512 | 0.990 |
| Q18 | 7.860 | 7.407 | 7.504 | 9.183 | 9.271 | 7.908 |
| Q19 | 0.509 | 0.511 | 0.567 | 0.496 | 0.546 | 0.683 |
| Q20 | 1.285 | 1.376 | 1.936 | 1.191 | 1.373 | 1.211 |
| Q21 | 8.217 | 8.070 | 6.157 | 8.150 | 6.725 | 9.125 |
| Q22 | 1.309 | 1.414 | 0.812 | 1.023 | 1.107 | 0.825 |

## 💡 Architectural Insights
- **Vectorized Layout**: ClickHouse utilizes continuous columnar memory vectors. It forces CPU registers to execute packed SIMD (AVX/AVX-512) loops directly.
- **Socket Isolation**: Strictly binds Shards to local Socket DDR or remote CXL structures to clearly measure asymmetric bus bottlenecks.
