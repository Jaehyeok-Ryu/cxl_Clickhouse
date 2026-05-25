# 📊 ClickHouse Vectorized NUMA TPC-H Report

- **Scale Factor**: 30.0 GB
- **Memory Policy Applied**: `weighted`
- **Execution Mode**: `throughput`

## 🎛️ Real-time NUMA Physical Node Allocation
```text

Per-node numastat info (in MBs):
                  Node 0   Node 1 Node 2 Node 3    Total
                -------- -------- ------ ------ --------
Numa_Hit        21760944 12884952 670253 667033 35983182
Numa_Miss           1365        0    394   1248     3007
Numa_Foreign         394     2613      0      0     3007
Interleave_Hit   3500169  3525881 538386 542411  8106848
Local_Node      21216500 12364292      0      0 33580791
Other_Node        545809   520660 670647 668281  2405397

```

## 📥 Parallel Data Loading Performance
- **Total Loading Duration**: **36.77 seconds**

## 📈 Concurrent Streams Results (Throughput Test)
- **Number of Streams**: 6
- **Total Elapsed Time ($T_s$)**: **47.72 seconds**
- **🏆 TPC-H Throughput Metric ($Qth@Size$)**: **9957.88 Qth/Hour**

### ⏱️ Individual Streams Query Latency Breakdowns (seconds)
| Query ID | Stream 1 | Stream 2 | Stream 3 | Stream 4 | Stream 5 | Stream 6 |
| :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| Q01 | 0.368 | 0.380 | 0.607 | 0.647 | 0.472 | 0.551 |
| Q02 | 0.704 | 0.740 | 0.994 | 0.788 | 1.317 | 1.392 |
| Q03 | 1.742 | 1.369 | 1.604 | 1.773 | 1.226 | 2.115 |
| Q04 | 2.008 | 1.903 | 1.517 | 1.747 | 0.836 | 2.051 |
| Q05 | 2.865 | 2.713 | 2.376 | 2.363 | 2.168 | 2.845 |
| Q06 | 0.134 | 0.204 | 0.189 | 0.164 | 0.211 | 0.354 |
| Q07 | 2.952 | 1.540 | 1.353 | 1.286 | 1.563 | 1.302 |
| Q08 | 5.033 | 3.823 | 2.783 | 3.263 | 3.916 | 3.439 |
| Q09 | 6.155 | 5.251 | 5.800 | 5.774 | 8.283 | 5.946 |
| Q10 | 2.774 | 4.072 | 3.836 | 2.839 | 3.191 | 2.838 |
| Q11 | 0.266 | 0.315 | 0.486 | 0.322 | 0.451 | 0.400 |
| Q12 | 0.420 | 0.263 | 0.283 | 0.246 | 0.451 | 0.276 |
| Q13 | 5.931 | 6.042 | 5.963 | 6.552 | 5.974 | 6.801 |
| Q14 | 0.581 | 0.514 | 0.566 | 0.616 | 0.657 | 0.750 |
| Q15 | 0.252 | 0.285 | 0.400 | 0.317 | 0.471 | 0.330 |
| Q16 | 0.544 | 0.917 | 0.538 | 0.543 | 0.486 | 0.510 |
| Q17 | 1.304 | 1.800 | 1.422 | 2.040 | 1.765 | 1.364 |
| Q18 | 5.349 | 5.957 | 6.668 | 5.710 | 6.860 | 4.913 |
| Q19 | 0.375 | 0.405 | 0.506 | 0.481 | 0.407 | 0.377 |
| Q20 | 1.016 | 2.086 | 1.420 | 1.520 | 1.011 | 1.055 |
| Q21 | 5.752 | 5.404 | 6.187 | 6.872 | 5.412 | 6.254 |
| Q22 | 0.654 | 0.635 | 1.144 | 0.604 | 0.587 | 0.503 |

## 💡 Architectural Insights
- **Vectorized Layout**: ClickHouse utilizes continuous columnar memory vectors. It forces CPU registers to execute packed SIMD (AVX/AVX-512) loops directly.
- **Socket Isolation**: Strictly binds Shards to local Socket DDR or remote CXL structures to clearly measure asymmetric bus bottlenecks.
