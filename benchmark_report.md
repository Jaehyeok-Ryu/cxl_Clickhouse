# 📊 ClickHouse Vectorized NUMA TPC-H Report

- **Scale Factor**: 1.0 GB
- **Memory Policy Applied**: `ddr-only`
- **Execution Mode**: `throughput`

## 🎛️ Real-time NUMA Physical Node Allocation
```text

Per-node numastat info (in MBs):
                Node 0 Node 1 Node 2 Node 3  Total
                ------ ------ ------ ------ ------
Numa_Hit        501827 340317  11652   2869 856665
Numa_Miss            0      0      0      0      0
Numa_Foreign         0      0      0      0      0
Interleave_Hit   18657  18602   2864   2868  42991
Local_Node      498071 336833      0      0 834904
Other_Node        3756   3484  11652   2869  21761

```

## 📥 Parallel Data Loading Performance
- **Total Loading Duration**: **36.90 seconds**

## 📈 Concurrent Streams Results (Throughput Test)
- **Number of Streams**: 4
- **Total Elapsed Time ($T_s$)**: **39.18 seconds**
- **🏆 TPC-H Throughput Metric ($Qth@Size$)**: **8085.37 Qth/Hour**

### ⏱️ Individual Streams Query Latency Breakdowns (seconds)
| Query ID | Stream 1 | Stream 2 | Stream 3 | Stream 4 |
| :---: | :---: | :---: | :---: | :---: |
| Q01 | 0.593 | 0.528 | 0.465 | 0.483 |
| Q02 | 0.714 | 0.534 | 0.620 | 0.488 |
| Q03 | 1.663 | 1.508 | 1.754 | 1.564 |
| Q04 | 1.189 | 1.245 | 1.216 | 1.249 |
| Q05 | 2.239 | 2.221 | 3.050 | 2.323 |
| Q06 | 0.230 | 0.156 | 0.232 | 0.196 |
| Q07 | 1.014 | 1.250 | 1.037 | 1.165 |
| Q08 | 2.356 | 2.599 | 2.696 | 2.717 |
| Q09 | 5.188 | 4.899 | 4.739 | 6.112 |
| Q10 | 2.453 | 2.834 | 2.842 | 2.595 |
| Q11 | 0.333 | 0.466 | 0.389 | 0.383 |
| Q12 | 0.308 | 0.356 | 0.372 | 0.260 |
| Q13 | 5.613 | 5.920 | 5.711 | 5.855 |
| Q14 | 0.664 | 0.498 | 0.559 | 0.449 |
| Q15 | 0.256 | 0.257 | 0.265 | 0.253 |
| Q16 | 0.482 | 0.337 | 0.404 | 0.399 |
| Q17 | 1.213 | 1.024 | 1.055 | 0.940 |
| Q18 | 4.161 | 4.883 | 4.378 | 4.266 |
| Q19 | 0.492 | 0.577 | 0.432 | 0.435 |
| Q20 | 1.110 | 0.996 | 0.985 | 0.952 |
| Q21 | 4.554 | 4.535 | 4.751 | 5.007 |
| Q22 | 0.521 | 0.469 | 0.419 | 0.498 |

## 💡 Architectural Insights
- **Vectorized Layout**: ClickHouse utilizes continuous columnar memory vectors. It forces CPU registers to execute packed SIMD (AVX/AVX-512) loops directly.
- **Socket Isolation**: Strictly binds Shards to local Socket DDR or remote CXL structures to clearly measure asymmetric bus bottlenecks.
