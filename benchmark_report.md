# 📊 ClickHouse Vectorized NUMA TPC-H Report

- **Scale Factor**: 1.0 GB
- **Memory Policy Applied**: `ddr-only`
- **Execution Mode**: `throughput`

## 🎛️ Real-time NUMA Physical Node Allocation
```text

Per-node numastat info (in MBs):
                Node 0 Node 1 Node 2  Total
                ------ ------ ------ ------
Numa_Hit        120858  79420      1 200278
Numa_Miss            0      0      0      0
Numa_Foreign         0      0      0      0
Interleave_Hit      26     23      0     49
Local_Node      120802  79381      0 200183
Other_Node          56     38      1     95

```

## 📥 Parallel Data Loading Performance
- **Total Loading Duration**: **36.41 seconds**

## 📈 Concurrent Streams Results (Throughput Test)
- **Number of Streams**: 2
- **Total Elapsed Time ($T_s$)**: **31.33 seconds**
- **🏆 TPC-H Throughput Metric ($Qth@Size$)**: **5056.12 Qth/Hour**

### ⏱️ Individual Streams Query Latency Breakdowns (seconds)
| Query ID | Stream 1 | Stream 2 |
| :---: | :---: | :---: |
| Q01 | 0.396 | 0.533 |
| Q02 | 0.465 | 0.485 |
| Q03 | 0.754 | 1.346 |
| Q04 | 1.051 | 0.937 |
| Q05 | 1.906 | 1.748 |
| Q06 | 0.181 | 0.202 |
| Q07 | 0.856 | 0.915 |
| Q08 | 2.133 | 2.431 |
| Q09 | 4.153 | 3.966 |
| Q10 | 2.373 | 2.287 |
| Q11 | 0.326 | 0.363 |
| Q12 | 0.293 | 0.197 |
| Q13 | 4.602 | 4.487 |
| Q14 | 0.434 | 0.412 |
| Q15 | 0.222 | 0.234 |
| Q16 | 0.399 | 0.345 |
| Q17 | 0.706 | 0.757 |
| Q18 | 3.289 | 3.996 |
| Q19 | 0.385 | 0.406 |
| Q20 | 0.842 | 0.829 |
| Q21 | 4.041 | 3.770 |
| Q22 | 0.401 | 0.442 |

## 💡 Architectural Insights
- **Vectorized Layout**: ClickHouse utilizes continuous columnar memory vectors. It forces CPU registers to execute packed SIMD (AVX/AVX-512) loops directly.
- **Socket Isolation**: Strictly binds Shards to local Socket DDR or remote CXL structures to clearly measure asymmetric bus bottlenecks.
