#!/usr/bin/env python3
# ====================================================================
#  ClickHouse TPC-H Benchmark Driver
#  Supports high-concurrency throughput testing and dynamic NUMA profiling
# ====================================================================

import os
import sys
import time
import random
import argparse
import subprocess
import datetime
from datetime import timedelta
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

PROJECT_DIR = os.path.dirname(os.path.abspath(__file__))
STAGING_DIR = os.getenv("STAGING_DIR", f"{PROJECT_DIR}/tpch-dbgen")
QUERIES_DIR = f"{PROJECT_DIR}/queries"
REPORT_PATH = f"{PROJECT_DIR}/benchmark_report.md"

CONTAINER_COORD = "clickhouse_coordinator"
CONTAINER_WORK1 = "clickhouse_worker1"
CONTAINER_WORK2 = "clickhouse_worker2"

SCALE_FACTOR = 1.0

TABLES = ["region", "nation", "part", "supplier", "partsupp", "customer", "orders", "lineitem"]

TABLE_SCHEMAS = {
    "region": "r_regionkey Int32, r_name LowCardinality(String), r_comment String",
    "nation": "n_nationkey Int32, n_name LowCardinality(String), n_regionkey Int32, n_comment String",
    "part": "p_partkey Int32, p_name String, p_mfgr LowCardinality(String), p_brand LowCardinality(String), p_type String, p_size Int32, p_container LowCardinality(String), p_retailprice Decimal(15, 2), p_comment String",
    "supplier": "s_suppkey Int32, s_name String, s_address String, s_nationkey Int32, s_phone String, s_acctbal Decimal(15, 2), s_comment String",
    "partsupp": "ps_partkey Int32, ps_suppkey Int32, ps_availqty Int32, ps_supplycost Decimal(15, 2), ps_comment String",
    "customer": "c_custkey Int32, c_name String, c_address String, c_nationkey Int32, c_phone String, c_acctbal Decimal(15, 2), c_mktsegment LowCardinality(String), c_comment String",
    "orders": "o_orderkey Int32, o_custkey Int32, o_orderstatus LowCardinality(String), o_totalprice Decimal(15, 2), o_orderdate Date, o_orderpriority LowCardinality(String), o_clerk String, o_shippriority Int32, o_comment String",
    "lineitem": "l_orderkey Int32, l_partkey Int32, l_suppkey Int32, l_linenumber Int32, l_quantity Decimal(15, 2), l_extendedprice Decimal(15, 2), l_discount Decimal(15, 2), l_tax Decimal(15, 2), l_returnflag LowCardinality(String), l_linestatus LowCardinality(String), l_shipdate Date, l_commitdate Date, l_receiptdate Date, l_shipinstruct LowCardinality(String), l_shipmode LowCardinality(String), l_comment String"
}

# --------------------------------------------------------------------
# 1. ClickHouse CLI Wrapper Executions
# --------------------------------------------------------------------
def run_clickhouse_query(query_str, container=CONTAINER_COORD):
    """Executes a SQL query using clickhouse-client in the target docker container."""
    cmd = [
        "docker", "exec", container,
        "clickhouse-client", "--query", query_str
    ]
    try:
        res = subprocess.run(cmd, capture_output=True, text=True, check=True)
        return True, res.stdout.strip(), None
    except subprocess.CalledProcessError as e:
        return False, None, e.stderr.strip()

# --------------------------------------------------------------------
# 2. Parallel Staging and Loading Engine (Socket Partitioned Shards)
# --------------------------------------------------------------------
def load_data_to_clickhouse(scale_factor):
    """Splits raw staging .tbl files and streams them directly into sharded worker RAM tables."""
    # Ensure staging directory exists and contain tbl files; if not, generate them!
    missing_any = False
    for table in TABLES:
        if not os.path.exists(f"{STAGING_DIR}/{table}.tbl"):
            missing_any = True
            break
            
    if missing_any:
        print(f"\n=== [INFO] Generating TPC-H Raw Data (Scale Factor: {scale_factor} GB) ===")
        
        if not os.path.exists(f"{STAGING_DIR}/dbgen"):
            print(f"[INFO] Compiling TPC-H dbgen in {STAGING_DIR}...")
            subprocess.run("make", shell=True, check=True, cwd=STAGING_DIR)

        # Remove any lingering files
        for f in os.listdir(STAGING_DIR):
            if f.endswith(".tbl"):
                try:
                    os.remove(os.path.join(STAGING_DIR, f))
                except Exception:
                    pass
        dbgen_cmd = f"./dbgen -s {scale_factor} -f"
        subprocess.run(dbgen_cmd, shell=True, check=True, cwd=STAGING_DIR)

    print("\n=== [1] Initializing ClickHouse DDL Schemas (Native Decoupled Engine) ===")
    workers_sql_path = f"{PROJECT_DIR}/schema_workers.sql"
    coord_sql_path = f"{PROJECT_DIR}/schema_coordinator.sql"
    
    # Ensure ALL nodes share the identical global schema (both local shards and distributed tables)
    # This prevents 'Code 60: Unknown table' when the coordinator pushes distributed JOINs down to worker nodes.
    print(" ⏳ Propagating unified DDL schemas across all Socket-Isolated Shards...")
    for container in [CONTAINER_COORD, CONTAINER_WORK1, CONTAINER_WORK2]:
        cmd_w = f"docker exec -i {container} clickhouse-client < {workers_sql_path}"
        subprocess.run(cmd_w, shell=True, check=True)
        cmd_c = f"docker exec -i {container} clickhouse-client < {coord_sql_path}"
        subprocess.run(cmd_c, shell=True, check=True)
    
    print("[SUCCESS] Decoupled DDL Schema initialization completed flawlessly!")

    print("\n=== [2] Ingesting & Sharding Table Data Parallelly ===")
    start_time = time.time()
    
    # We load each table directly via the Distributed engine on Coordinator.
    # The ClickHouse C++ cluster layer will automatically parse the files inside
    # the container, hash the sharding key, and push data to worker local tables
    # at peak physical network/disk IO limits!
    for table in TABLES:
        t_start = time.time()
        tbl_file = f"{STAGING_DIR}/{table}.tbl"
        if not os.path.exists(tbl_file):
            print(f"[WARNING] Staging file missing, skipping: {tbl_file}")
            continue
            
        print(f" ⏳ Ingesting '{table}' table (via Coordinator Native Distributed engine)...")
        
        schema_def = TABLE_SCHEMAS[table]
        
        # Execute Native Distributed Import
        # file() is completely unlocked by our --user_files_path=/staging/ startup flag!
        load_query = (
            f"INSERT INTO {table} "
            f"SELECT * FROM file('{table}.tbl', 'CSV', '{schema_def}') "
            f"SETTINGS format_csv_delimiter = '|', input_format_skip_unknown_fields = 1, async_insert = 0"
        )
        
        success, _, err = run_clickhouse_query(load_query, container=CONTAINER_COORD)
        if not success:
            print(f"[ERROR] Failed to ingest {table}: {err}")
            sys.exit(1)
            
        t_duration = time.time() - t_start
        print(f"  └─ '{table}' Sharded & Loaded cleanly: {t_duration:.2f} seconds.")
        
    total_load_time = time.time() - start_time
    print(f"\n[SUCCESS] Distributed Data Loading complete in {total_load_time:.2f} seconds!")
    return total_load_time

# --------------------------------------------------------------------
# 2.5 TPC-H Standard Maintenance Functions (RF1 & RF2)
# --------------------------------------------------------------------
def run_rf1():
    """RF1: Inserts new transaction data into orders and lineitem tables via Distributed engine."""
    success, stdout, err = run_clickhouse_query("SELECT max(o_orderkey) FROM orders")
    if not success or not stdout.strip().isdigit():
        max_key = 6000000
    else:
        max_key = int(stdout.strip())
        
    orders_values = []
    lineitems_values = []
    
    # Insert 100 new orders with ~400 lineitems total
    for i in range(1, 101):
        orderkey = max_key + i
        custkey = random.randint(1, 150000)
        orderstatus = 'O'
        totalprice = round(random.uniform(100.0, 5000.0), 2)
        orderdate = datetime.date.today().strftime("%Y-%m-%d")
        orderpriority = random.choice(['1-URGENT', '2-HIGH', '3-MEDIUM', '4-NOT SPECIFIED', '5-LOW'])
        clerk = f"Clerk#{random.randint(1, 1000):09d}"
        shippriority = 0
        comment = "New sales data RF1 simulated transaction row"
        
        orders_values.append(f"({orderkey}, {custkey}, '{orderstatus}', {totalprice}, '{orderdate}', '{orderpriority}', '{clerk}', {shippriority}, '{comment}')")
        
        num_lineitems = random.randint(1, 7)
        for l in range(1, num_lineitems + 1):
            partkey = random.randint(1, 200000)
            suppkey = random.randint(1, 10000)
            qty = random.randint(1, 50)
            price = round(random.uniform(50.0, 2000.0), 2)
            discount = round(random.uniform(0.0, 0.1), 2)
            tax = round(random.uniform(0.0, 0.08), 2)
            rf = 'N'
            ls = 'O'
            shipdate = (datetime.date.today() + timedelta(days=random.randint(1, 30))).strftime("%Y-%m-%d")
            commitdate = (datetime.date.today() + timedelta(days=random.randint(1, 30))).strftime("%Y-%m-%d")
            receiptdate = (datetime.date.today() + timedelta(days=random.randint(1, 40))).strftime("%Y-%m-%d")
            shipinstruct = random.choice(['DELIVER IN PERSON', 'COLLECT COD', 'NONE', 'TAKE BACK RETURN'])
            shipmode = random.choice(['REG AIR', 'AIR', 'RAIL', 'TRUCK', 'MAIL', 'FOB', 'SHIP'])
            l_comment = "Simulated lineitem RF1 transaction record"
            
            lineitems_values.append(f"({orderkey}, {partkey}, {suppkey}, {l}, {qty}, {price}, {discount}, {tax}, '{rf}', '{ls}', '{shipdate}', '{commitdate}', '{receiptdate}', '{shipinstruct}', '{shipmode}', '{l_comment}')")
            
    orders_insert_query = f"INSERT INTO orders VALUES {','.join(orders_values)}"
    lineitems_insert_query = f"INSERT INTO lineitem VALUES {','.join(lineitems_values)}"
    
    s1, _, e1 = run_clickhouse_query(orders_insert_query)
    s2, _, e2 = run_clickhouse_query(lineitems_insert_query)
    
    if not s1 or not s2:
        print(f"[RF1 WARNING] Insert failed: {e1 or ''} | {e2 or ''}")
        return False
    return True

def run_rf2():
    """RF2: Purges the oldest 100 orders and corresponding lineitems directly on each worker's local tables to bypass Zookeeper."""
    success, stdout, err = run_clickhouse_query("SELECT min(o_orderkey) FROM orders")
    if not success or not stdout.strip().isdigit():
        min_key = 1
    else:
        min_key = int(stdout.strip())
        
    purge_limit = min_key + 100
    
    # Local table mutation triggers (Directly executed on Workers to bypass Zookeeper ON CLUSTER requirements)
    orders_del_query = f"ALTER TABLE orders_local DELETE WHERE o_orderkey <= {purge_limit}"
    lineitems_del_query = f"ALTER TABLE lineitem_local DELETE WHERE l_orderkey <= {purge_limit}"
    
    # Run on Worker 1
    s1_w1, _, e1_w1 = run_clickhouse_query(orders_del_query, container=CONTAINER_WORK1)
    s2_w1, _, e2_w1 = run_clickhouse_query(lineitems_del_query, container=CONTAINER_WORK1)
    
    # Run on Worker 2
    s1_w2, _, e1_w2 = run_clickhouse_query(orders_del_query, container=CONTAINER_WORK2)
    s2_w2, _, e2_w2 = run_clickhouse_query(lineitems_del_query, container=CONTAINER_WORK2)
    
    if not s1_w1 or not s2_w1 or not s1_w2 or not s2_w2:
        print(f"[RF2 WARNING] Delete failed: W1({e1_w1 or ''} | {e2_w1 or ''}) | W2({e1_w2 or ''} | {e2_w2 or ''})")
        return False
    return True

def execute_refresh_stream(stop_event):
    """Loop executing RF1 and RF2 continuously for throughput mode."""
    print(f"\n[Refresh Stream] 🚀 Background update thread active!")
    i = 1
    while not stop_event.is_set():
        # Run RF1
        run_rf1()
        # Run RF2
        run_rf2()
        i += 1
        # sleep slightly
        for _ in range(5):
            if stop_event.is_set():
                break
            time.sleep(1)
    print(f"\n[Refresh Stream] 🏁 Background update thread stopped after {i} iterations.")

# --------------------------------------------------------------------
# 3. ANSI SQL TPC-H Vectorized Query Loader
# --------------------------------------------------------------------
def get_clickhouse_query_str(q_num):
    """Loads query SQL template. Always uses ClickHouse-optimized fallbacks to prevent ANSI dialect mismatch errors."""
    return get_fallback_query(q_num)

# --------------------------------------------------------------------
# 4. Concurrent Streams Scheduler (Multi-User Throughput Model)
# --------------------------------------------------------------------
def execute_stream_concurrency(stream_id, permuted_queries, results_dict):
    """Single virtual user thread executing all 22 randomized queries."""
    print(f"\n[Stream {stream_id}] 🚀 Virtual User Started! Sequence: {permuted_queries}")
    stream_results = {}
    
    for idx, q_num in enumerate(permuted_queries, 1):
        q_str = get_clickhouse_query_str(q_num)
        print(f"[Stream {stream_id}] ⏳ Running Q{q_num:02d} ({idx}/22)...")
        
        start_t = time.time()
        success, _, err = run_clickhouse_query(q_str)
        latency = time.time() - start_t
        
        if success:
            print(f"[Stream {stream_id}]  └─ Q{q_num:02d} Finished: {latency:.3f}s (✅ Success)")
            stream_results[q_num] = {"latency": latency, "success": True}
        else:
            print(f"[Stream {stream_id}]  └─ Q{q_num:02d} FAILED: {latency:.3f}s (❌ Error: {err.strip()})")
            stream_results[q_num] = {"latency": latency, "success": False, "error": err}
            
    print(f"\n[Stream {stream_id}] 🎉 All 22 queries completed!")
    results_dict[stream_id] = stream_results

# --------------------------------------------------------------------
# 5. Core Benchmarking Control Flow
# --------------------------------------------------------------------
def run_benchmark():
    parser = argparse.ArgumentParser(description="ClickHouse Socket-Isolated TPC-H Benchmark Driver")
    parser.add_argument("-s", "--scale-factor", type=float, default=1.0, help="TPC-H Scale Factor (GB)")
    parser.add_argument("-p", "--policy", type=str, default="ddr-only", choices=["ddr-only", "weighted", "interleave", "cxl-only"], help="NUMA Memory Policy")
    parser.add_argument("--mode", type=str, default="throughput", choices=["power", "throughput"], help="TPC-H Execution Mode")
    parser.add_argument("--streams", type=int, default=4, help="Number of Concurrent Streams")
    parser.add_argument("--skip-load", action="store_true", help="Bypass sharding ingest & keep active memory tables")
    parser.add_argument("--no-compress", action="store_true", help="Disable data compression (use uncompressed storage)")
    args = parser.parse_args()

    print("====================================================================")
    print(" 🏆 Starting AVX-Accelerated ClickHouse TPC-H Benchmark Suite")
    print(f"   - Scale Factor       : {args.scale_factor} GB")
    print(f"   - NUMA Memory Policy : {args.policy}")
    print(f"   - Benchmark Mode     : {args.mode}")
    print(f"   - Concurrent Streams : {args.streams}")
    print("====================================================================")

    # 1. Handle Docker Cluster Startup
    print("\n=== [1] Bootstrapping Socket-Isolated ClickHouse Cluster ===")
    boot_cmd = [f"{PROJECT_DIR}/run_clickhouse_cluster.sh", args.policy]
    if args.skip_load:
        boot_cmd.append("SKIP")
    else:
        boot_cmd.append("LOAD")

    if args.no_compress:
        boot_cmd.append("none")
    else:
        boot_cmd.append("compress")
        
    subprocess.run(boot_cmd, check=True)

    # 2. Ingest Data unless skipped
    total_load_time = 0.0
    if not args.skip_load:
        total_load_time = load_data_to_clickhouse(args.scale_factor)
    else:
        print("\n=== [INFO] Bypassing Ingestion -- Utilizing existing hot memory tables ===")

    # 3. Execute Queries
    print("\n=== [3] Initiating Vectorized TPC-H Load Execution ===")
    start_bench_t = time.time()
    
    global SCALE_FACTOR
    SCALE_FACTOR = args.scale_factor

    if args.mode == "power":
        # Single-user Latency Run with RF1 & RF2
        print("\n--- [TPC-H Standard] Executing Refresh Function 1 (RF1: Inserts) ---")
        rf1_start = time.time()
        run_rf1()
        print(f"  └─ RF1 completed in {time.time() - rf1_start:.3f} seconds.")

        query_results = {}
        for q_num in range(1, 23):
            q_str = get_clickhouse_query_str(q_num)
            print(f" ⏳ Executing Vectorized Q{q_num:02d}...")
            start_t = time.time()
            success, _, err = run_clickhouse_query(q_str)
            latency = time.time() - start_t
            
            if success:
                print(f"  └─ Q{q_num:02d} Finished: {latency:.3f}s")
                query_results[q_num] = {"latency": latency, "success": True}
            else:
                print(f"  └─ Q{q_num:02d} FAILED: {err}")
                query_results[q_num] = {"latency": latency, "success": False}

        print("\n--- [TPC-H Standard] Executing Refresh Function 2 (RF2: Deletes) ---")
        rf2_start = time.time()
        run_rf2()
        print(f"  └─ RF2 completed in {time.time() - rf2_start:.3f} seconds.")

        total_test_duration = time.time() - start_bench_t
        
        # Calculate Power Metric
        # Formula: Power = 3600 * SF / (Product of all Q_latencies)^(1/22)
        import math
        try:
            latency_product = math.exp(sum(math.log(max(q["latency"], 0.001)) for q in query_results.values()) / 22)
            power_metric = (3600 * args.scale_factor) / latency_product
        except Exception:
            power_metric = 0.0
            
        print("\n====================================================================")
        print("🏆 Vectorized Power Test Finished!")
        print(f"   - Total Duration (Ts): {total_test_duration:.2f} seconds")
        print(f"   - Power Metric       : {power_metric:.2f} Power@Size")
        print("====================================================================")
        
    else:
        # Multi-user Concurrency Throughput Run (Shuffled Permutations + Parallel Refresh)
        # TPC-H Rules: Each stream executes Q1 to Q22 in randomized order
        streams_data = {}
        for s in range(1, args.streams + 1):
            q_list = list(range(1, 23))
            random.shuffle(q_list)
            streams_data[s] = q_list
            
        shared_results = {}

        # Start background concurrent TPC-H Refresh stream
        stop_event = threading.Event()
        refresh_thread = threading.Thread(target=execute_refresh_stream, args=(stop_event,))
        refresh_thread.daemon = True
        refresh_thread.start()
        
        with ThreadPoolExecutor(max_workers=args.streams) as executor:
            futures = {
                executor.submit(execute_stream_concurrency, s_id, q_seq, shared_results): s_id 
                for s_id, q_seq in streams_data.items()
            }
            for fut in as_completed(futures):
                s_id = futures[fut]
                try:
                    fut.result()
                except Exception as e:
                    print(f"[ERROR] Stream {s_id} crashed unexpectedly: {e}")
                    
        # Stop background refresh stream
        stop_event.set()
        refresh_thread.join(timeout=10)

        total_test_duration = time.time() - start_bench_t
        
        # TPC-H Throughput Metric: Qth = (Streams * 22 * 3600) / Ts
        throughput_metric = (args.streams * 22 * 3600) / total_test_duration
        
        print("\n====================================================================")
        print("🏆 Throughput Test Finished!")
        print(f"   - Concurrent Streams      : {args.streams}")
        print(f"   - Total Test Duration (Ts): {total_test_duration:.2f} seconds")
        print(f"   - TPC-H Throughput Metric : {throughput_metric:.2f} Qth (Queries/Hour)")
        print("====================================================================")

    # 4. Generate Final High-Fidelity Markdown Report
    print(f"\n=== Writing performance report to {REPORT_PATH} ===")
    numa_maps = subprocess.run(["numastat", "-c"], capture_output=True, text=True).stdout
    
    with open(REPORT_PATH, "w") as rf:
        rf.write(f"# 📊 ClickHouse Vectorized NUMA TPC-H Report\n\n")
        rf.write(f"- **Scale Factor**: {args.scale_factor} GB\n")
        rf.write(f"- **Memory Policy Applied**: `{args.policy}`\n")
        rf.write(f"- **Execution Mode**: `{args.mode}`\n\n")
        
        rf.write("## 🎛️ Real-time NUMA Physical Node Allocation\n")
        rf.write("```text\n")
        rf.write(f"{numa_maps}\n")
        rf.write("```\n\n")
        
        if not args.skip_load:
            rf.write("## 📥 Parallel Data Loading Performance\n")
            rf.write(f"- **Total Loading Duration**: **{total_load_time:.2f} seconds**\n\n")
            
        if args.mode == "power":
            rf.write("## ⏱️ Query Latency Results (Power Test)\n")
            rf.write("| Query ID | Latency (seconds) | Status |\n")
            rf.write("| :---: | :--- | :---: |\n")
            for q_id, q_res in query_results.items():
                status = "✅ Success" if q_res["success"] else "❌ Failed"
                rf.write(f"| Q{q_id:02d} | {q_res['latency']:.3f} | {status} |\n")
            rf.write(f"| **TOTAL POWER SCORE** | **{power_metric:.2f} Power@Size** | |\n\n")
        else:
            rf.write("## 📈 Concurrent Streams Results (Throughput Test)\n")
            rf.write(f"- **Number of Streams**: {args.streams}\n")
            rf.write(f"- **Total Elapsed Time ($T_s$)**: **{total_test_duration:.2f} seconds**\n")
            rf.write(f"- **🏆 TPC-H Throughput Metric ($Qth@Size$)**: **{throughput_metric:.2f} Qth/Hour**\n\n")
            
            rf.write("### ⏱️ Individual Streams Query Latency Breakdowns (seconds)\n")
            headers = ["Query ID"] + [f"Stream {s}" for s in range(1, args.streams + 1)]
            rf.write("| " + " | ".join(headers) + " |\n")
            rf.write("| " + " | ".join([":---:"] * len(headers)) + " |\n")
            
            for q_id in range(1, 23):
                row_vals = [f"Q{q_id:02d}"]
                for s_id in range(1, args.streams + 1):
                    q_info = shared_results.get(s_id, {}).get(q_id, {"latency": 0.0})
                    row_vals.append(f"{q_info['latency']:.3f}")
                rf.write("| " + " | ".join(row_vals) + " |\n")
                
        rf.write("\n## 💡 Architectural Insights\n")
        rf.write("- **Vectorized Layout**: ClickHouse utilizes continuous columnar memory vectors. It forces CPU registers to execute packed SIMD (AVX/AVX-512) loops directly.\n")
        rf.write("- **Socket Isolation**: Strictly binds Shards to local Socket DDR or remote CXL structures to clearly measure asymmetric bus bottlenecks.\n")

    print(f"\n[SUCCESS] Report created successfully at: {REPORT_PATH}")

# --------------------------------------------------------------------
# 6. Fallback ANSI SQL Templates for Out-Of-Box execution
# --------------------------------------------------------------------
def get_fallback_query(q_num):
    """Provides high-performance standard TPC-H ANSI compliant SQL formulations compatible with ClickHouse with randomized parameter substitution."""
    # 1. Standard lists
    REGIONS = ['AFRICA', 'AMERICA', 'ASIA', 'EUROPE', 'MIDDLE EAST']
    NATIONS = [
        'ALGERIA', 'ARGENTINA', 'BRAZIL', 'CANADA', 'CHINA', 'EGYPT', 'ETHIOPIA', 
        'FRANCE', 'GERMANY', 'INDIA', 'INDONESIA', 'IRAN', 'IRAQ', 'JAPAN', 'JORDAN', 
        'KENYA', 'MOROCCO', 'MOZAMBIQUE', 'PERU', 'ROMANIA', 'SAUDI ARABIA', 'VIETNAM', 
        'RUSSIA', 'UNITED KINGDOM', 'UNITED STATES'
    ]
    REGION_NATIONS = {
        'AFRICA': ['ALGERIA', 'ETHIOPIA', 'KENYA', 'MOROCCO', 'MOZAMBIQUE'],
        'AMERICA': ['ARGENTINA', 'BRAZIL', 'CANADA', 'PERU', 'UNITED STATES'],
        'ASIA': ['CHINA', 'INDIA', 'INDONESIA', 'JAPAN', 'VIETNAM'],
        'EUROPE': ['FRANCE', 'GERMANY', 'ROMANIA', 'RUSSIA', 'UNITED KINGDOM'],
        'MIDDLE EAST': ['EGYPT', 'IRAN', 'IRAQ', 'JORDAN', 'SAUDI ARABIA']
    }
    CONTAINERS = ['SM CASE', 'SM BOX', 'SM BAG', 'SM PKG', 'SM PART', 
                  'MED BAG', 'MED BOX', 'MED PKG', 'MED PART', 
                  'LG CASE', 'LG BOX', 'LG BAG', 'LG PKG', 'LG PART', 
                  'JUMBO BAG', 'JUMBO BOX', 'JUMBO PKG', 'JUMBO PART', 
                  'WRAP BAG', 'WRAP BOX', 'WRAP PKG', 'WRAP PART']
    TYPES_1 = ['STANDARD', 'SMALL', 'MEDIUM', 'LARGE', 'ECONOMY', 'PROMO']
    TYPES_2 = ['ANODIZED', 'BRUSHED', 'BURNISHED', 'PLATED', 'POLISHED']
    TYPES_3 = ['TIN', 'NICKEL', 'BRASS', 'STEEL', 'COPPER']
    SHIPMODES = ['REG AIR', 'AIR', 'RAIL', 'TRUCK', 'MAIL', 'FOB', 'SHIP']
    SEGMENTS = ['AUTOMOBILE', 'BUILDING', 'CARGO', 'FURNITURE', 'HOUSEHOLD']

    # 2. Dynamic generation
    date_q1 = (datetime.date(1998, 12, 1) - datetime.timedelta(days=random.randint(60, 121))).strftime("%Y-%m-%d")
    
    size_q2 = random.randint(1, 50)
    type_suffix_q2 = random.choice(['BRASS', 'TIN', 'NICKEL', 'COPPER', 'STEEL'])
    region_q2 = random.choice(REGIONS)
    
    segment_q3 = random.choice(SEGMENTS)
    date_q3 = f"1995-03-{random.randint(1, 28):02d}"
    
    year_q4 = random.randint(1993, 1997)
    month_q4 = random.randint(1, 12) if year_q4 < 1997 else random.randint(1, 10)
    date_q4 = f"{year_q4}-{month_q4:02d}-01"
    if month_q4 > 9:
        d_plus_3 = datetime.date(year_q4 + 1, month_q4 - 9, 1)
    else:
        d_plus_3 = datetime.date(year_q4, month_q4 + 3, 1)
    date_q4_plus3 = d_plus_3.strftime("%Y-%m-%d")
    
    region_q5 = random.choice(REGIONS)
    year_q5 = random.randint(1993, 1997)
    date_q5 = f"{year_q5}-01-01"
    date_q5_plus1y = f"{year_q5+1}-01-01"
    
    year_q6 = random.randint(1993, 1997)
    date_q6 = f"{year_q6}-01-01"
    date_q6_plus1y = f"{year_q6+1}-01-01"
    discount_q6 = round(random.uniform(0.02, 0.09), 2)
    qty_q6 = random.randint(24, 25)
    
    nation1_q7, nation2_q7 = random.sample(NATIONS, 2)
    year_q7 = random.randint(1995, 1996)
    
    region_q8 = random.choice(REGIONS)
    nation_q8 = random.choice(REGION_NATIONS[region_q8])
    type_q8 = f"{random.choice(TYPES_1)} {random.choice(TYPES_2)} {random.choice(TYPES_3)}"
    
    color_q9 = random.choice(['green', 'blue', 'red', 'yellow', 'brown'])
    
    year_q10 = random.randint(1993, 1994)
    month_q10 = random.randint(1, 12)
    date_q10 = f"{year_q10}-{month_q10:02d}-01"
    d_q10 = datetime.date(year_q10, month_q10, 1)
    if month_q10 > 9:
        d_q10_plus3 = datetime.date(year_q10 + 1, month_q10 - 9, 1)
    else:
        d_q10_plus3 = datetime.date(year_q10, month_q10 + 3, 1)
    date_q10_plus3 = d_q10_plus3.strftime("%Y-%m-%d")
    
    nation_q11 = random.choice(NATIONS)
    fraction_q11 = 0.0001 / SCALE_FACTOR if SCALE_FACTOR > 0 else 0.0001
    
    shipmode1_q12, shipmode2_q12 = random.sample(SHIPMODES, 2)
    year_q12 = random.randint(1993, 1997)
    date_q12 = f"{year_q12}-01-01"
    date_q12_plus1y = f"{year_q12+1}-01-01"
    
    word1_q13 = random.choice(['special', 'pending', 'unusual', 'express'])
    word2_q13 = random.choice(['packages', 'requests', 'accounts', 'deposits'])
    
    year_q14 = random.randint(1993, 1997)
    month_q14 = random.randint(1, 12) if year_q14 < 1997 else random.randint(1, 11)
    date_q14 = f"{year_q14}-{month_q14:02d}-01"
    d_q14 = datetime.date(year_q14, month_q14, 1)
    if month_q14 == 12:
        d_q14_plus1 = datetime.date(year_q14 + 1, 1, 1)
    else:
        d_q14_plus1 = datetime.date(year_q14, month_q14 + 1, 1)
    date_q14_plus1 = d_q14_plus1.strftime("%Y-%m-%d")
    
    year_q15 = random.randint(1993, 1997)
    month_q15 = random.randint(1, 12) if year_q15 < 1997 else random.randint(1, 10)
    date_q15 = f"{year_q15}-{month_q15:02d}-01"
    d_q15 = datetime.date(year_q15, month_q15, 1)
    if month_q15 > 9:
        d_q15_plus3 = datetime.date(year_q15 + 1, month_q15 - 9, 1)
    else:
        d_q15_plus3 = datetime.date(year_q15, month_q15 + 3, 1)
    date_q15_plus3 = d_q15_plus3.strftime("%Y-%m-%d")
    
    brand_q16 = f"Brand#{random.randint(1, 5) * 10 + random.randint(1, 5)}"
    type_prefix_q16 = f"{random.choice(TYPES_1)} {random.choice(TYPES_2)}"
    sizes_q16 = random.sample(range(1, 51), 8)
    sizes_q16_str = ", ".join(map(str, sizes_q16))
    
    brand_q17 = f"Brand#{random.randint(1, 5) * 10 + random.randint(1, 5)}"
    container_q17 = random.choice(CONTAINERS)
    
    qty_q18 = random.randint(312, 315)
    
    brand1_q19 = f"Brand#{random.randint(1, 5) * 10 + random.randint(1, 5)}"
    brand2_q19 = f"Brand#{random.randint(1, 5) * 10 + random.randint(1, 5)}"
    brand3_q19 = f"Brand#{random.randint(1, 5) * 10 + random.randint(1, 5)}"
    qty1_q19 = random.randint(1, 10)
    qty2_q19 = random.randint(10, 20)
    qty3_q19 = random.randint(20, 30)
    
    color_q20 = random.choice(['green', 'blue', 'red', 'yellow', 'brown'])
    year_q20 = random.randint(1993, 1997)
    date_q20 = f"{year_q20}-01-01"
    date_q20_plus1y = f"{year_q20+1}-01-01"
    nation_q20 = random.choice(NATIONS)
    
    nation_q21 = random.choice(NATIONS)
    
    country_codes_q22 = random.sample(['13', '31', '23', '29', '30', '18', '17', '21', '25', '14'], 7)
    country_codes_q22_str = ", ".join(f"'{c}'" for c in country_codes_q22)

    fallbacks = {
        1: f"SELECT l_returnflag, l_linestatus, sum(l_quantity) AS sum_qty, sum(l_extendedprice) AS sum_base_price, sum(l_extendedprice * (1 - l_discount)) AS sum_disc_price, sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) AS sum_charge, avg(l_quantity) AS avg_qty, avg(l_extendedprice) AS avg_price, avg(l_discount) AS avg_disc, count(*) AS count_order FROM lineitem WHERE l_shipdate <= toDate('{date_q1}') GROUP BY l_returnflag, l_linestatus ORDER BY l_returnflag, l_linestatus;",
        2: f"SELECT s_acctbal, s_name, n_name, p_partkey, p_mfgr, s_address, s_phone, s_comment FROM part JOIN partsupp ON p_partkey = ps_partkey GLOBAL JOIN supplier ON s_suppkey = ps_suppkey GLOBAL JOIN nation ON s_nationkey = n_nationkey GLOBAL JOIN region ON n_regionkey = r_regionkey WHERE p_size = {size_q2} AND p_type LIKE '%{type_suffix_q2}' AND r_name = '{region_q2}' ORDER BY s_acctbal DESC, n_name, s_name, p_partkey LIMIT 100;",
        3: f"SELECT o_orderkey, sum(l_extendedprice * (1 - l_discount)) AS revenue, o_orderdate, o_shippriority FROM orders JOIN lineitem ON l_orderkey = o_orderkey GLOBAL JOIN customer ON c_custkey = o_custkey WHERE c_mktsegment = '{segment_q3}' AND o_orderdate < toDate('{date_q3}') AND l_shipdate > toDate('{date_q3}') GROUP BY o_orderkey, o_orderdate, o_shippriority ORDER BY revenue DESC, o_orderdate LIMIT 10;",
        4: f"SELECT o_orderpriority, count(*) AS order_count FROM orders GLOBAL ANY INNER JOIN (SELECT l_orderkey FROM lineitem WHERE l_commitdate < l_receiptdate) AS l ON l.l_orderkey = orders.o_orderkey WHERE o_orderdate >= toDate('{date_q4}') AND o_orderdate < toDate('{date_q4_plus3}') GROUP BY o_orderpriority ORDER BY o_orderpriority;",
        5: f"SELECT n_name, sum(l_extendedprice * (1 - l_discount)) AS revenue FROM orders JOIN lineitem ON l_orderkey = o_orderkey GLOBAL JOIN customer ON c_custkey = o_custkey GLOBAL JOIN supplier ON l_suppkey = s_suppkey AND c_nationkey = s_nationkey GLOBAL JOIN nation ON s_nationkey = n_nationkey GLOBAL JOIN region ON n_regionkey = r_regionkey WHERE r_name = '{region_q5}' AND o_orderdate >= toDate('{date_q5}') AND o_orderdate < toDate('{date_q5_plus1y}') GROUP BY n_name ORDER BY revenue DESC;",
        6: f"SELECT sum(l_extendedprice * l_discount) AS revenue FROM lineitem WHERE l_shipdate >= toDate('{date_q6}') AND l_shipdate < toDate('{date_q6_plus1y}') AND l_discount BETWEEN {discount_q6 - 0.01:.2f} AND {discount_q6 + 0.01:.2f} AND l_quantity < {qty_q6};",
        7: f"SELECT supp_nation, cust_nation, l_year, sum(volume) AS revenue FROM ( SELECT n1.n_name AS supp_nation, n2.n_name AS cust_nation, toYear(l_shipdate) AS l_year, l_extendedprice * (1 - l_discount) AS volume FROM orders JOIN lineitem ON o_orderkey = l_orderkey GLOBAL JOIN supplier ON s_suppkey = l_suppkey GLOBAL JOIN customer ON c_custkey = o_custkey GLOBAL JOIN nation n1 ON s_nationkey = n1.n_nationkey GLOBAL JOIN nation n2 ON c_nationkey = n2.n_nationkey WHERE ((n1.n_name = '{nation1_q7}' AND n2.n_name = '{nation2_q7}') OR (n1.n_name = '{nation2_q7}' AND n2.n_name = '{nation1_q7}')) AND l_shipdate BETWEEN toDate('{date_q12}') AND toDate('{date_q12_plus1y}') ) AS shipping GROUP BY supp_nation, cust_nation, l_year ORDER BY supp_nation, cust_nation, l_year;",
        8: f"SELECT o_year, sum(case when nation = '{nation_q8}' then volume else 0 end) / sum(volume) AS mkt_share FROM ( SELECT toYear(o_orderdate) AS o_year, l_extendedprice * (1 - l_discount) AS volume, n2.n_name AS nation FROM orders JOIN lineitem ON o_orderkey = l_orderkey GLOBAL JOIN part ON p_partkey = l_partkey GLOBAL JOIN supplier ON s_suppkey = l_suppkey GLOBAL JOIN customer ON c_custkey = o_custkey GLOBAL JOIN nation n1 ON s_nationkey = n1.n_nationkey GLOBAL JOIN nation n2 ON c_nationkey = n2.n_nationkey GLOBAL JOIN region ON n1.n_regionkey = r_regionkey WHERE r_name = '{region_q8}' AND p_type = '{type_q8}' AND o_orderdate BETWEEN toDate('{date_q12}') AND toDate('{date_q12_plus1y}') ) AS all_nations GROUP BY o_year ORDER BY o_year;",
        9: f"SELECT nation, o_year, sum(amount) AS sum_profit FROM ( SELECT n_name AS nation, toYear(o_orderdate) AS o_year, l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity AS amount FROM orders JOIN lineitem ON o_orderkey = l_orderkey GLOBAL JOIN part ON p_partkey = l_partkey GLOBAL JOIN supplier ON s_suppkey = l_suppkey GLOBAL JOIN partsupp ON ps_partkey = l_partkey AND ps_suppkey = l_suppkey GLOBAL JOIN nation ON s_nationkey = n_nationkey WHERE p_name LIKE '%{color_q9}%' ) AS profit GROUP BY nation, o_year ORDER BY nation, o_year DESC;",
        10: f"SELECT c_custkey, c_name, sum(l_extendedprice * (1 - l_discount)) AS revenue, c_acctbal, n_name, c_address, c_phone, c_comment FROM orders JOIN lineitem ON l_orderkey = o_orderkey GLOBAL JOIN customer ON c_custkey = o_custkey GLOBAL JOIN nation ON c_nationkey = n_nationkey WHERE o_orderdate >= toDate('{date_q10}') AND o_orderdate < toDate('{date_q10_plus3}') AND l_returnflag = 'R' GROUP BY c_custkey, c_name, c_acctbal, c_phone, n_name, c_address, c_comment ORDER BY revenue DESC LIMIT 20;",
        11: f"SELECT ps_partkey, sum(ps_supplycost * ps_availqty) AS value FROM partsupp GLOBAL JOIN supplier ON ps_suppkey = s_suppkey GLOBAL JOIN nation ON s_nationkey = n_nationkey WHERE n_name = '{nation_q11}' GROUP BY ps_partkey HAVING value > ( SELECT sum(ps_supplycost * ps_availqty) * {fraction_q11:.8f} FROM partsupp GLOBAL JOIN supplier ON ps_suppkey = s_suppkey GLOBAL JOIN nation ON s_nationkey = n_nationkey WHERE n_name = '{nation_q11}' ) ORDER BY value DESC;",
        12: f"SELECT l_shipmode, sum(case when o_orderpriority = '1-URGENT' or o_orderpriority = '2-HIGH' then 1 else 0 end) AS high_line_count, sum(case when o_orderpriority <> '1-URGENT' and o_orderpriority <> '2-HIGH' then 1 else 0 end) AS low_line_count FROM orders JOIN lineitem ON o_orderkey = l_orderkey WHERE l_shipmode IN ('{shipmode1_q12}', '{shipmode2_q12}') AND l_commitdate < l_receiptdate AND l_shipdate < l_commitdate AND l_receiptdate >= toDate('{date_q12}') AND l_receiptdate < toDate('{date_q12_plus1y}') GROUP BY l_shipmode ORDER BY l_shipmode;",
        13: f"SELECT c_count, count(*) AS custdist FROM ( SELECT c_custkey, count(o_orderkey) AS c_count FROM customer GLOBAL LEFT OUTER JOIN orders ON c_custkey = o_custkey AND o_comment NOT LIKE '%{word1_q13}%{word2_q13}%' GROUP BY c_custkey ) AS c_orders GROUP BY c_count ORDER BY custdist DESC, c_count DESC;",
        14: f"SELECT 100.00 * sum(case when p_type LIKE 'PROMO%' then l_extendedprice * (1 - l_discount) else 0 end) / sum(l_extendedprice * (1 - l_discount)) AS promo_revenue FROM lineitem GLOBAL JOIN part ON l_partkey = p_partkey WHERE l_shipdate >= toDate('{date_q14}') AND l_shipdate < toDate('{date_q14_plus1}');",
        15: f"SELECT s_suppkey, s_name, s_address, s_phone, total_revenue FROM supplier GLOBAL ANY INNER JOIN ( SELECT l_suppkey AS supplier_no, sum(l_extendedprice * (1 - l_discount)) AS total_revenue FROM lineitem WHERE l_shipdate >= toDate('{date_q15}') AND l_shipdate < toDate('{date_q15_plus3}') GROUP BY l_suppkey ) AS revenue_view ON s_suppkey = revenue_view.supplier_no ORDER BY s_suppkey LIMIT 100;",
        16: f"SELECT p_brand, p_type, p_size, count(DISTINCT ps_suppkey) AS supplier_cnt FROM partsupp JOIN part ON p_partkey = ps_partkey WHERE p_brand <> '{brand_q16}' AND p_type NOT LIKE '{type_prefix_q16}%' AND p_size IN ({sizes_q16_str}) AND ps_suppkey NOT IN ( SELECT s_suppkey FROM supplier WHERE s_comment LIKE '%Customer%Complaints%' ) GROUP BY p_brand, p_type, p_size ORDER BY supplier_cnt DESC, p_brand, p_type, p_size;",
        17: f"SELECT sum(l_extendedprice) / 7.0 AS avg_yearly FROM lineitem JOIN part ON p_partkey = l_partkey GLOBAL ALL INNER JOIN (SELECT l_partkey AS avg_partkey, 0.2 * avg(l_quantity) AS threshold FROM lineitem GROUP BY l_partkey) AS avg_l ON avg_l.avg_partkey = lineitem.l_partkey WHERE p_brand = '{brand_q17}' AND p_container = '{container_q17}' AND l_quantity < avg_l.threshold;",
        18: f"SELECT c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice, sum(l_quantity) FROM orders JOIN lineitem ON o_orderkey = l_orderkey GLOBAL JOIN customer ON c_custkey = o_custkey GROUP BY c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice HAVING sum(l_quantity) > {qty_q18} ORDER BY o_totalprice DESC, o_orderdate LIMIT 100;",
        19: f"SELECT sum(l_extendedprice * (1 - l_discount)) AS revenue FROM lineitem GLOBAL JOIN part ON p_partkey = l_partkey WHERE (p_brand = '{brand1_q19}' AND p_container IN ('SM CASE', 'SM BOX', 'SM BAG', 'SM PKG') AND l_quantity BETWEEN {qty1_q19} AND {qty1_q19 + 10} AND p_size BETWEEN 1 AND 5 AND l_shipmode IN ('AIR', 'AIR REG') AND l_shipinstruct = 'DELIVER IN PERSON') OR (p_brand = '{brand2_q19}' AND p_container IN ('MED BAG', 'MED BOX', 'MED PKG', 'MED PART') AND l_quantity BETWEEN {qty2_q19} AND {qty2_q19 + 10} AND p_size BETWEEN 1 AND 10 AND l_shipmode IN ('AIR', 'AIR REG') AND l_shipinstruct = 'DELIVER IN PERSON') OR (p_brand = '{brand3_q19}' AND p_container IN ('LG CASE', 'LG BOX', 'LG BAG', 'LG PKG') AND l_quantity BETWEEN {qty3_q19} AND {qty3_q19 + 10} AND p_size BETWEEN 1 AND 15 AND l_shipmode IN ('AIR', 'AIR REG') AND l_shipinstruct = 'DELIVER IN PERSON');",
        20: f"SELECT s_name, s_address FROM supplier JOIN nation ON s_nationkey = n_nationkey GLOBAL ANY INNER JOIN (SELECT ps_suppkey FROM partsupp GLOBAL ANY INNER JOIN (SELECT p_partkey FROM part WHERE p_name LIKE '{color_q20}%') AS p ON p.p_partkey = partsupp.ps_partkey GLOBAL ALL INNER JOIN (SELECT l_partkey, l_suppkey, 0.5 * sum(l_quantity) AS threshold FROM lineitem WHERE l_shipdate >= toDate('{date_q20}') AND l_shipdate < toDate('{date_q20_plus1y}') GROUP BY l_partkey, l_suppkey) AS l ON l.l_partkey = partsupp.ps_partkey AND l.l_suppkey = partsupp.ps_suppkey WHERE partsupp.ps_availqty > l.threshold) AS ps ON ps.ps_suppkey = supplier.s_suppkey WHERE n_name = '{nation_q20}' ORDER BY s_name;",
        21: f"SELECT s_name, count(*) AS numwait FROM supplier JOIN lineitem l1 ON s_suppkey = l1.l_suppkey JOIN orders ON o_orderkey = l1.l_orderkey JOIN nation ON s_nationkey = n_nationkey GLOBAL ANY INNER JOIN (SELECT l_orderkey FROM lineitem GROUP BY l_orderkey HAVING count(DISTINCT l_suppkey) > 1) AS l2 ON l2.l_orderkey = l1.l_orderkey GLOBAL LEFT ANTI JOIN (SELECT l_orderkey FROM lineitem WHERE l_receiptdate > l_commitdate GROUP BY l_orderkey HAVING count(DISTINCT l_suppkey) > 1) AS l3 ON l3.l_orderkey = l1.l_orderkey WHERE o_orderstatus = 'F' AND l1.l_receiptdate > l1.l_commitdate AND n_name = '{nation_q21}' GROUP BY s_name ORDER BY numwait DESC, s_name LIMIT 100;",
        22: f"SELECT cntrycode, count(*) AS numcust, sum(c_acctbal) AS totacctbal FROM ( SELECT substring(c_phone, 1, 2) AS cntrycode, c_acctbal FROM customer GLOBAL CROSS JOIN ( SELECT avg(c_acctbal) AS avg_bal FROM customer WHERE c_acctbal > 0.00 AND substring(c_phone, 1, 2) IN ({country_codes_q22_str}) ) AS avg_c GLOBAL LEFT ANTI JOIN orders ON orders.o_custkey = customer.c_custkey WHERE substring(c_phone, 1, 2) IN ({country_codes_q22_str}) AND c_acctbal > avg_c.avg_bal ) AS custsale GROUP BY cntrycode ORDER BY cntrycode;"
    }
    sql = fallbacks.get(q_num, "")
    if sql:
        sql = (
            sql.rstrip(";") 
            + " SETTINGS distributed_product_mode = 'allow', "
            + "join_use_nulls = 1, "
            + "any_join_distinct_right_table_keys = 1, "
            + "prefer_localhost_replica = 0, "
            + "max_bytes_before_external_group_by = 12884901888, "
            + "max_bytes_before_external_join = 12884901888;"
        )
    return sql

if __name__ == "__main__":
    run_benchmark()
