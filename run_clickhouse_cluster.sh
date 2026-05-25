#!/usr/bin/env bash
# ====================================================================
#  ClickHouse Socket-Isolated Docker & NUMA Orchestrator (Named Volumes)
#  Strictly isolates Socket 0 (Worker 1) and Socket 1 (Worker 2)
#  Eliminates permission issues permanently using Docker Named Volumes.
# ====================================================================

set -e

# Get the absolute directory path where this script resides dynamically
PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CONFIG_DIR="${PROJECT_DIR}/config"

# Allow STAGING_DIR to be overridden, default dynamically relative to user's home directory
STAGING_DIR="${STAGING_DIR:-$HOME/cxl_TPC/tpch-dbgen}"

IMAGE_NAME="clickhouse/clickhouse-server:latest"
CONTAINER_COORD="clickhouse_coordinator"
CONTAINER_WORKER1="clickhouse_worker1"
CONTAINER_WORKER2="clickhouse_worker2"

# Ensure config directory exists
mkdir -p "${CONFIG_DIR}"

# --------------------------------------------------------------------
# 1. Detect CPU Socket Layout (Same topology detection as Citus)
# --------------------------------------------------------------------
NUM_SOCKETS=$(lscpu | grep "Socket(s):" | awk '{print $2}')
CORES_PER_SOCKET=$(lscpu | grep "Core(s) per socket:" | awk '{print $4}')
THREADS_PER_CORE=$(lscpu | grep "Thread(s) per core:" | awk '{print $4}')

echo "=== [INFO] System Hardware Architecture Detected ==="
echo " - Total Sockets         : ${NUM_SOCKETS}"
echo " - Cores per Socket      : ${CORES_PER_SOCKET}"
echo " - Threads per Core      : ${THREADS_PER_CORE}"

# Safe Fallbacks for Single Socket development machines
if [ "${NUM_SOCKETS}" -eq 1 ]; then
    echo "[WARNING] Single socket system detected. Simulating Socket boundaries..."
    HALF_CORES=$((CORES_PER_SOCKET * THREADS_PER_CORE / 2))
    MAX_CORE_ID=$((CORES_PER_SOCKET * THREADS_PER_CORE - 1))
    SOCKET0_CPUS="0-$((HALF_CORES - 1))"
    SOCKET1_CPUS="${HALF_CORES}-${MAX_CORE_ID}"
else
    # Dual-socket production server (CloudCXL Server)
    # Socket 0: Even cores usually, Socket 1: Odd cores or strictly partitioned ranges.
    # We retrieve the actual CPU ranges for Node 0 and Node 1.
    SOCKET0_CPUS=$(lscpu -p=node,cpu | grep -E "^0," | cut -d',' -f2 | paste -sd, -)
    SOCKET1_CPUS=$(lscpu -p=node,cpu | grep -E "^1," | cut -d',' -f2 | paste -sd, -)
fi

echo " - Socket 0 CPU Binding   : ${SOCKET0_CPUS}"
echo " - Socket 1 CPU Binding   : ${SOCKET1_CPUS}"
echo "===================================================="

# --------------------------------------------------------------------
# 2. Parse NUMA Memory Policy Argument
# --------------------------------------------------------------------
POLICY="${1:-ddr-only}"
SKIP_LOAD="${2:-}"
COMPRESS_MODE="${3:-compress}"  # 'compress' (default, lz4) or 'none' (uncompressed)


# NUMA Nodes: Node 0 (DDR), Node 1 (DDR/CXL Socket 1), Node 2 (CXL Remote)
case "$POLICY" in
    "ddr-only")
        NUMA_FLAG_0="--cpunodebind=0 --membind=0"
        NUMA_FLAG_1="--cpunodebind=1 --membind=1"
        ;;
    "weighted")
        NUMA_FLAG_0="--cpunodebind=0 --weighted-interleave=all"
        NUMA_FLAG_1="--cpunodebind=1 --weighted-interleave=all"
        ;;
    "interleave")
        NUMA_FLAG_0="--cpunodebind=0 --interleave=all"
        NUMA_FLAG_1="--cpunodebind=1 --interleave=all"
        ;;
    "cxl-only")
        NUMA_FLAG_0="--cpunodebind=0 --membind=2"
        # Remote CXL node is node 2
        NUMA_FLAG_1="--cpunodebind=1 --membind=2"
        ;;
    *)
        echo "[ERROR] Unknown memory policy: ${POLICY}"
        echo "Usage: $0 [ddr-only|weighted|interleave|cxl-only]"
        exit 1
        ;;
esac

echo "[INFO] Orchestrator executing with Policy: ${POLICY}"

# --------------------------------------------------------------------
# 3. Generate ClickHouse Config XMLs dynamically
# --------------------------------------------------------------------
# A. Generate Cluster configuration file (remote_servers.xml)
cat <<EOF > "${CONFIG_DIR}/remote_servers.xml"
<clickhouse>
    <remote_servers>
        <tpc_cluster>
            <shard>
                <internal_replication>true</internal_replication>
                <replica>
                    <host>clickhouse_worker1</host>
                    <port>9000</port>
                </replica>
            </shard>
            <shard>
                <internal_replication>true</internal_replication>
                <replica>
                    <host>clickhouse_worker2</host>
                    <port>9000</port>
                </replica>
            </shard>
        </tpc_cluster>
    </remote_servers>
    <max_server_memory_usage>68719476736</max_server_memory_usage> <!-- 64GB system-wide -->
    <!-- Configure listen to allow remote cluster networking -->
    <listen_host>::</listen_host>
</clickhouse>
EOF

# B. Generate memory settings & cache-bypass overrides inside default profiles
cat <<EOF > "${CONFIG_DIR}/memory_tuning.xml"
<clickhouse>
    <profiles>
        <default>
            <max_memory_usage>34359738368</max_memory_usage> <!-- 32GB per query -->
            <max_threads>8</max_threads>
            <!-- 1. Disable ClickHouse engine level caches to force raw memory requests -->
            <use_uncompressed_cache>0</use_uncompressed_cache>
            <merge_tree_max_rows_to_use_cache>0</merge_tree_max_rows_to_use_cache>
            <merge_tree_max_bytes_to_use_cache>0</merge_tree_max_bytes_to_use_cache>
            <!-- 2. Elevate block size to bypass CPU L3 Cache (Force memory-bandwidth requests) -->
            <max_block_size>1048576</max_block_size>
        </default>
    </profiles>
</clickhouse>
EOF

# C. Generate Compression configuration (compression.xml)
if [ "$COMPRESS_MODE" = "none" ] || [ "$COMPRESS_MODE" = "uncompressed" ]; then
    echo "[INFO] COMPRESSION IS DISABLED (method: none)"
    COMPRESS_METHOD="none"
else
    echo "[INFO] COMPRESSION IS ENABLED (method: lz4)"
    COMPRESS_METHOD="lz4"
fi

cat <<EOF > "${CONFIG_DIR}/compression.xml"
<clickhouse>
    <compression>
        <case>
            <method>${COMPRESS_METHOD}</method>
        </case>
    </compression>
</clickhouse>
EOF


# --------------------------------------------------------------------
# 4. Clean up any existing ClickHouse docker resources
# --------------------------------------------------------------------
echo "[INFO] Releasing running ClickHouse Docker cluster containers..."
docker rm -f "$CONTAINER_COORD" "$CONTAINER_WORKER1" "$CONTAINER_WORKER2" >/dev/null 2>&1 || true

# Recreate Docker Named Volumes to wipe old state and align with clickhouse daemon permissions
if [ "$SKIP_LOAD" != "SKIP" ]; then
    echo "[INFO] Re-initializing Docker Named Volumes..."
    docker volume rm ch_coord_data ch_worker1_data ch_worker2_data >/dev/null 2>&1 || true
    docker volume create ch_coord_data >/dev/null 2>&1 || true
    docker volume create ch_worker1_data >/dev/null 2>&1 || true
    docker volume create ch_worker2_data >/dev/null 2>&1 || true
else
    echo "[INFO] SKIP_LOAD active. Preserving existing Docker Named Volumes..."
fi

# Ensure clean docker network exists
docker network create clickhouse-net >/dev/null 2>&1 || true

# --------------------------------------------------------------------
# 5. Launch ClickHouse Coordinator (Runs strictly on Socket 0 DDR)
# --------------------------------------------------------------------
echo "[INFO] Spawning ClickHouse Coordinator (Host Port: 8123)..."
docker run -d \
    --name "$CONTAINER_COORD" \
    --network clickhouse-net \
    --cpuset-cpus="$SOCKET0_CPUS" \
    --cpuset-mems="0" \
    -p 8123:8123 \
    -p 9000:9000 \
    -v "${CONFIG_DIR}/remote_servers.xml:/etc/clickhouse-server/config.d/remote_servers.xml" \
    -v "${CONFIG_DIR}/memory_tuning.xml:/etc/clickhouse-server/users.d/memory_tuning.xml" \
    -v "${CONFIG_DIR}/compression.xml:/etc/clickhouse-server/config.d/compression.xml" \
    -v "${CONFIG_DIR}/user_files_path.xml:/etc/clickhouse-server/config.d/user_files_path.xml" \
    -v "${STAGING_DIR}:/staging:rw" \
    -v ch_coord_data:/var/lib/clickhouse \
    "$IMAGE_NAME"

# --------------------------------------------------------------------
# 6. Launch Worker 1 (Socket 0 - Strict DDR Bound)
# --------------------------------------------------------------------
echo "[INFO] Spawning Worker 1 (Host Port: 8124, CPU: Socket 0, Memory: ${NUMA_FLAG_0})..."
docker run -d \
    --name "$CONTAINER_WORKER1" \
    --network clickhouse-net \
    --privileged \
    --user clickhouse \
    -p 8124:8123 \
    -p 9001:9000 \
    -v /usr/local/bin/numactl:/usr/local/bin/numactl \
    -v /usr/local/lib/libnuma.so.1:/usr/lib/x86_64-linux-gnu/libnuma.so.1 \
    -v "${CONFIG_DIR}/remote_servers.xml:/etc/clickhouse-server/config.d/remote_servers.xml" \
    -v "${CONFIG_DIR}/memory_tuning.xml:/etc/clickhouse-server/users.d/memory_tuning.xml" \
    -v "${CONFIG_DIR}/compression.xml:/etc/clickhouse-server/config.d/compression.xml" \
    -v "${CONFIG_DIR}/user_files_path.xml:/etc/clickhouse-server/config.d/user_files_path.xml" \
    -v "${STAGING_DIR}:/staging:rw" \
    -v ch_worker1_data:/var/lib/clickhouse \
    --entrypoint /usr/local/bin/numactl \
    "$IMAGE_NAME" \
    --cpunodebind=0 \
    $NUMA_FLAG_0 \
    clickhouse-server --config-file=/etc/clickhouse-server/config.xml

# --------------------------------------------------------------------
# 7. Launch Worker 2 (Socket 1 - Dynamic Memory bound by Policy)
# --------------------------------------------------------------------
echo "[INFO] Spawning Worker 2 (Host Port: 8125, CPU: Socket 1, Memory: ${NUMA_FLAG_1})..."
docker run -d \
    --name "$CONTAINER_WORKER2" \
    --network clickhouse-net \
    --privileged \
    --user clickhouse \
    -p 8125:8123 \
    -p 9002:9000 \
    -v /usr/local/bin/numactl:/usr/local/bin/numactl \
    -v /usr/local/lib/libnuma.so.1:/usr/lib/x86_64-linux-gnu/libnuma.so.1 \
    -v "${CONFIG_DIR}/remote_servers.xml:/etc/clickhouse-server/config.d/remote_servers.xml" \
    -v "${CONFIG_DIR}/memory_tuning.xml:/etc/clickhouse-server/users.d/memory_tuning.xml" \
    -v "${CONFIG_DIR}/compression.xml:/etc/clickhouse-server/config.d/compression.xml" \
    -v "${CONFIG_DIR}/user_files_path.xml:/etc/clickhouse-server/config.d/user_files_path.xml" \
    -v "${STAGING_DIR}:/staging:rw" \
    -v ch_worker2_data:/var/lib/clickhouse \
    --entrypoint /usr/local/bin/numactl \
    "$IMAGE_NAME" \
    --cpunodebind=1 \
    $NUMA_FLAG_1 \
    clickhouse-server --config-file=/etc/clickhouse-server/config.xml

# --------------------------------------------------------------------
# 8. Wait for Cluster to be ready & verify connectivity
# --------------------------------------------------------------------
echo "[INFO] Waiting for ClickHouse nodes to warm up..."
for i in {1..20}; do
    if docker exec "$CONTAINER_COORD" clickhouse-client --query "SELECT 1" >/dev/null 2>&1; then
        echo "[SUCCESS] ClickHouse Coordinator is online and responding!"
        break
    fi
    if [ "$i" -eq 20 ]; then
        echo "[ERROR] Coordinator failed to respond within 20 seconds."
        exit 1
    fi
    sleep 1
done

echo "[INFO] Verifying Distributed Sharded tpc_cluster Topology..."
docker exec "$CONTAINER_COORD" clickhouse-client --query "SELECT * FROM system.clusters WHERE cluster='tpc_cluster' FORMAT PrettyCompact"

echo "===================================================================="
echo " 🎉 ClickHouse Socket-Isolated Cluster Ready on Policy: ${POLICY}!"
echo "===================================================================="
