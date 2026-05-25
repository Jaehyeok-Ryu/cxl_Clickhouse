# 📊 ClickHouse TPC-H Throughput 벤치마크 및 자원 확장 가이드

본 가이드는 `cxl_ClickHouse` 프로젝트 내에서 TPC-H **Throughput (처리량) 모드 실험을 수행하는 방법**과 각 컨테이너(Coordinator 및 Workers)의 **CPU, Memory 자원 사용량을 극대화하여 하드웨어 부하를 높이는(Scaling Up) 구체적인 튜닝 방안**을 정리합니다.

---

## 1. TPC-H Throughput Mode 실험 수행 방법

Throughput 모드는 여러 가상 사용자가 무작위 순서로 22개의 TPC-H 쿼리를 동시에 실행할 때의 **시간당 처리 쿼리 수($Qth@Size$)**를 측정합니다.

### 🚀 실험 실행 명령어 기본 패턴
```bash
# CXL 메모리 정책 하에 Scale Factor 30GB, 8개 동시 스트림으로 Throughput 테스트 수행
./benchmark.py --mode throughput --scale-factor 30.0 --streams 8 --policy cxl-only
```

### 💡 주요 파라미터 설명
*   `--mode throughput`: 벤치마크를 다중 사용자 처리량 모드로 실행합니다. (기본값)
*   `-s`, `--scale-factor`: TPC-H 데이터의 원천 크기(GB)를 설정합니다. (예: `1.0`, `30.0`, `100.0` 등)
*   `-p`, `--policy`: NUMA 메모리 할당 정책을 선택합니다. (`ddr-only`, `cxl-only`, `interleave`, `weighted`)
*   `--streams`: 동시 쿼리를 수행할 가상 사용자(스레드) 수를 결정합니다. (예: `4`, `8`, `12` 등)
*   `--skip-load`: **(매우 유용)** 이미 워커 노드에 데이터가 샤딩되어 적재되어 있다면, 데이터 생성 및 로딩 과정을 건너뛰고 메모리에 상주한 테이블을 대상으로 즉시 쿼리만 실행합니다.
*   `--no-compress`: 데이터 압축을 해제하고 저장하여 메모리 전송 대역폭에 강한 스트레스를 줍니다.

---

## 2. CPU 사용량 극대화 방안 (CPU Saturation)

ClickHouse의 SIMD 가속과 다중 스레드 연산 능력을 극대화하여 CPU 코어를 100%에 가깝게 포화 상태로 만드는 설정입니다.

### ① 동시성 스트림 수(`--streams`) 상향
가장 직관적인 방법으로, CPU의 물리 코어 및 하이퍼스레딩 개수에 가깝거나 그 이상의 스트림을 설정합니다.
```bash
# 24코어 시스템인 경우 12~24개 이상의 스트림으로 다중 접속 부하 생성
./benchmark.py --mode throughput --streams 16 --skip-load
```

### ② 쿼리당 최대 스레드 할당 수(`max_threads`) 튜닝
단일 쿼리를 병렬 처리할 때 사용할 최대 CPU 스레드 수를 설정합니다.
*   **설정 파일 위치**: [config/memory_tuning.xml](config/memory_tuning.xml)
*   **조정 방식**: 기본값인 `<max_threads>8</max_threads>`를 워커가 동작하는 소켓의 **물리 코어 수 전체**에 맞춰 늘려줍니다. (예: 소켓당 물리 코어가 16개인 경우 `16`~`32`로 설정)
```xml
<clickhouse>
    <profiles>
        <default>
            <!-- 쿼리 병렬 연산 스레드 수를 물리 소켓 코어 수에 맞춤 -->
            <max_threads>16</max_threads>
        </default>
    </profiles>
</clickhouse>
```

---

## 3. 메모리 사용량 및 대역폭 극대화 방안 (Memory Saturation)

DDR 및 CXL 메모리 버스 대역폭에 최대 부하를 인가하고, 대규모 테이블 조인 시 램 공간을 한계치까지 점유하게 유도하는 핵심 설정들입니다.

### ① 데이터 스케일 아웃 (`-s` 확장 및 `--no-compress`)
*   **Scale Factor 상향**: `-s 30.0` 또는 `-s 100.0`으로 데이터 크기를 키워 절대적인 메모리 적재량을 늘립니다.
*   **비압축 스토리지 정책 활성화**: `--no-compress` 플래그를 사용하면 압축 알고리즘(LZ4)을 거치지 않은 순수 원본 컬럼 벡터를 메모리로 올립니다. CPU 연산 대비 **순수 메모리 버스(CXL/DDR Bus) 트래픽을 가중**시키는 효과가 있습니다.
    ```bash
    ./benchmark.py --mode throughput -s 30.0 --streams 8 --no-compress
    ```

### ② 벡터 블록 크기(`max_block_size`) 확장
ClickHouse의 강력한 성능 비결 중 하나는 데이터를 Vectorized 블록 단위로 묶어 CPU 레지스터에 넘기는 것입니다. 이 블록 크기를 키우면 중간 버퍼 연산 시 Transient RAM 사용량이 급격히 늘어납니다.
*   **설정 파일 위치**: [config/memory_tuning.xml](config/memory_tuning.xml)
*   **조정 방식**: 100만 행 단위(기본 1,048,576)에서 400만 행 이상으로 확장하여 CPU L3 캐시를 완전히 우회하고 RAM을 강제 점유하도록 유도합니다.
```xml
<max_block_size>4194304</max_block_size>
```

### ③ 메모리 캐시 우회 (Cache Bypass)
ClickHouse가 메모리에 올려둔 데이터 블록을 재사용하지 않고, **매 쿼리 실행 시마다 디스크/메모리 채널로부터 데이터를 처음부터 풀스캔**하도록 강제합니다.
*   **설정 파일 위치**: [config/memory_tuning.xml](config/memory_tuning.xml)
```xml
<use_uncompressed_cache>0</use_uncompressed_cache>
<merge_tree_max_rows_to_use_cache>0</merge_tree_max_rows_to_use_cache>
<merge_tree_max_bytes_to_use_cache>0</merge_tree_max_bytes_to_use_cache>
```

### ④ 디스크 스필링(Disk Spilling) 방지 및 메모리 한계 상향
대규모 분산 JOIN 및 Aggregation(GROUP BY) 시 메모리가 부족하면 디스크에 임시 데이터를 쓰기 시작하여(Disk Spilling) 물리 메모리 점유율 상승이 둔화될 수 있습니다. 이를 막기 위해 임계치 설정과 서버 한계치를 상향 조정합니다.

*   **서버 제한 상향 (`run_clickhouse_cluster.sh`)**:
    *   [remote_servers.xml 생성부](run_clickhouse_cluster.sh): `<max_server_memory_usage>`값을 늘려줍니다. (예: 128GB로 증설 시 `137438953472`)
    *   [memory_tuning.xml 생성부](run_clickhouse_cluster.sh): `<max_memory_usage>`값을 늘려줍니다. (예: 64GB로 증설 시 `68719476736`)
*   **In-Memory 조인 활성화 (`benchmark.py`)**:
    *   [benchmark.py의 쿼리 세팅부](benchmark.py): 디스크 스필링 임계값인 `max_bytes_before_external_group_by`와 `max_bytes_before_external_join` 설정을 더 크게 지정하여 중간 데이터를 끝까지 RAM 위에서만 처리하도록 유도합니다. (예: `34359738368` (32GB))

---

## 🛠️ 권장 실험 튜닝 설정 조합 예시

하드웨어 부하(Stress)를 극한으로 끌어올려 CXL 메모리 버스 지연 특성을 집중 관찰하고 싶을 때 추천하는 종합 설정값 세트입니다.

1.  **[config/memory_tuning.xml](config/memory_tuning.xml)** 수정:
    ```xml
    <clickhouse>
        <profiles>
            <default>
                <max_memory_usage>68719476736</max_memory_usage> <!-- 64GB -->
                <max_threads>16</max_threads> <!-- 소켓 내 코어 수에 맞게 조정 -->
                <use_uncompressed_cache>0</use_uncompressed_cache>
                <merge_tree_max_rows_to_use_cache>0</merge_tree_max_rows_to_use_cache>
                <merge_tree_max_bytes_to_use_cache>0</merge_tree_max_bytes_to_use_cache>
                <max_block_size>4194304</max_block_size> <!-- 대용량 벡터 연산 유도 -->
            </default>
        </profiles>
    </clickhouse>
    ```
2.  **벤치마크 드라이버 실행**:
    ```bash
    # 30GB Scale Factor, 압축 사용 안 함, 12개 가상 유저 동시 스트림, CXL 전용 바인딩
    ./benchmark.py --mode throughput --scale-factor 30.0 --streams 12 --policy cxl-only --no-compress
    ```
