-- ====================================================================
--  ClickHouse TPC-H Distributed Tables Schema for Coordinator Node
-- ====================================================================

CREATE TABLE IF NOT EXISTS region
(
    r_regionkey Int32,
    r_name      LowCardinality(String),
    r_comment   String
) ENGINE = Distributed(tpc_cluster, default, region_local, r_regionkey);

CREATE TABLE IF NOT EXISTS nation
(
    n_nationkey Int32,
    n_name      LowCardinality(String),
    n_regionkey Int32,
    n_comment   String
) ENGINE = Distributed(tpc_cluster, default, nation_local, n_nationkey);

CREATE TABLE IF NOT EXISTS part
(
    p_partkey     Int32,
    p_name        String,
    p_mfgr        LowCardinality(String),
    p_brand       LowCardinality(String),
    p_type        String,
    p_size        Int32,
    p_container   LowCardinality(String),
    p_retailprice Decimal(15, 2),
    p_comment     String
) ENGINE = Distributed(tpc_cluster, default, part_local, p_partkey);

CREATE TABLE IF NOT EXISTS supplier
(
    s_suppkey   Int32,
    s_name      String,
    s_address   String,
    s_nationkey Int32,
    s_phone     String,
    s_acctbal   Decimal(15, 2),
    s_comment   String
) ENGINE = Distributed(tpc_cluster, default, supplier_local, s_suppkey);

CREATE TABLE IF NOT EXISTS partsupp
(
    ps_partkey    Int32,
    ps_suppkey    Int32,
    ps_availqty   Int32,
    ps_supplycost Decimal(15, 2),
    ps_comment    String
) ENGINE = Distributed(tpc_cluster, default, partsupp_local, ps_partkey);

CREATE TABLE IF NOT EXISTS customer
(
    c_custkey    Int32,
    c_name       String,
    c_address    String,
    c_nationkey  Int32,
    c_phone      String,
    c_acctbal    Decimal(15, 2),
    c_mktsegment LowCardinality(String),
    c_comment    String
) ENGINE = Distributed(tpc_cluster, default, customer_local, c_custkey);

CREATE TABLE IF NOT EXISTS orders
(
    o_orderkey      Int32,
    o_custkey       Int32,
    o_orderstatus   LowCardinality(String),
    o_totalprice    Decimal(15, 2),
    o_orderdate     Date,
    o_orderpriority LowCardinality(String),
    o_clerk         String,
    o_shippriority  Int32,
    o_comment       String
) ENGINE = Distributed(tpc_cluster, default, orders_local, o_orderkey);

CREATE TABLE IF NOT EXISTS lineitem
(
    l_orderkey      Int32,
    l_partkey       Int32,
    l_suppkey       Int32,
    l_linenumber    Int32,
    l_quantity      Decimal(15, 2),
    l_extendedprice Decimal(15, 2),
    l_discount      Decimal(15, 2),
    l_tax           Decimal(15, 2),
    l_returnflag    LowCardinality(String),
    l_linestatus    LowCardinality(String),
    l_shipdate      Date,
    l_commitdate    Date,
    l_receiptdate   Date,
    l_shipinstruct  LowCardinality(String),
    l_shipmode      LowCardinality(String),
    l_comment       String
) ENGINE = Distributed(tpc_cluster, default, lineitem_local, l_orderkey);
