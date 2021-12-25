package com.gmall.realtime.common;

/**
 * description:
 * Created by thinkpad on 2021-10-09
 */
public class GmallConfig {
    public static final String HBASE_SCHEMA = "GMALL0428_REALTIME";
    public static final String PHOENIX_SERVER = "jdbc:phoenix:hadoop102,hadoop103,hadoop104:2181";
    public static final String CLICKHOUSE_URL = "jdbc:clickhouse://hadoop102:8123/default";

}
