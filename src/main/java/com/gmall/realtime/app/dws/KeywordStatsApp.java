package com.gmall.realtime.app.dws;

import com.gmall.realtime.utils.MyKafkaUtil;

import com.gmall.realtime.app.func.KeywordUDTF;
import com.gmall.realtime.beans.GmallConstant;
import com.gmall.realtime.beans.KeywordStats;
import com.gmall.realtime.utils.ClickHouseUtil;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * Date: 2021/10/16
 * Desc: 关键词统计
 */
public class KeywordStatsApp {
    public static void main(String[] args) throws Exception {
        //TODO 1.基本环境准备
        //1.1 流处理环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //1.2 设置并行度
        env.setParallelism(4);
        //1.3 表执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //注册表函数
        tableEnv.createTemporarySystemFunction("ik_analyze", KeywordUDTF.class);

        //TODO 2.设置检查点(略)

        //TODO 3.从kafka中读取数据，创建动态表
        String topic = "dwd_page_log";
        String groupId = "keyword_stats_app_group";

        tableEnv.executeSql("CREATE TABLE page_view " +
                "(common MAP<STRING,STRING>, " +
                "page MAP<STRING,STRING>," +
                "ts BIGINT, " +
                "rowtime AS TO_TIMESTAMP(FROM_UNIXTIME(ts/1000, 'yyyy-MM-dd HH:mm:ss')) ," +
                "WATERMARK FOR  rowtime  AS  rowtime - INTERVAL '3' SECOND) " +
                "WITH (" + MyKafkaUtil.getKafkaDDL(topic, groupId) + ")");

        //TODO 4.过滤数据，从表中将搜索行为过滤出来
        Table fullwordView = tableEnv.sqlQuery("select page['item'] fullword ," +
                "rowtime from page_view  " +
                "where page['page_id']='good_list' " +
                "and page['item'] IS NOT NULL ");

        //TODO 5.分词统计   将表中字段和分词函数执行的结果进行连接
        Table keywordTable = tableEnv.sqlQuery("SELECT keyword, rowtime " +
                " FROM " + fullwordView + ", LATERAL TABLE(ik_analyze(fullword)) AS t(keyword)");

        //TODO 6.分组、开窗、聚合计算
        Table keywordStatsSearch = tableEnv.sqlQuery("select keyword,count(*) ct, '"
                + GmallConstant.KEYWORD_SEARCH + "' source ," +
                "DATE_FORMAT(TUMBLE_START(rowtime, INTERVAL '10' SECOND),'yyyy-MM-dd HH:mm:ss') stt," +
                "DATE_FORMAT(TUMBLE_END(rowtime, INTERVAL '10' SECOND),'yyyy-MM-dd HH:mm:ss') edt," +
                "UNIX_TIMESTAMP()*1000 ts from   " + keywordTable
                + " GROUP BY TUMBLE(rowtime, INTERVAL '10' SECOND ),keyword");

        //TODO 7.将动态表转换为流
        DataStream<KeywordStats> keywordStatsDS = tableEnv.toAppendStream(keywordStatsSearch, KeywordStats.class);

        keywordStatsDS.print(">>>>");

        //TODO 8.将流中的数据写到ClickHouse中
        keywordStatsDS.addSink(
                ClickHouseUtil.getSinkFunction("insert into keyword_stats_0428(keyword,ct,source,stt,edt,ts) values(?,?,?,?,?,?)")
        );

        env.execute();
    }
}
