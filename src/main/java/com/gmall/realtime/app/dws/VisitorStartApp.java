package com.gmall.realtime.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.gmall.realtime.beans.VisitorStats;
import com.gmall.realtime.utils.ClickHouseUtil;
import com.gmall.realtime.utils.DateTimeUtil;
import com.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.Date;

/**
 * description:TODO DWS层访客主题统计
 * Created by thinkpad on 2021-10-15
 */
public class VisitorStartApp {
    public static void main(String[] args) throws Exception {
//        TODO 1.1.基本环境准备
        //TODO 1.基本环境准备
        //1.1 流处理环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //1.2 设置并行度
        env.setParallelism(4);

        //TODO 2.设置检查点
        //TODO 3.从kafka中读取数据
        //3.1 声明消费的主题以及消费者组
        String pageLogTopic = "dwd_page_log";
        String uvTopic = "dwm_unique_visitor";
        String ujdTopic = "dwm_user_jump_detail";
        String groupId = "visitor_stats_app_group";

        //3.2 创建消费者对象
        FlinkKafkaConsumer<String> pageLogSource = MyKafkaUtil.getKafkaSource(pageLogTopic, groupId);
        FlinkKafkaConsumer<String> uvSource = MyKafkaUtil.getKafkaSource(uvTopic, groupId);
        FlinkKafkaConsumer<String> ujdSource = MyKafkaUtil.getKafkaSource(ujdTopic, groupId);

        //3.3 消费数据  封装为流
        DataStreamSource<String> pageLogStrDS = env.addSource(pageLogSource);
        DataStreamSource<String> uvStrDS = env.addSource(uvSource);
        DataStreamSource<String> ujdStrDS = env.addSource(ujdSource);

//        pageLogStrDS.print();
//        uvStrDS.print();
//        ujdStrDS.print();
        //TODO 4.对流中数据进行类型转换  jsonStr-->VisitorStats对象
        SingleOutputStreamOperator<VisitorStats> pageLogStatsDS = pageLogStrDS.map(
                new MapFunction<String, VisitorStats>() {
                    @Override
                    public VisitorStats map(String jsonStr) throws Exception {
                        JSONObject jsonObj = JSON.parseObject(jsonStr);
                        JSONObject commonJsonObj = jsonObj.getJSONObject("common");
                        JSONObject pageJsonObj = jsonObj.getJSONObject("page");
                        VisitorStats visitorStats = new VisitorStats(
                                "",
                                "",
                                commonJsonObj.getString("vc"),
                                commonJsonObj.getString("ch"),
                                commonJsonObj.getString("ar"),
                                commonJsonObj.getString("is_new"),
                                0L, 1L, 0L, 0L,
                                pageJsonObj.getLong("during_time"),
                                jsonObj.getLong("ts")
                        );
//                        判断是否是新的会话
                        String lastPageId = pageJsonObj.getString("last_page_id");
                        if (lastPageId == null || lastPageId.length() == 0) {
                            visitorStats.setSv_ct(1L);
                        }
                        return visitorStats;
                    }
                }
        );

//        读取UV
        SingleOutputStreamOperator<VisitorStats> uvStatsDS = uvStrDS.map(
                new MapFunction<String, VisitorStats>() {
                    @Override
                    public VisitorStats map(String jsonStr) throws Exception {
                        JSONObject jsonObj = JSON.parseObject(jsonStr);
                        JSONObject commonJsonObj = jsonObj.getJSONObject("common");
                        VisitorStats visitorStats = new VisitorStats(
                                "",
                                "",
                                commonJsonObj.getString("vc"),
                                commonJsonObj.getString("ch"),
                                commonJsonObj.getString("ar"),
                                commonJsonObj.getString("is_new"),
                                1L, 0L, 0L, 0L,
                                0L,
                                jsonObj.getLong("ts")
                        );
                        return visitorStats;
                    }
                }
        );

//        读取ujd
        SingleOutputStreamOperator<VisitorStats> ujdStateDS = ujdStrDS.map(
                new MapFunction<String, VisitorStats>() {
                    @Override
                    public VisitorStats map(String jsonStr) throws Exception {
                        JSONObject jsonObj = JSON.parseObject(jsonStr);
                        JSONObject commonJsonObj = jsonObj.getJSONObject("common");

                        VisitorStats visitorStats = new VisitorStats(
                                "",
                                "",
                                commonJsonObj.getString("vc"),
                                commonJsonObj.getString("ch"),
                                commonJsonObj.getString("ar"),
                                commonJsonObj.getString("is_new"),
                                0L, 0L, 0L, 1L,
                                0L,
                                jsonObj.getLong("ts")
                        );
                        return visitorStats;
                    }
                }
        );
        //TODO 5.将三条流的数据合并在一起
        DataStream<VisitorStats> unionDS = pageLogStatsDS.union(
                uvStatsDS,
                ujdStateDS);
        unionDS.print(">>>>");
        //TODO 6.指定Watermark以及提取事件时间字段
        SingleOutputStreamOperator<VisitorStats> visitorStatsWithWaterMark = unionDS.assignTimestampsAndWatermarks(
                WatermarkStrategy.<VisitorStats>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                        .withTimestampAssigner(
                                new SerializableTimestampAssigner<VisitorStats>() {
                                    @Override
                                    public long extractTimestamp(VisitorStats visitorStats, long recordTimestamp) {
                                        return visitorStats.getTs();
                                    }
                                }
                        )
        );

        //TODO 7.分组
        KeyedStream<VisitorStats, Tuple4<String, String, String, String>> keyedDS = visitorStatsWithWaterMark.keyBy(
                new KeySelector<VisitorStats, Tuple4<String, String, String, String>>() {
                    @Override
                    public Tuple4<String, String, String, String> getKey(VisitorStats visitorStats) throws Exception {
                        return Tuple4.of(
                                visitorStats.getVc(),
                                visitorStats.getAr(),
                                visitorStats.getCh(),
                                visitorStats.getIs_new());
                    }
                }
        );
        //TODO 8.开窗
        WindowedStream<VisitorStats, Tuple4<String, String, String, String>, TimeWindow> windowDS = keyedDS.window(TumblingEventTimeWindows.of(Time.seconds(10)));

        //TODO 9.聚合计算
        SingleOutputStreamOperator<VisitorStats> reduceDS = windowDS
                .reduce(
                        new ReduceFunction<VisitorStats>() {
                            @Override
                            public VisitorStats reduce(VisitorStats stats1, VisitorStats stats2) throws Exception {
                                stats1.setPv_ct(stats1.getPv_ct() + stats2.getPv_ct());
                                stats1.setUv_ct(stats1.getUv_ct() + stats2.getUv_ct());
                                stats1.setUj_ct(stats1.getUj_ct() + stats2.getUj_ct());
                                stats1.setSv_ct(stats1.getSv_ct() + stats2.getSv_ct());
                                stats1.setDur_sum(stats1.getDur_sum() + stats2.getDur_sum());
                                return stats1;
                            }
                        },
                        new ProcessWindowFunction<VisitorStats, VisitorStats, Tuple4<String, String, String, String>, TimeWindow>() {
                            @Override
                            public void process(Tuple4<String, String, String, String> stringStringStringStringTuple4, Context context, Iterable<VisitorStats> visitorStats, Collector<VisitorStats> out) throws Exception {
                                // 将getStart()为long类型，使用我们自己封装的类DateTimeUtil转为日期
                                for (VisitorStats visitorStat : visitorStats) {
                                    visitorStat.setStt(DateTimeUtil.toYmdhms(new Date(context.window().getStart())));
                                    visitorStat.setEdt(DateTimeUtil.toYmdhms(new Date(context.window().getEnd())));
                                    visitorStat.setTs(System.currentTimeMillis());
                                    out.collect(visitorStat);
                                }
                            }
                        }
                );
//        reduceDS.print("********");
        //TODO 10.将聚合的结果写到OLAP数据库  clickhouse
        // JdbcSink.sink()返回sinkFunction.      addSink需要传递的参数就是sinkFunction类型的
//        参数1：sql 占位符由流中对象的属性值；来替换，怎么替换参数2提供方法
//        参数2：重写的accept方法参数,给问号占位符赋值
//        参数3：执行的时候一些选项设置批次大小
//        参数4：连接数据库的相关属性
        reduceDS.addSink(
                ClickHouseUtil.getSinkFunction("insert into visitor_stats_0428 values(?,?,?,?,?,?,?,?,?,?,?,?)")
        );

        env.execute();


    }
}
