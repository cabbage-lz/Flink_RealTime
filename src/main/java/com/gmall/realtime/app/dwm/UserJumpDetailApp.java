package com.gmall.realtime.app.dwm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.*;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * description:TODO FlinkCEP
 * TODO跳出明细计算:
 * 统计last_page_id为空
 * 30分钟没访问
 * 或者访问了网站但是超时了
 * TODO 思路：
 * 基本环境准备
 * 从Kafka中读取数据
 * 将json格式字符串转换为json对象
 * 使用CEP过滤用户跳出行为
 * -定义pattern
 * -将定义模式应用到流上
 * -提取数据
 * 将跳出明细发送到kafka的dwm层
 * Created by thinkpad on 2021-10-10
 */
public class UserJumpDetailApp {
    public static void main(String[] args) throws Exception {
        //TODO 1.基本环境准备
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        //TODO 2.检查点设置
        //TODO 3.从kafka中读取数据
        String topic = "dwd_page_log";
        String groupId = "user_jump_detail_group";
        FlinkKafkaConsumer<String> source = MyKafkaUtil.getKafkaSource(topic, groupId);

/*               DataStream<String> kafkaDS = env
            .fromElements(
                "{\"common\":{\"mid\":\"101\"},\"page\":{\"page_id\":\"home\"},\"ts\":10000} ",
                "{\"common\":{\"mid\":\"102\"},\"page\":{\"page_id\":\"home\"},\"ts\":12000}",
                "{\"common\":{\"mid\":\"102\"},\"page\":{\"page_id\":\"good_list\",\"last_page_id\":" +
                    "\"home\"},\"ts\":15000} ",
                "{\"common\":{\"mid\":\"102\"},\"page\":{\"page_id\":\"good_list\",\"last_page_id\":" +
                    "\"detail\"},\"ts\":30000} "
            );*/
//        TODO 4.流中数据转换  jsonStr-》jsonObj
        DataStreamSource<String> kafkaDS = env.addSource(source);
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.map(kafkads -> JSON.parseObject(kafkads));
//        jsonObjDS.print(">>>>>");
//        TODO 5.指定Watermark以及提取事件时间字段
        SingleOutputStreamOperator<JSONObject> jsonObjWithWatermarkDS = jsonObjDS.assignTimestampsAndWatermarks(
                WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                        .withTimestampAssigner(
                                new SerializableTimestampAssigner<JSONObject>() {
                                    @Override
                                    public long extractTimestamp(JSONObject element, long recordTimestamp) {
                                        return element.getLong("ts");
                                    }
                                }
                        )
        );
//        TODO 6. 按照mid对数据进行分组
        KeyedStream<JSONObject, String> keyedDS = jsonObjWithWatermarkDS.keyBy(jsonobj -> jsonobj.getJSONObject("common").getString("mid"));
//        TODO 7.使用CEP过滤流中的用户跳出明细
        // 7.1定义pattern
        Pattern<JSONObject, JSONObject> pattern = Pattern.<JSONObject>begin("first").where(
                new SimpleCondition<JSONObject>() {
                    @Override
                    public boolean filter(JSONObject jsonObj) {
                        String lastPageId = jsonObj.getJSONObject("page").getString("last_page_id");
                        if (lastPageId == null || lastPageId.length() == 0) {
                            return true;
                        }
                        return false;
                    }
                }
        ).next("second").where(
                new SimpleCondition<JSONObject>() {
                    @Override
                    public boolean filter(JSONObject jsonObj) {
                        String pageId = jsonObj.getJSONObject("page").getString("page_id");
                        if (pageId != null) {
                            return true;
                        }
                        return false;
                    }
                }
        ).within(Time.seconds(10));
//        7.2将pattern应用到流上
        PatternStream<JSONObject> patternDS = CEP.pattern(keyedDS, pattern);
//        7.3将从流中提取符合pattern的数据
        OutputTag<String> timeoutTag = new OutputTag<String>("timeoutTag") {
        };
        SingleOutputStreamOperator<Object> resDS = patternDS.flatSelect(
                timeoutTag,
                new PatternFlatTimeoutFunction<JSONObject, String>() {
                    @Override
                    public void timeout(Map<String, List<JSONObject>> pattern, long l, Collector<String> out) throws Exception {
// 提取超时数据
                        List<JSONObject> timeoutList = pattern.get("first");
                        for (JSONObject jsonObj : timeoutList) {
                            out.collect(jsonObj.toJSONString());
                        }
                    }
                },
                new PatternFlatSelectFunction<JSONObject, Object>() {
                    @Override
                    public void flatSelect(Map<String, List<JSONObject>> map, Collector<Object> collector) throws Exception {
//提取完全匹配的数据 ，完全匹配的数据属于跳转，不需要提取
                    }
                }
        );
//        TODO 8.将符合条件的数据发送的kafka主题中去
// 8.1通过侧输出流标签获取数据
        DataStream<String> timeoutDS = resDS.getSideOutput(timeoutTag);
        timeoutDS.print(">>>");
        timeoutDS.addSink(MyKafkaUtil.getKafkaSink("dwm_user_jump_detail"));
        env.execute();
    }
}

