package com.gmall.realtime.app.dwm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.text.SimpleDateFormat;

/**
 * description:TODO UV计算
 * Created by thinkpad on 2021-10-09
 */
public class UniqueVisitApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
//      TODO  1.从kafka读取数据
        String groupId = "unique_visit_app";
        String topic = "dwd_page_log";

        FlinkKafkaConsumer<String> kafkaSource = MyKafkaUtil.getKafkaSource(topic, groupId);
        DataStreamSource<String> jsonStream = env.addSource(kafkaSource);
//        TODO 2.结构转换  字符串--》json
        SingleOutputStreamOperator<JSONObject> jsonObjStream = jsonStream.map(jsonString -> JSON.parseObject(jsonString));

        jsonObjStream.print("uv:");
//        TODO 3.按照mid分组
        KeyedStream<JSONObject, String> keyByWithMidDstream = jsonObjStream.keyBy(jsonObj -> jsonObj.getJSONObject("common").getString("mid"));

//        TODO 4.过滤老访客
        SingleOutputStreamOperator<JSONObject> filterDS = keyByWithMidDstream.filter(new RichFilterFunction<JSONObject>() {
            //定义状态用于存放最后访问的日期
            private ValueState<String> lastValueState;
            // 日期格式
            private SimpleDateFormat sdf;

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                sdf = new SimpleDateFormat("yyyyMMdd");
                ValueStateDescriptor<String> valueStateDescriptor = new ValueStateDescriptor<>("lastViewDateState", String.class);
//                        设置状态失效时间
                valueStateDescriptor.enableTimeToLive(StateTtlConfig.newBuilder(Time.days(1))
                        .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                        .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                        .build());

                lastValueState = getRuntimeContext().getState(valueStateDescriptor);


            }

            @Override
            public boolean filter(JSONObject jsonObj) throws Exception {
                String lastPageId = jsonObj.getJSONObject("page").getString("last_page_id");
                if (lastPageId != null && lastPageId.length() > 0) {
                    return false;
                }
                //获取上次访问日期
                String lastVisitDate = lastValueState.value();
                //获取当前访问日期
                String curVisitDate = sdf.format(jsonObj.getLong("ts"));
                //如果上次访问时间为当天
                if (lastVisitDate != null && lastVisitDate.length() > 0 && lastVisitDate.equals(curVisitDate)) {
                    return false;
                } else {
                    lastValueState.update(curVisitDate);
                    return true;
                }

            }

        });

//TODO 7.将UV数据写到kafka的dwm主题中
        filterDS.print(">>>");
        filterDS
                .map(jsonObj -> jsonObj.toJSONString())
                .addSink(MyKafkaUtil.getKafkaSink("dwm_unique_visitor"));
        env.execute();
    }
}
