package com.gmall.realtime.app.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.text.SimpleDateFormat;

/**
 * description:TODO  日志数据分流
 * Created by thinkpad on 2021-09-29
 * *  需要启动的进程
 * *      zk、kafka、logger.sh（nginx,数据采集程序）、[hdfs]、BaseLogApp
 * *  执行流程
 * *      -模拟生成日志数据
 * *      -将生成的日志交给nginx
 * *      -nginx将请求转发给日志采集服务
 * *      -日志采集服务将日志发送到kafka的ods_base_log
 * *      -(FLink程序)BaseLogApp从ods_base_log读取数据
 */
public class BaseLogApp {
    public static void main(String[] args) throws Exception {
//      TODO 1.环境准备
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
//      TODO 2.检查点设置
        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(60000L);
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 3000L));
        env.setStateBackend(new FsStateBackend("hdfs://hadoop102:8020/gmall/ck"));
//        TODO 指定操作hdfs的用户
        System.setProperty("HADOOP_USER_NAME", "atguigu");
//      TODO 3.从kafka消费主题
//         TODO 3.1声明消费主题、消费者组
        String topic = "ods_base_log";
        String groupId = "base_log_app_group";
//        TODO 3.2创建消费者对象
        FlinkKafkaConsumer<String> flinkKafkaConsumer = MyKafkaUtil.getKafkaSource(topic, groupId);
//        TODO 3.3消费数据
        DataStreamSource<String> kafkaStrDS = env.addSource(flinkKafkaConsumer);


//        TODO 4.字符串转换成json对象
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaStrDS.map(
                jsonStr -> JSON.parseObject(jsonStr)
        );
//        jsonObjDS.print("===>");
//        TODO 5.新老访客标记修复
//            5.1按照设备ID分组
        KeyedStream<JSONObject, String> keyedDS = jsonObjDS.keyBy(jsonObj -> jsonObj.getJSONObject("common").getString("mid"));
//        5.2分组后的数据进行状态修复
        SingleOutputStreamOperator<JSONObject> jsonObjWithNewDS = keyedDS.map(
                new RichMapFunction<JSONObject, JSONObject>() {
                    // 定义一个状态用于存放设备上次访问的日期
                    private ValueState<String> lastVisitDateState;
                    private SimpleDateFormat sdf;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        lastVisitDateState = getRuntimeContext().getState(
                                new ValueStateDescriptor<String>("lastVisitDateState", String.class)
                        );
                        sdf = new SimpleDateFormat("yyyyMMdd");
                    }

                    @Override
                    public JSONObject map(JSONObject jsonObj) throws Exception {
                        String isNew = jsonObj.getJSONObject("common").getString("is_new");

//                        如果新老访客的标记为1，那么有可能需要修复
                        if ("1".equals(isNew)) {
//                           从状态中获取上次访问日期
                            String lastVisitDate = lastVisitDateState.value();

                            String curVisitDate = sdf.format(jsonObj.getLong("ts"));
//                            判断上次访问日期是否存在   非空判断;lastVisitDate !=null && lastVisitDate.length()>0
                            if (lastVisitDate != null && lastVisitDate.length() > 0) {
//                                不为空，说明曾经访问过，需要修复,同时判断当前访问日期与上次日期是否相等，不相等则修复
                                if (!curVisitDate.equals(lastVisitDate)) {
                                    isNew = "0";
                                    jsonObj.getJSONObject("common").put("is_new", isNew);
                                }
                            } else {
//                                为空，不需要修复,将当前时间更新进状态变量
                                lastVisitDateState.update(curVisitDate);
                            }
                        }
                        return jsonObj;
                    }
                }
        );
////测试新老访客修复
//        jsonObjWithNewDS.print();
//TODO  6.分流 启动侧输出流 曝光侧输出流 页面--主流
//        6.1定义侧输出流标签
        OutputTag<String> startTag = new OutputTag<String>("startTag") {
        };
        OutputTag<String> displayTag = new OutputTag<String>("displayTag") {
        };

//  TODO 6.2 分流
        SingleOutputStreamOperator<String> pageDS = jsonObjWithNewDS.process(
                new ProcessFunction<JSONObject, String>() {
                    @Override
                    public void processElement(JSONObject jsonObj, Context ctx, Collector<String> out) throws Exception {
//                        判断是否为启动日志
                        JSONObject startJsonObj = jsonObj.getJSONObject("start");
                        String jsonStr = jsonObj.toJSONString();
                        if (startJsonObj != null && startJsonObj.size() > 0) {
//                            启动日志
                            ctx.output(startTag, jsonStr);
                        } else {
//                            如果不是启动日志，则都为页面日志
                            out.collect(jsonStr);
//                            页面日志中可能有曝光日志
                            JSONArray displayArr = jsonObj.getJSONArray("displays");
                            if (displayArr != null && displayArr.size() > 0) {
                                Long ts = jsonObj.getLong("ts");  //加入曝光时间戳
                                String page_id = jsonObj.getJSONObject("page").getString("page_id");

//                                说明有曝光信息，遍历获取所有曝光数据
                                for (int i = 0; i < displayArr.size(); i++) {
                                    JSONObject displayJsonObj = displayArr.getJSONObject(i);
                                    displayJsonObj.put("ts", ts);
                                    displayJsonObj.put("page_id", page_id);
//                                    将曝光信息放入曝光侧输出流
                                    ctx.output(displayTag, displayJsonObj.toJSONString());
                                }
                            }
                        }
                    }
                }
        );
//        TODO 6.3 获取启动流和曝光流
        DataStream<String> startDS = pageDS.getSideOutput(startTag);
        DataStream<String> displayDS = pageDS.getSideOutput(displayTag);
        startDS.print(">>>>");
        displayDS.print("*****");
        pageDS.print("$$$$$");
//        TODO 7.将不同流中的数据写到不同的kafka主题中
        startDS.addSink(MyKafkaUtil.getKafkaSink("dwd_start_log"));     //FlinkKafkaProducer 继承了 TwoPhaseCommitSinkFunction 继承了  RichSinkFunction
        displayDS.addSink(MyKafkaUtil.getKafkaSink("dwd_display_log"));
        pageDS.addSink(MyKafkaUtil.getKafkaSink("dwd_page_log"));
        env.execute();
    }
}
