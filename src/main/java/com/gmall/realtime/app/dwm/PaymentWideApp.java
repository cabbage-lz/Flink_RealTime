package com.gmall.realtime.app.dwm;

import com.alibaba.fastjson.JSON;
import com.gmall.realtime.beans.OrderWide;
import com.gmall.realtime.beans.PaymentInfo;
import com.gmall.realtime.beans.PaymentWide;
import com.gmall.realtime.utils.DateTimeUtil;
import com.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;


import java.time.Duration;

/**
 * description:TODO 支付主题宽表
 * Created by thinkpad on 2021-10-13
 */
public class PaymentWideApp {
    public static void main(String[] args) throws Exception {
//          TODO 1.基本环境准备
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
//        TODO 2.设置检查点

//        TODO 3.从kafka中读取数据
//          3.1声明消费主题及消费者组
        String ordeeWideTopic = "dwm_order_wide";
        String paymentInfoTopic = "dwd_payment_info";
        String groupId = "payment_wide_app_group";
//          3.2创建消费者对象
        FlinkKafkaConsumer<String> orderWideSource = MyKafkaUtil.getKafkaSource(ordeeWideTopic, groupId);
        FlinkKafkaConsumer<String> paymentInfoSource = MyKafkaUtil.getKafkaSource(paymentInfoTopic, groupId);
//          3.3消费数据封装为流
        DataStreamSource<String> orderWideStrDS = env.addSource(orderWideSource);
        DataStreamSource<String> paymentInfoStrDS = env.addSource(paymentInfoSource);
//        TODO 4.读取的数据进行类型转换（转换成对应的实体类对象）
//4.1 订单宽表
        SingleOutputStreamOperator<OrderWide> orderWideDS = orderWideStrDS.map(
                new MapFunction<String, OrderWide>() {
                    @Override
                    public OrderWide map(String value) throws Exception {
                        OrderWide orderWide = JSON.parseObject(value, OrderWide.class);
                        return orderWide;
                    }
                }
        );
//          4.2支付
        SingleOutputStreamOperator<PaymentInfo> paymentInfoDS = paymentInfoStrDS.map(
                new MapFunction<String, PaymentInfo>() {
                    @Override
                    public PaymentInfo map(String value) throws Exception {
                        PaymentInfo paymentInfo = JSON.parseObject(value, PaymentInfo.class);
                        return paymentInfo;
                    }
                }
        );
//orderWideDS.print(">>>>>>>>");
//paymentInfoDS.print("*****");
//        TODO 5.指定水位线和提取时间戳
//5.1 订单宽表
        SingleOutputStreamOperator<OrderWide> orderWideWithWatermarkDS = orderWideDS.assignTimestampsAndWatermarks(
                WatermarkStrategy.<OrderWide>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                        .withTimestampAssigner(
                                new SerializableTimestampAssigner<OrderWide>() {
                                    @Override
                                    public long extractTimestamp(OrderWide orderWide, long recordTimestamp) {
                                        return DateTimeUtil.toTs(orderWide.getCreate_time());
                                    }
                                }
                        )
        );
//5.2支付
        SingleOutputStreamOperator<PaymentInfo> paymentInfoWithWatermarkDS = paymentInfoDS.assignTimestampsAndWatermarks(
                WatermarkStrategy.<PaymentInfo>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                        .withTimestampAssigner(
                                new SerializableTimestampAssigner<PaymentInfo>() {
                                    @Override
                                    public long extractTimestamp(PaymentInfo paymentInfo, long recordTimestamp) {
                                        return DateTimeUtil.toTs(paymentInfo.getCallback_time());
                                    }
                                }
                        )

        );
        //TODO 6.使用keyby指定连接的字段
        //6.1 订单宽表
        KeyedStream<OrderWide, Long> orderWideKeyedDS = orderWideWithWatermarkDS.keyBy(OrderWide::getOrder_id);
        //6.2 支付
        KeyedStream<PaymentInfo, Long> paymentInfoKeyedDS = paymentInfoWithWatermarkDS.keyBy(PaymentInfo::getOrder_id);

        //TODO 7.双流join
        SingleOutputStreamOperator<PaymentWide> joinedDS = paymentInfoKeyedDS
                .intervalJoin(orderWideKeyedDS)
                .between(Time.seconds(-1800), Time.seconds(0))
                .process(
                        new ProcessJoinFunction<PaymentInfo, OrderWide, PaymentWide>() {
                            @Override
                            public void processElement(PaymentInfo paymentInfo, OrderWide orderWide, Context ctx, Collector<PaymentWide> out) throws Exception {
                                out.collect(new PaymentWide(paymentInfo, orderWide));
                            }
                        }
                );
//        TODO 8.将支付宽表数据写到kafka的dwm层主题中（类型转换）
//        joinedDS.print(">>>>");

        joinedDS
                .map(JSON::toJSONString)
                .addSink(MyKafkaUtil.getKafkaSink("dwm_payment_wide"));

        env.execute();
    }
}
