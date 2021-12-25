package com.gmall.realtime.app.dwm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.gmall.realtime.app.func.DimAsyncFunction;
import com.gmall.realtime.beans.OrderDetail;
import com.gmall.realtime.beans.OrderInfo;
import com.gmall.realtime.beans.OrderWide;
import com.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Date;
import java.util.concurrent.TimeUnit;

/**
 * description:TODO 订单宽表
 * 写到dwm_order_wide
 * Created by thinkpad on 2021-10-10
 *  *  -需要启动的进程
 *  *      zk、kafka、hdfs、hbase、maxwell、BaseDBApp、OrderWideApp
 *  *  -执行流程
 *  *      运行模拟生成业务数据的jar包
 *  *      会有订单和订单明细数据插入到业务数据库MySQL中
 *  *      MySQL数据库发生了变化，会将变化记录到binlog中
 *  *      Maxwell会从binlog中读取数据，将读取到的数据封装为json发送到kafka ods_base_db_m
 *  *      BaseDBApp从ods_base_db_m读取数据，进行动态分流
 *  *          *前提：配置表中一定要配置订单和订单明细的配置项
 *  *      将订单和订单明细数据分别发送到了kafka的dwd_order_info 和dwd_order_detail主题中
 *  *      OrderWideApp从dwd_order_info 和dwd_order_detail主题中读取数据
 *  *      对读取到的两条流的数据进行类型的转换，jsonStr-->实体类对象
 *  *      指定Watermark以及提取事件时间字段
 *  *      使用keyby指定两条流的连接字段
 *  *      使用intervalJoin进行双流join
 *  *          A.intervalJoin(B).between(下界,上界).process()
 */
public class OrderWideApp {
    public static void main(String[] args) throws Exception {
        //TODO 1.基本环境准备
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        //TODO 2.检查点设置
//        TODO 3.从kafka中读取数据
//3.1 声明消费主题以及消费者组
        String orderInfoTpic = "dwd_order_info";
        String orderDetailTopic = "dwd_order_detail";
        String groupId = "order_wide_app_group";
//        3.2创建消费者对象orderInfo
        FlinkKafkaConsumer<String> orderInfoKafkaSource = MyKafkaUtil.getKafkaSource("dwd_order_info", "order_wide_app_group");
//        3.3创建消费者对象orderDetail
        FlinkKafkaConsumer<String> orderDetailKafkaSource = MyKafkaUtil.getKafkaSource("dwd_order_detail", "order_wide_app_group");
//        3.4消费数据
        DataStreamSource<String> orderInfoStrDS = env.addSource(orderInfoKafkaSource);
        DataStreamSource<String> orderDetailStrDS = env.addSource(orderDetailKafkaSource);
//        TODO 4.数据类型转换
//        --转换成OrderInfo
        SingleOutputStreamOperator<OrderInfo> orderInfoDS = orderInfoStrDS.map(
                new RichMapFunction<String, OrderInfo>() {
                    private SimpleDateFormat sdf;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

                    }

                    @Override
                    public OrderInfo map(String jsonStr) throws Exception {
                        OrderInfo orderInfo = JSON.parseObject(jsonStr, OrderInfo.class);
                        orderInfo.setCreate_ts(sdf.parse(orderInfo.getCreate_time()).getTime());
                        return orderInfo;
                    }
                }
        );
//        --转换成OrderDetail
        SingleOutputStreamOperator<OrderDetail> orderDetailDS = orderDetailStrDS.map(
                new RichMapFunction<String, OrderDetail>() {
                    private SimpleDateFormat sdf;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                    }

                    @Override
                    public OrderDetail map(String jsonStr) throws Exception {
                        OrderDetail orderDetail = JSON.parseObject(jsonStr, OrderDetail.class);
                        orderDetail.setCreate_ts(sdf.parse(orderDetail.getCreate_time()).getTime());
                        return orderDetail;
                    }
                }
        );

//        orderInfoDS.print(">>>>>>");
//        orderDetailDS.print("*******");

//        TODO 5.指定watermark 提取事件时间字段
//        5.1orderInfo
        SingleOutputStreamOperator<OrderInfo> orderInfoWithWatermarkDS = orderInfoDS.assignTimestampsAndWatermarks(
                WatermarkStrategy.<OrderInfo>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                        .withTimestampAssigner(
                                new SerializableTimestampAssigner<OrderInfo>() {
                                    @Override
                                    public long extractTimestamp(OrderInfo element, long recordTimestamp) {
                                        return element.getCreate_ts();
                                    }
                                }
                        )
        );
//       5.2orderDetail
        SingleOutputStreamOperator<OrderDetail> orderDetailWithWatermarkDS = orderDetailDS.assignTimestampsAndWatermarks(
                WatermarkStrategy.<OrderDetail>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                        .withTimestampAssigner(
                                new SerializableTimestampAssigner<OrderDetail>() {
                                    @Override
                                    public long extractTimestamp(OrderDetail element, long recordTimestamp) {
                                        return element.getCreate_ts();
                                    }
                                }
                        )
        );
//        TODO 6.指定连接字段
        KeyedStream<OrderInfo, Long> orderInfoKeyedDS = orderInfoWithWatermarkDS.keyBy(OrderInfo::getId);
        KeyedStream<OrderDetail, Long> orderDetailKeyedDS = orderDetailWithWatermarkDS.keyBy(OrderDetail::getOrder_id);
//        TODO 7.双流join 使用intervaljoin进行连接
        SingleOutputStreamOperator<OrderWide> orderWideDS = orderInfoKeyedDS
                .intervalJoin(orderDetailKeyedDS)
                .between(Time.seconds(-5), Time.seconds(5))
                .process(
                        new ProcessJoinFunction<OrderInfo, OrderDetail, OrderWide>() {
                            @Override
                            public void processElement(OrderInfo orderInfo, OrderDetail orderDetail, Context ctx, Collector<OrderWide> out) throws Exception {
                                out.collect(new OrderWide(orderInfo, orderDetail));
                            }
                        }
                );
//        orderWideDS.print(">>>");


        //TODO 8.和用户维度进行关联
        //1.将异步 I/O 操作应用于 DataStream 作为 DataStream 的一次转换操作
        SingleOutputStreamOperator<OrderWide> orderWideWithUserInfoDS = AsyncDataStream.unorderedWait(
                orderWideDS,
                //具体异步请求的操作
                new DimAsyncFunction<OrderWide>("DIM_USER_INFO") {
                    @Override
                    public void join(OrderWide orderWide, JSONObject dimInfoJsonObj) throws ParseException {
                        String gender = dimInfoJsonObj.getString("GENDER");
                        orderWide.setUser_gender(gender);
                        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
                        String birthdayStr = dimInfoJsonObj.getString("BIRTHDAY");
                        Date birthdayDate = sdf.parse(birthdayStr);
                        Long birthdayTime = birthdayDate.getTime();
                        Long currentTimeMillis = System.currentTimeMillis();

                        Long ageLong = (currentTimeMillis - birthdayTime) / 1000 / 60 / 60 / 24 / 365;

                        orderWide.setUser_age(ageLong.intValue());
                    }

                    @Override
                    public String getKey(OrderWide orderWide) {
                        return orderWide.getUser_id().toString();
                    }
                },
                60,
                TimeUnit.SECONDS
        );

        //TODO 9.和地区维度进行关联：
        SingleOutputStreamOperator<OrderWide> orderWideWithProvinceDS = AsyncDataStream.unorderedWait(
                orderWideWithUserInfoDS,
                new DimAsyncFunction<OrderWide>("DIM_BASE_PROVINCE") {
                    @Override
                    public void join(OrderWide orderWide, JSONObject dimInfoJsonObj) throws Exception {
                        orderWide.setProvince_name(dimInfoJsonObj.getString("NAME"));
                        orderWide.setProvince_area_code(dimInfoJsonObj.getString("AREA_CODE"));
                        orderWide.setProvince_iso_code(dimInfoJsonObj.getString("ISO_CODE"));
                        orderWide.setProvince_3166_2_code(dimInfoJsonObj.getString("ISO_3166_2"));
                    }

                    @Override
                    public String getKey(OrderWide orderWide) {
                        return orderWide.getProvince_id().toString();
                    }
                },
                60, TimeUnit.SECONDS
        );
//        orderWideWithProvinceDS.print(">>>>>");
//        TODO 10.和商品维度进行关联    dimInfoJsonObj为关联到的维度信息
        SingleOutputStreamOperator<OrderWide> orderWideWithSkuDstream = AsyncDataStream.unorderedWait(
                orderWideWithProvinceDS,
                new DimAsyncFunction<OrderWide>("DIM_SKU_INFO") {
                    @Override
                    public void join(OrderWide orderWide, JSONObject dimInfoJsonObj) throws ParseException, Exception {
                        orderWide.setSku_name(dimInfoJsonObj.getString("SKU_NAME"));
                        orderWide.setCategory3_id(dimInfoJsonObj.getLong("CATEGORY3_ID"));
                        orderWide.setSpu_id(dimInfoJsonObj.getLong("SPU_ID"));
                        orderWide.setTm_id(dimInfoJsonObj.getLong("TM_ID"));
                    }

                    @Override
                    public String getKey(OrderWide orderWide) {
                        return orderWide.getSku_id().toString();
                    }
                },
                60, TimeUnit.SECONDS
        );

        //TODO 11.关联SPU商品维度
        SingleOutputStreamOperator<OrderWide> orderWideWithSpuDstream = AsyncDataStream.unorderedWait(
                orderWideWithSkuDstream, new DimAsyncFunction<OrderWide>("DIM_SPU_INFO") {
                    @Override
                    public void join(OrderWide orderWide, JSONObject jsonObject) throws Exception {
                        orderWide.setSpu_name(jsonObject.getString("SPU_NAME"));
                    }

                    @Override
                    public String getKey(OrderWide orderWide) {
                        return String.valueOf(orderWide.getSpu_id());
                    }
                }, 60, TimeUnit.SECONDS);

//TODO 12.关联品类维度
        SingleOutputStreamOperator<OrderWide> orderWideWithCategory3Dstream = AsyncDataStream.unorderedWait(
                orderWideWithSpuDstream, new DimAsyncFunction<OrderWide>("DIM_BASE_CATEGORY3") {
                    @Override
                    public void join(OrderWide orderWide, JSONObject jsonObject) throws Exception {
                        orderWide.setCategory3_name(jsonObject.getString("NAME"));
                    }

                    @Override
                    public String getKey(OrderWide orderWide) {
                        return String.valueOf(orderWide.getCategory3_id());
                    }
                }, 60, TimeUnit.SECONDS);

        //TODO 13.关联品牌维度
        SingleOutputStreamOperator<OrderWide> orderWideWithTmDstream = AsyncDataStream.unorderedWait(
                orderWideWithCategory3Dstream, new DimAsyncFunction<OrderWide>("DIM_BASE_TRADEMARK") {
                    @Override
                    public void join(OrderWide orderWide, JSONObject jsonObject) throws Exception {
                        orderWide.setTm_name(jsonObject.getString("TM_NAME"));
                    }

                    @Override
                    public String getKey(OrderWide orderWide) {
                        return String.valueOf(orderWide.getTm_id());
                    }
                }, 60, TimeUnit.SECONDS);
//        orderWideWithTmDstream.print(">>>>>>>");

//        TODO 14将订单宽表数据写到kafka主题中
        orderWideWithTmDstream
//                .map(orderWide->JSON.toJSONString(orderWide))
                .map(JSON::toJSONString)
                .addSink(MyKafkaUtil.getKafkaSink("dwm_order_wide"));
        env.execute();
    }
}
