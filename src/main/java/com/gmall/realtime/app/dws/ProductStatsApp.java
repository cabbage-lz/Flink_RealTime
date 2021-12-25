package com.gmall.realtime.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.gmall.realtime.app.func.DimAsyncFunction;
import com.gmall.realtime.beans.GmallConstant;
import com.gmall.realtime.beans.OrderWide;
import com.gmall.realtime.beans.PaymentWide;
import com.gmall.realtime.beans.ProductStats;
import com.gmall.realtime.utils.ClickHouseUtil;
import com.gmall.realtime.utils.DateTimeUtil;
import com.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.concurrent.TimeUnit;

/**
 * description:商品主题表
 * 点击
 * 曝光
 * 收藏
 * 加入购物车
 * 下单
 * 支付
 * 退款
 * 评价
 * Created by thinkpad on 2021-10-17
 */
public class ProductStatsApp {
    public static void main(String[] args) throws Exception {
        //TODO 1.基本环境准备
        //1.1 流处理环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //1.2 设置并新股
        env.setParallelism(4);
        //TODO 2.设置检查点(略)
        //TODO 3.从kafka中读取数据
        //3.1 声明消费的主题以及消费者组
        String pageViewSourceTopic = "dwd_page_log";
        String favorInfoSourceTopic = "dwd_favor_info";
        String cartInfoSourceTopic = "dwd_cart_info";
        String orderWideSourceTopic = "dwm_order_wide";
        String paymentWideSourceTopic = "dwm_payment_wide";
        String refundInfoSourceTopic = "dwd_order_refund_info";
        String commentInfoSourceTopic = "dwd_comment_info";
        String groupId = "product_stats_app";
        //3.2 创建消费者对象
        FlinkKafkaConsumer<String> pageViewSource = MyKafkaUtil.getKafkaSource(pageViewSourceTopic, groupId);
        FlinkKafkaConsumer<String> orderWideSource = MyKafkaUtil.getKafkaSource(orderWideSourceTopic, groupId);
        FlinkKafkaConsumer<String> paymentWideSource = MyKafkaUtil.getKafkaSource(paymentWideSourceTopic, groupId);
        FlinkKafkaConsumer<String> favorInfoSourceSouce = MyKafkaUtil.getKafkaSource(favorInfoSourceTopic, groupId);
        FlinkKafkaConsumer<String> cartInfoSource = MyKafkaUtil.getKafkaSource(cartInfoSourceTopic, groupId);
        FlinkKafkaConsumer<String> refundInfoSource = MyKafkaUtil.getKafkaSource(refundInfoSourceTopic, groupId);
        FlinkKafkaConsumer<String> commentInfoSource = MyKafkaUtil.getKafkaSource(commentInfoSourceTopic, groupId);
        //3.3 消费数据  封装为流
        DataStreamSource<String> pvDS = env.addSource(pageViewSource);
        DataStreamSource<String> favorInfoDS = env.addSource(favorInfoSourceSouce);
        DataStreamSource<String> cartInfoDS = env.addSource(cartInfoSource);
        DataStreamSource<String> orderWideDS = env.addSource(orderWideSource);
        DataStreamSource<String> paymentWideDS = env.addSource(paymentWideSource);
        DataStreamSource<String> refundInfoDS = env.addSource(refundInfoSource);
        DataStreamSource<String> commentInfoDS = env.addSource(commentInfoSource);
        //TODO 4.对流中数据进行类型转换  jsonStr-->ProductStats对象
//4.1点击和曝光
        SingleOutputStreamOperator<ProductStats> clickAndDisplayStatsDS = pvDS.process(
//                new ProcessFunction<String, ProductStats>() {
//                    @Override
//                    public void processElement(String jsonStr, Context ctx, Collector<ProductStats> out) throws Exception {
//                        JSONObject jsonObj = JSON.parseObject(jsonStr);
//                        Long ts = jsonObj.getLong("ts");
//                        JSONObject pageJsonObj = jsonObj.getJSONObject("page");
//                        String pageId = pageJsonObj.getString("page_id");
//
//                        if (pageId != null && "good_detail".equals(pageId)) {
////点击行为处理
//                            ProductStats productStats = ProductStats.builder()
//                                    .sku_id(pageJsonObj.getLong("item"))
//                                    .click_ct(1L)
//                                    .ts(ts)
//                                    .build();
//                            out.collect(productStats);
//
//                        }
//// 曝光行为处理
//                        JSONArray displayArr = jsonObj.getJSONArray("displays");
//                        if (displayArr != null && displayArr.size() > 0) {
//                            for (int i = 0; i < displayArr.size(); i++) {
//                                JSONObject displayJSONObj = displayArr.getJSONObject(i);
//                                if ("sku_id".equals(displayJSONObj.getString("item_type"))) {
//                                    //将当前曝光封装为一个商品统计对象
//                                    ProductStats productStats = ProductStats.builder()
//                                            .sku_id(displayJSONObj.getLong("item"))
//                                            .display_ct(1L)
//                                            .ts(ts)
//                                            .build();
//                                    out.collect(productStats);
//                                }
//                            }
//                        }
//
//                    }
//                }

                new ProcessFunction<String, ProductStats>() {
                    @Override
                    public void processElement(String jsonStr, Context ctx, Collector<ProductStats> out) throws Exception {
                        JSONObject jsonObj = JSON.parseObject(jsonStr);
                        JSONObject pageJsonObj = jsonObj.getJSONObject("page");
                        String pageId = pageJsonObj.getString("page_id");
                        Long ts = jsonObj.getLong("ts");
                        if (pageId != null && "good_detail".equals(pageId)){
//                            点击行为的商品为item
//                            点击行为
                            ProductStats productStats = ProductStats.builder()
                                    .sku_id(pageJsonObj.getLong("item"))
                                    .click_ct(1L)
                                    .ts(ts)
                                    .build();
                            out.collect(productStats);
                        }
//                        是否是曝光行为   jsonObj.getJSONArray
                        JSONArray displayArr = jsonObj.getJSONArray("displays");
                        if (displayArr !=null && displayArr.size()>0){
                            for (int i = 0; i < displayArr.size(); i++) {
                                JSONObject displayJsonObj= displayArr.getJSONObject(i);
//                                判断曝光的displayArrJSONObject  类型是否为商品
                                if ("sku_id".equals(displayJsonObj.getString("item_type"))){
                                    ProductStats productStats = ProductStats.builder()
                                            .sku_id(displayJsonObj.getLong("item"))
                                            .display_ct(1L)
                                            .ts(ts)
                                            .build();
                                    out.collect(productStats);
                                }

                            }
                        }
                    }
                }

        );
//4.2收藏和加购流处理
        SingleOutputStreamOperator<ProductStats> favorInfoStatsDS = favorInfoDS.map(
                new MapFunction<String, ProductStats>() {
                    @Override
                    public ProductStats map(String jsonStr) throws Exception {
                        JSONObject favorInfoStrJsonObj = JSON.parseObject(jsonStr);

                        return ProductStats.builder()
                                .sku_id(favorInfoStrJsonObj.getLong("sku_id"))
                                .favor_ct(1L)
                                .ts(DateTimeUtil.toTs(favorInfoStrJsonObj.getString("create_time")))
                                .build();
                    }
                }
        );
//        4.3加购
        SingleOutputStreamOperator<ProductStats> cartInfoStatsDS = cartInfoDS.map(
                json -> {
                    JSONObject cartInfo = JSON.parseObject(json);
                    Long ts = DateTimeUtil.toTs(cartInfo.getString("create_time"));
                    return ProductStats.builder().sku_id(cartInfo.getLong("sku_id"))
                            .cart_ct(1L).ts(ts).build();
                });
        //4.4 下单
        SingleOutputStreamOperator<ProductStats> orderWideStatsDS = orderWideDS.map(
                new MapFunction<String, ProductStats>() {
                    @Override
                    public ProductStats map(String jsonStr) throws Exception {
                        OrderWide orderWide = JSON.parseObject(jsonStr, OrderWide.class);
//                        HashSet<Long> hashSet = new HashSet<>();
//                        hashSet.add(orderWide.getOrder_id());
                        return ProductStats.builder()
                                .sku_id(orderWide.getSku_id())
                                .order_sku_num(orderWide.getSku_num())
                                .order_amount(orderWide.getSplit_total_amount())
                                .orderIdSet(new HashSet(Collections.singleton(orderWide.getOrder_id())))
                                .ts(DateTimeUtil.toTs(orderWide.getCreate_time()))
                                .build();
                    }
                }
        );
        //4.5 支付
        SingleOutputStreamOperator<ProductStats> paymentWideStatsDS = paymentWideDS.map(
                json -> {
                    PaymentWide paymentWide = JSON.parseObject(json, PaymentWide.class);
                    Long ts = DateTimeUtil.toTs(paymentWide.getCallback_time());
                    return ProductStats.builder().sku_id(paymentWide.getSku_id())
                            .payment_amount(paymentWide.getSplit_total_amount())
                            .paidOrderIdSet(new HashSet(Collections.singleton(paymentWide.getOrder_id())))
                            .ts(ts).build();
                });
        //4.6 退单
        SingleOutputStreamOperator<ProductStats> refundStatsDS = refundInfoDS.map(
                json -> {
                    JSONObject refundJsonObj = JSON.parseObject(json);
                    Long ts = DateTimeUtil.toTs(refundJsonObj.getString("create_time"));
                    ProductStats productStats = ProductStats.builder()
                            .sku_id(refundJsonObj.getLong("sku_id"))
                            .refund_amount(refundJsonObj.getBigDecimal("refund_amount"))
                            .refundOrderIdSet(
                                    new HashSet(Collections.singleton(refundJsonObj.getLong("order_id"))))
                            .ts(ts).build();
                    return productStats;
                });
//        评价
        SingleOutputStreamOperator<ProductStats> commonInfoStatsDS = commentInfoDS.map(
                json -> {
                    JSONObject commonJsonObj = JSON.parseObject(json);
                    Long ts = DateTimeUtil.toTs(commonJsonObj.getString("create_time"));
                    Long goodCt = GmallConstant.APPRAISE_GOOD.equals(commonJsonObj.getString("appraise")) ? 1L : 0L;
                    ProductStats productStats = ProductStats.builder()
                            .sku_id(commonJsonObj.getLong("sku_id"))
                            .comment_ct(1L).good_comment_ct(goodCt).ts(ts).build();
                    return productStats;
                });
        //TODO 5.将七条流的数据合并在一起
        DataStream<ProductStats> unionDS = clickAndDisplayStatsDS.union(
                favorInfoStatsDS,
                cartInfoStatsDS,
                orderWideStatsDS,
                paymentWideStatsDS,
                refundStatsDS,
                commonInfoStatsDS
        );
//        unionDS.print();
        //TODO 6.指定Watermark以及提取事件时间字段
        SingleOutputStreamOperator<ProductStats> productStatsWithWatermarkDS = unionDS.assignTimestampsAndWatermarks(
                WatermarkStrategy.<ProductStats>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                        .withTimestampAssigner(
                                new SerializableTimestampAssigner<ProductStats>() {
                                    @Override
                                    public long extractTimestamp(ProductStats productStats, long recordTimestamp) {
                                        return productStats.getTs();
                                    }
                                }
                        )
        );
        //TODO 7.分组
        KeyedStream<ProductStats, Long> keyedDS = productStatsWithWatermarkDS.keyBy(r -> r.getSku_id());
        //TODO 8.开窗
        WindowedStream<ProductStats, Long, TimeWindow> windowDS = keyedDS.window(TumblingEventTimeWindows.of(Time.seconds(10)));
        //TODO 9.聚合计算
        SingleOutputStreamOperator<ProductStats> reduceDS = windowDS.reduce(
                new ReduceFunction<ProductStats>() {
                    @Override
                    public ProductStats reduce(ProductStats stats1, ProductStats stats2) throws Exception {
                        stats1.setDisplay_ct(stats1.getDisplay_ct() + stats2.getDisplay_ct());
                        stats1.setClick_ct(stats1.getClick_ct() + stats2.getClick_ct());
                        stats1.setCart_ct(stats1.getCart_ct() + stats2.getCart_ct());
                        stats1.setFavor_ct(stats1.getFavor_ct() + stats2.getFavor_ct());
                        stats1.setOrder_amount(stats1.getOrder_amount().add(stats2.getOrder_amount()));
                        stats1.getOrderIdSet().addAll(stats2.getOrderIdSet());
                        stats1.setOrder_ct(stats1.getOrderIdSet().size() + 0L);
                        stats1.setOrder_sku_num(stats1.getOrder_sku_num() + stats2.getOrder_sku_num());
                        stats1.setPayment_amount(stats1.getPayment_amount().add(stats2.getPayment_amount()));

                        stats1.getRefundOrderIdSet().addAll(stats2.getRefundOrderIdSet());
                        stats1.setRefund_order_ct(stats1.getRefundOrderIdSet().size() + 0L);
                        stats1.setRefund_amount(stats1.getRefund_amount().add(stats2.getRefund_amount()));

                        stats1.getPaidOrderIdSet().addAll(stats2.getPaidOrderIdSet());
                        stats1.setPaid_order_ct(stats1.getPaidOrderIdSet().size() + 0L);

                        stats1.setComment_ct(stats1.getComment_ct() + stats2.getComment_ct());
                        stats1.setGood_comment_ct(stats1.getGood_comment_ct() + stats2.getGood_comment_ct());

                        return stats1;

                    }
                },
                new ProcessWindowFunction<ProductStats, ProductStats, Long, TimeWindow>() {
                    @Override
                    public void process(Long aLong, Context context, Iterable<ProductStats> elements, Collector<ProductStats> out) throws Exception {
                        for (ProductStats productStats : elements) {
                            productStats.setStt(DateTimeUtil.toYmdhms(new Date(context.window().getStart())));
                            productStats.setEdt(DateTimeUtil.toYmdhms(new Date(context.window().getEnd())));
                            productStats.setTs(System.currentTimeMillis());
                            out.collect(productStats);
                        }
                    }
                }
        );

        //reduceDS.print(">>>>>");

        //TODO 10.维度关联
        //10.1 和商品维度进行关联
        SingleOutputStreamOperator<ProductStats> productStatsWithSkuDS = AsyncDataStream.unorderedWait(
                reduceDS,
                new DimAsyncFunction<ProductStats>("DIM_SKU_INFO") {
                    @Override
                    public void join(ProductStats productStats, JSONObject dimInfoJsonObj) throws Exception {
                        productStats.setSku_name(dimInfoJsonObj.getString("SKU_NAME"));
                        productStats.setSku_price(dimInfoJsonObj.getBigDecimal("PRICE"));
                        productStats.setSpu_id(dimInfoJsonObj.getLong("SPU_ID"));
                        productStats.setCategory3_id(dimInfoJsonObj.getLong("CATEGORY3_ID"));
                        productStats.setTm_id(dimInfoJsonObj.getLong("TM_ID"));
                    }

                    @Override
                    public String getKey(ProductStats productStats) {
                        return productStats.getSku_id().toString();
                    }
                },
                60, TimeUnit.SECONDS
        );
        //10.2 和spu维度进行关联
        SingleOutputStreamOperator<ProductStats> productStatsWithSpuDS =
                AsyncDataStream.unorderedWait(productStatsWithSkuDS,
                        new DimAsyncFunction<ProductStats>("DIM_SPU_INFO") {
                            @Override
                            public void join(ProductStats productStats, JSONObject jsonObject) throws Exception {
                                productStats.setSpu_name(jsonObject.getString("SPU_NAME"));
                            }

                            @Override
                            public String getKey(ProductStats productStats) {
                                return String.valueOf(productStats.getSpu_id());
                            }
                        }, 60, TimeUnit.SECONDS);

        //10.3 和品类维度进行关联
        SingleOutputStreamOperator<ProductStats> productStatsWithCategory3DS =
                AsyncDataStream.unorderedWait(productStatsWithSpuDS,
                        new DimAsyncFunction<ProductStats>("DIM_BASE_CATEGORY3") {
                            @Override
                            public void join(ProductStats productStats, JSONObject jsonObject) throws Exception {
                                productStats.setCategory3_name(jsonObject.getString("NAME"));
                            }

                            @Override
                            public String getKey(ProductStats productStats) {
                                return String.valueOf(productStats.getCategory3_id());
                            }
                        }, 60, TimeUnit.SECONDS);

        //10.4 和品牌维度进行关联
        SingleOutputStreamOperator<ProductStats> productStatsWithTmDS =
                AsyncDataStream.unorderedWait(productStatsWithCategory3DS,
                        new DimAsyncFunction<ProductStats>("DIM_BASE_TRADEMARK") {
                            @Override
                            public void join(ProductStats productStats, JSONObject jsonObject) throws Exception {
                                productStats.setTm_name(jsonObject.getString("TM_NAME"));
                            }

                            @Override
                            public String getKey(ProductStats productStats) {
                                return String.valueOf(productStats.getTm_id());
                            }
                        }, 60, TimeUnit.SECONDS);

        productStatsWithTmDS.print(">>>>>");

        //TODO 11.将聚合的结果写到OLAP数据库  clickhouse
        productStatsWithTmDS.addSink(
                ClickHouseUtil.getSinkFunction("insert into product_stats_0428 values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)")
        );
        env.execute();
    }
}
