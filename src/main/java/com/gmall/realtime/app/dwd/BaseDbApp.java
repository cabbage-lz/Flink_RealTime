package com.gmall.realtime.app.dwd;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;
import com.gmall.realtime.app.func.DimDink;
import com.gmall.realtime.app.func.MyDeserializationSchemaFunction;
import com.gmall.realtime.app.func.TableProcessFunction;
import com.gmall.realtime.beans.TableProcess;
import com.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.flink.util.OutputTag;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;

/**
 * description:TODO 业务数据动态分流
 * 读取业务数据、清洗、分流
 * Created by thinkpad on 2021-10-08
 * * 业务数据动态分流测试
 * *  -需要启动的进程
 * *      zk、kafka、maxwell、hdfs、hbase、BaseDBApp
 * *  -执行流程
 * *      业务数据库表发生变化
 * *      变化会记录到binlog中
 * *      maxwell从binlog中读取变化，并封装为json格式字符串，发送到kafka的ods_base_db_m主题中
 * *      BaseDBApp从ods_base_db_m中读取数据，并对数据进行ETL
 * *      BaseDBApp从配置表中读取配置信息(使用FlinkCDC读取配置表)
 * *      BaseDBApp中会将读取的配置信息转换为广播流，并和业务主流进行连接
 * *      连接之后，分别对两条流数据进行处理
 * *          >processElement
 * *              获取状态
 * *              从业务流的json对象中获取操作的业务数据的表名以及类型
 * *              将表名和类型封装为key，到状态中获取对应的配置信息
 * *              字段过滤
 * *              根据配置信息判断是维度还是事实
 * *                  维度----维度侧输出流
 * *                  事实----主流
 * *          >processBroadcastElement
 * *              获取配置信息，封装为TableProcess对象
 * *              判断是不是维度表配置，如果是维度表配置，提前建表
 * *              获取状态，将封装的对象放到状态中保存  ，key是配置表中配置的 表名 + 操作类型
 * *
 */
public class BaseDbApp {
    public static void main(String[] args) throws Exception {
        //TODO 1.基本环境准备
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        //TODO 2.检查点相关的配置
//        //2.1 开启检查点
//        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);
//        //2.2 设置检查点超时时间
//        env.getCheckpointConfig().setCheckpointTimeout(60000L);
//        //2.3 设置取消job的时候，是否保留检查点
//        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
//        //2.4 设置重启策略
//        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3,3000L));
//        //2.5 设置状态后端
//        env.setStateBackend(new FsStateBackend("hdfs://hadoop102:8020/xx"));
//        //2.6 设置操作hdfs的用户
//        System.setProperty("HADOOP_USER_NAME","atguigu");
        //TODO 3.从kafka主题中读取数据
        //3.1 声明消费的主题以及消费者组
        String topic = "ods_base_db_m";
        String groupId = "base_db_app_group";
        //3.2 创建消费者对象
        FlinkKafkaConsumer<String> kafkaSource = MyKafkaUtil.getKafkaSource(topic, groupId);
        //3.3 消费数据  封装为流
        DataStreamSource<String> kafkaDS = env.addSource(kafkaSource);
        //TODO 4.对流中的数据类型进行转换  jsonStr->jsonObj
//        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.map(JSON::parseObject);
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.map(
                new MapFunction<String, JSONObject>() {
                    @Override
                    public JSONObject map(String value) throws Exception {
                        return JSON.parseObject(value);
                    }
                }
        );
        //TODO 5.简单的ETL
        SingleOutputStreamOperator<JSONObject> filterDS = jsonObjDS.filter(
                new FilterFunction<JSONObject>() {
                    @Override
                    public boolean filter(JSONObject jsonObj) throws Exception {
                        boolean flag =
                                jsonObj.getString("table") != null
                                        && jsonObj.getString("table").length() > 0
                                        && jsonObj.getJSONObject("data") != null
                                        && jsonObj.getString("data").length() > 3;
                        return flag;
                    }
                }
        );

//        filterDS.print("===>");


        //TODO 6.使用FlinkCDC读取MySQL中的配置表。一旦binlog有变化FlinkCDC就会读取MySQL中的配置表
//6.1 获取MySQLSourceFunction
        DebeziumSourceFunction<String> sourceFunction = MySQLSource.<String>builder()
                .hostname("hadoop102")
                .port(3306)
                .username("root")
                .password("123456")
                .databaseList("gmall0428_realtime")
                .tableList("gmall0428_realtime.table_process")
                .deserializer(new MyDeserializationSchemaFunction())
                .startupOptions(StartupOptions.initial())
                .build();

        //6.2 读取数据  封装为流（TODO 配置流）
        DataStreamSource<String> mysqlDS = env.addSource(sourceFunction);
        //TODO 7. 将读取到的配置信息进行广播
        //7.1 定义广播状态描述器
        MapStateDescriptor<String, TableProcess> mapStateDescriptor = new MapStateDescriptor<>("table-process", String.class, TableProcess.class);
        //7.2 定义广播流
        BroadcastStream<String> broadcastStream = mysqlDS.broadcast(mapStateDescriptor);
        //TODO 8.将主流和广播流合并在一起  进行动态分流
        //8.1连接两条流
        BroadcastConnectedStream<JSONObject, String> connectDS = filterDS.connect(broadcastStream);
       //8.2 对连接之后的流 进行处理---分流   事实数据---主流         维度数据---侧输出流
        OutputTag<JSONObject> dimTag = new OutputTag<JSONObject>("dimTag") {
        };   //TODO 定义侧输出流标签


        SingleOutputStreamOperator<JSONObject> realDS = connectDS.process(
                new TableProcessFunction(dimTag, mapStateDescriptor)
        );

        // 8.3 获取维度流数据
        DataStream<JSONObject> dimDS = realDS.getSideOutput(dimTag);

        realDS.print(">>");
        dimDS.print("##");

        //TODO 9.将维度侧输出流数据写到Hbase中
        dimDS.addSink(new DimDink());

        //TODO 10.将主流业务数据写到kafka的dwd主题中
//        所有的数据发送到不同的主题中去,通过自定义序列化方式创建kafka生产者
        realDS.addSink(
//流中数据拿到获得一个kafkaproducer，传递一个参数，通过匿名类对发送的消息进行序列化，序列化成ProducerRecord
                MyKafkaUtil.getKafkaSinkBySchema(new KafkaSerializationSchema<JSONObject>() {
                    @Override
                    public ProducerRecord<byte[], byte[]> serialize(JSONObject jsonObj, @Nullable Long timestamp) {
                        String topic = jsonObj.getString("sink_table");
                        return new ProducerRecord<byte[], byte[]>(topic, jsonObj.getJSONObject("data").toJSONString().getBytes());
                    }
                })
        );
        env.execute();
    }
}
