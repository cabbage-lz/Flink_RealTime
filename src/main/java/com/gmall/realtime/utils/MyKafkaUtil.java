package com.gmall.realtime.utils;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.util.Properties;

/**
 * description:TODO 操作kafka的工具类
 * Created by thinkpad on 2021-09-29
 */
public class MyKafkaUtil {
    private static final String KAFKA_SERVER = "hadoop102:9092,hadoop103:9092,hadoop104:9092";
    private static final String DEFAULT_TOPIC = "default_topic";

    //    TODO 获取KafkaSource--消费者
    public static FlinkKafkaConsumer<String> getKafkaSource(String topic, String groupId) {
        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_SERVER);
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        FlinkKafkaConsumer<String> flinkKafkaConsumer = new FlinkKafkaConsumer<>(topic, new SimpleStringSchema(), props);
        return flinkKafkaConsumer;
    }

    //    TODO 获取KafkaSink--生产者
    public static FlinkKafkaProducer<String> getKafkaSink(String topic) {
        return new FlinkKafkaProducer<String>(KAFKA_SERVER, topic, new SimpleStringSchema());
    }

    //拼接Kafka相关属性到DDL
    public static String getKafkaDDL(String topic, String groupId) {
        String ddl = "'connector' = 'kafka', " +
                " 'topic' = '" + topic + "'," +
                " 'properties.bootstrap.servers' = '" + KAFKA_SERVER + "', " +
                " 'properties.group.id' = '" + groupId + "', " +
                "  'format' = 'json', " +
                "  'scan.startup.mode' = 'latest-offset'  ";
        return ddl;
    }

    //保证精准一次性
//    public static FlinkKafkaProducer<String> getKafkaSink(String topic) {
//        Properties props = new Properties();
//        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_SERVER);
//        props.setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, 1000 * 60 * 15 + "");
//        return new FlinkKafkaProducer<String>(DEFAULT_TOPIC, new KafkaSerializationSchema<String>() {
//            @Override
//            public ProducerRecord<byte[], byte[]> serialize(String element, @Nullable Long timestamp) {
//                return new ProducerRecord<byte[], byte[]>(topic, element.getBytes());
//            }
//        }, props, FlinkKafkaProducer.Semantic.EXACTLY_ONCE);
//    }

    //获取kafkaSink---生产者  TODO 向kafka主题中发送数据不确定数据类型   T：泛型模板。在静态方法前声明   <T>（泛型模板） FlinkKafkaProducer<T>（使用泛型）
    public static <T> FlinkKafkaProducer<T> getKafkaSinkBySchema(KafkaSerializationSchema<T> kafkaSerializationSchema) {
        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_SERVER);
        props.setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, 1000 * 60 * 15 + "");
//
        return new FlinkKafkaProducer<T>(DEFAULT_TOPIC, kafkaSerializationSchema, props, FlinkKafkaProducer.Semantic.EXACTLY_ONCE);
    }
}
