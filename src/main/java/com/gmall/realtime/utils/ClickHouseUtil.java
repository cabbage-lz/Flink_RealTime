package com.gmall.realtime.utils;

import com.gmall.realtime.beans.TransientSink;
import com.gmall.realtime.common.GmallConfig;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.lang.reflect.Field;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * description:ClickHouseUtil
 * Created by thinkpad on 2021-10-15
 */
public class ClickHouseUtil {

    public static <T> SinkFunction getSinkFunction(String sql) {
        SinkFunction<T> sinkFunction = JdbcSink.<T>sink(
//                "insert into  表（a,b,c）values('？','？','？')",
                sql,
                new JdbcStatementBuilder<T>() {
                    @Override
                    public void accept(PreparedStatement ps, T obj) throws SQLException {
//给？占位符赋值，将流中处理的对象属性赋值给占位符    通过反射   getDeclaredFields:只会获取当前类的属性
                        Field[] fieldArr = obj.getClass().getDeclaredFields();
//                        skipNum 字段偏移量
                        int skipNum = 0;
                        for (int i = 0; i < fieldArr.length; i++) {
                            Field field = fieldArr[i];
//                            判断是否需要保存到ClickHouse中    //通过反射获得字段上的注解
                            TransientSink transientSink = field.getAnnotation(TransientSink.class);
                            if (transientSink != null) {
                                skipNum++;
                                continue;
                            }

//                            设置访问权限
                            field.setAccessible(true);
//                            获取属性值
                            try {
//                                获取属性值
                                Object fieldValue = field.get(obj);
//                                将属性值赋值给问号占位符
                                ps.setObject(i + 1 - skipNum, fieldValue);
                            } catch (Exception e) {
                                e.printStackTrace();
                            }

                        }

                    }
                },
                new JdbcExecutionOptions.Builder()
                        .withBatchSize(5)
                        .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withDriverName("ru.yandex.clickhouse.ClickHouseDriver")
                        .withUrl(GmallConfig.CLICKHOUSE_URL)
                        .build()
        );
        return sinkFunction;
    }
//
//    public static <T>SinkFunction getsin(String sql){
//
//        SinkFunction<T> sink = JdbcSink.<T>sink(
//                sql,
//                new JdbcStatementBuilder<T>() {
//                    @Override
//                    public void accept(PreparedStatement ps, T obj) throws SQLException {
//                        Field[] fieldArr = obj.getClass().getDeclaredFields();
//                        for (int i = 0; i < fieldArr.length; i++) {
//                            Field field = fieldArr[i];
////                            设置访问权限
//                            field.setAccessible(true);
////                            获取属性值
//                            try {
//                                Object fieldValue = field.get(obj);
////                                将属性的值赋值给问号占位符
//                                ps.setObject(i+1,fieldValue);
//
//                            } catch (Exception e) {
//                                e.printStackTrace();
//                            }
//                        }
//                        //给问号赋值
//                    }
//                },
//                new JdbcExecutionOptions.Builder()
//                        .withBatchSize(5)
//                        .withBatchIntervalMs(3)
//                        .build(),
//                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
//                        .withDriverName("ru.yandex.clickhouse.ClickHouseDriver")
//                        .withConnectionCheckTimeoutSeconds()
//                        .build()
//
//
//        );
//    }

}
