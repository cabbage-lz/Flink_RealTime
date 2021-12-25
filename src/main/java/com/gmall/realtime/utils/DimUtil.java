package com.gmall.realtime.utils;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.java.tuple.Tuple2;
import redis.clients.jedis.Jedis;

import java.util.List;

/**
 * description:
 * Created by thinkpad on 2021-10-12
 */
public class DimUtil {
    //    TODO 小优化，直接传id
    public static JSONObject getDimInfo(String tableName, String id) throws Exception {
        return getDimInfo(tableName, Tuple2.of("id", id));
    }

    //        return getDimInfo(tableName,Tuple2.of("id",id));
//    }
    //TODO 优化：加入了旁路缓存  先从Redis中查询，如果查询到了，直接将查询结果返回；
    //TODO 如果没有查询到，再发送请求到Phoenix维度表中插入，并且将查询到的维度数据保存到缓存中
    //type:String       ttl:1day                拼接主键：key: dim:维度表表名:主键1_主键2,   通过主键去查
    public static JSONObject getDimInfo(String tableName, Tuple2<String, String>... columnNameAndValues) {

//        拼接查询维度的sql
        StringBuilder selectSql = new StringBuilder("select * from " + tableName + " where ");
//    拼接查询的主键
        StringBuilder redisKey = new StringBuilder("dim:" + tableName.toLowerCase() + ":");

//        对查询条件进行遍历
        for (int i = 0; i < columnNameAndValues.length; i++) {
            Tuple2<String, String> columnNameAndValue = columnNameAndValues[i];
            String columnName = columnNameAndValue.f0;
            String columnValue = columnNameAndValue.f1;
            selectSql.append(columnName + "='" + columnValue + "'");
            redisKey.append(columnValue);

            if (i < columnNameAndValues.length - 1) {
                selectSql.append(" and ");
                redisKey.append("_");
            }
        }

//    TODO 创建操作Redis的类，从Redis中获取数据
        Jedis jedis = null;    //操作redis客户端
        String dimJsonStr = null;  //封装从redis查询到的字符串
        JSONObject dimJsonObj = null;//最终返回的结果
        try {
            jedis = RedisUtil.getJedis();
            dimJsonStr = jedis.get(redisKey.toString());   //从redis中查数据
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("从Redis中查询维度数据发生异常");
        }

        if (dimJsonStr != null && dimJsonStr.length() > 0) {
//        TODO 从Redis中获取到了数据
            dimJsonObj = JSON.parseObject(dimJsonStr);
        } else {
//        TODO  没有从Redis中查询导数据，发送请求到Phoenix中去查询数据
            System.out.println("从Phoenix中查询维度的的sql:" + selectSql);
//底层还是调用PhoenixUtil查询维度表
            List<JSONObject> dimJsonList = PhoenixUtil.queryList(selectSql.toString(), JSONObject.class);
            if (dimJsonList != null && dimJsonList.size() > 0) {
                dimJsonObj = dimJsonList.get(0);
//                TODO 注意：查询到结果后还要写到redis中     setex：key，设置超时时间，value
                if (jedis != null) {
                    jedis.setex(redisKey.toString(), 3600 * 24, dimJsonObj.toJSONString());
                }
            } else {
                System.out.println("~~维度数据没找到:" + selectSql);
            }
        }
//        TODO 返回结果前，释放资源 释放Jedis
        if (jedis != null) {
            System.out.println("关闭redis");
            jedis.close();
        }
        return dimJsonObj;
    }

    public static JSONObject getDimInfoNoCache(String tableName, Tuple2<String, String>... columnNameAndValues) {
//        拼接查询维度的sql
        StringBuilder selectSql = new StringBuilder("select * from " + tableName + " where ");

//        对查询条件进行遍历
        for (int i = 0; i < columnNameAndValues.length; i++) {
            Tuple2<String, String> columnNameAndValue = columnNameAndValues[i];
            String columnName = columnNameAndValue.f0;
            String columnValue = columnNameAndValue.f1;
            selectSql.append(columnName + "='" + columnValue + "'");
            if (i < columnNameAndValues.length - 1) {
                selectSql.append(" and ");
            }
        }
        System.out.println("从Phoenix中查询维度的的sql:" + selectSql);
////底层还是调用PhoenixUtil查询维度表
        List<JSONObject> dimJsonList = PhoenixUtil.queryList(selectSql.toString(), JSONObject.class);
        JSONObject dimJsonObj = null;
        if (dimJsonList != null && dimJsonList.size() > 0) {
            dimJsonObj = dimJsonList.get(0);
        } else {
            System.out.println("~~维度数据没找到:" + selectSql);
        }
        return dimJsonObj;
    }

    //TODO 清除Redis中缓存数据
    public static void deleteCached(String tableName, String id) {
        String redisKey = "dim:" + tableName.toLowerCase() + ":" + id;
        try {
            Jedis jedis = RedisUtil.getJedis();
            jedis.del(redisKey);
            jedis.close();
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("从Redis中删除缓存发生异常:" + redisKey);
        }
    }

    public static void main(String[] args) throws Exception {
//        JSONObject dimInfo = getDimInfoNoCache("dim_base_trademark", Tuple2.of("id", "18"), Tuple2.of("tm_name", "aaa"));
//        JSONObject dimInfo = getDimInfo("dim_base_trademark", Tuple2.of("id", "18"));
        JSONObject dimInfo = getDimInfo("dim_base_trademark", "18");
        System.out.println(dimInfo);
    }
}