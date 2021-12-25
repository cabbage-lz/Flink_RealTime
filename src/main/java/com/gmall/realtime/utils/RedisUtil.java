package com.gmall.realtime.utils;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

/**
 * description:TODO 操作Redis的工具类
 * Created by thinkpad on 2021-10-12
 */
public class RedisUtil {
    private static JedisPool jedisPool;

    //初始化Jedis连接池
    private static void initJedisPool() {
        JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
        jedisPoolConfig.setMaxTotal(100);
        jedisPoolConfig.setMinIdle(5);
        jedisPoolConfig.setMaxIdle(5);
        jedisPoolConfig.setBlockWhenExhausted(true);
        jedisPoolConfig.setMaxWaitMillis(2000);
        jedisPoolConfig.setTestOnBorrow(true);
        jedisPool = new JedisPool(jedisPoolConfig, "hadoop102", 6379, 10000);
    }

    //获取Jedis客户端
    public static Jedis getJedis() {
        if (jedisPool == null) {
            initJedisPool();
            System.out.println("开辟连接池");
        }
        Jedis jedis = jedisPool.getResource();
        return jedis;
    }


    public static void main(String[] args) {
        Jedis jedis = getJedis();
        String pong = jedis.ping();
        System.out.println(pong);
    }
}
