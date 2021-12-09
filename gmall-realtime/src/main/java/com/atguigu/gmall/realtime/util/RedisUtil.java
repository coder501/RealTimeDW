package com.atguigu.gmall.realtime.util;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

public class RedisUtil {
   private static JedisPool jedisPool;

    public static void initJedisPool(){

        JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
        //设置最大可用连接数
        jedisPoolConfig.setMaxTotal(1000);
        //最大闲置连接数
        jedisPoolConfig.setMaxIdle(5);
        //最小连接数
        jedisPoolConfig.setMinIdle(5);
        //连接耗尽时是否等待
        jedisPoolConfig.setBlockWhenExhausted(true);
        //最长等待时间
        jedisPoolConfig.setMaxWaitMillis(2000L);
        //获取连接时 是否先测试以下
        jedisPoolConfig.setTestOnBorrow(true);
        jedisPool = new JedisPool(jedisPoolConfig, "hadoop102", 6379,1000);
    }

    public static Jedis getJedis(){
        if(jedisPool == null){
            initJedisPool();
        }

        return jedisPool.getResource();
    }

    //测试
    public static void main(String[] args) {
        Jedis jedis = RedisUtil.getJedis();
        String pong = jedis.ping();
        System.out.println(pong);
    }

}
