package com.atguigu.gmall.realtime.app.func;


import com.alibaba.fastjson.JSONObject;

/**
 * 维度关联查询的接口
 */
public interface DimJoinFunction<T> {
    /**
     * 需要实现如何从流对象中获取key
     * @param obj  数据流对象
     * @return
     */
    String getKey(T obj);


    /**
     *需要实现如何把结果装配给数据流对象
     * @param obj  数据流对象
     * @param jsonObj  异步查询结果
     */
    void join(T obj, JSONObject jsonObj) throws Exception;


}
