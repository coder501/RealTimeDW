package com.atguigu.gmall.realtime.app.func;



import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.util.DimUtil;
import com.atguigu.gmall.realtime.util.ThreadPoolUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.ExecutorService;

public abstract class DimAsyncFunction<T> extends RichAsyncFunction<T,T> implements DimJoinFunction<T> {

    private ExecutorService executorService;

    private String tableName;

    public DimAsyncFunction(String tableName) {
        this.tableName = tableName;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        //super.open(parameters);

        executorService = ThreadPoolUtil.getInstance();
    }

    @Override
    public void asyncInvoke(T obj, ResultFuture<T> resultFuture) throws Exception {
        executorService.submit(new Runnable() {
            @Override
            public void run() {
                try {
                    long start = System.currentTimeMillis();
                    String key = getKey(obj);
                    JSONObject dimJsonObj = DimUtil.getDimInfo(tableName, key);
                    if(dimJsonObj != null){
                        //维度数据和流数据相关联
                        //System.out.println("维表数据为 " + dimJsonObj.toJSONString());
                        join(obj,dimJsonObj);
                    }
                    long end = System.currentTimeMillis();
                    //System.out.println("异步耗时:" + (end - start) + "毫秒");
                    resultFuture.complete(Collections.singleton(obj));
                } catch (Exception e) {
                    System.out.println(String.format(tableName + "异步查询异常 %s",e));
                    e.printStackTrace();
                }
            }
        });
    }
}
