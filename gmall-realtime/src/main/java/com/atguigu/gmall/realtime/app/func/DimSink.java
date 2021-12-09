package com.atguigu.gmall.realtime.app.func;


import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.common.GmallConfig;
import com.atguigu.gmall.realtime.util.DimUtil;
import com.atguigu.gmall.realtime.util.RedisUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.ietf.jgss.GSSManager;
import redis.clients.jedis.Jedis;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Set;

public class DimSink extends RichSinkFunction<JSONObject> {

    private Connection conn;






    @Override
    public void open(Configuration parameters) throws Exception {
        //super.open(parameters);
        Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
        conn = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
    }

    @Override
    public void invoke(JSONObject jsonObj, Context context) throws Exception {

        String tableName = jsonObj.getString("sink_table");
        JSONObject dataJsonObj = jsonObj.getJSONObject("data");

        if(dataJsonObj != null && dataJsonObj.size()>0){
            String insertSql = genUpsertSql(tableName.toUpperCase(),dataJsonObj);
            System.out.println("向phoenix中插入数据的sql为: " + insertSql);
            PreparedStatement ps = null;

            try {
                ps = conn.prepareStatement(insertSql);
                ps.executeUpdate();
                conn.commit();
            }catch (SQLException e){
                e.printStackTrace();
                throw new RuntimeException("执行sql语句失败");
            }finally {
                if (ps != null) {
                    ps.close();
                }
            }


        }
        //如果维度数据是更新或者删除操作则删除缓存   缓存是删除而不是更新
        String type = jsonObj.getString("type");

        if( "delete".equals(type) || "update".equals(type)){
            DimUtil.deleteCached(tableName,dataJsonObj.getString("id"));
        }

    }

    private String genUpsertSql(String tableName, JSONObject dataJsonObj) {
        Set<String> keys = dataJsonObj.keySet();
        Collection<Object> values = dataJsonObj.values();



        String UpsertSql = "upsert into " + GmallConfig.HBASE_SCHEMA + "." + tableName
                + " (" + StringUtils.join(keys,",") + ")" + " values ('"
                +StringUtils.join(values,"','") + "')";

        return UpsertSql;

    }
}
