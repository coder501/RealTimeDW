package com.atguigu.gmall.realtime.app.func;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

import com.atguigu.gmall.realtime.bean.TableProcess;
import com.atguigu.gmall.realtime.common.GmallConfig;
import com.nimbusds.jose.JOSEException;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;


public class TableProcessFunction extends BroadcastProcessFunction<JSONObject,String,JSONObject> {

    private MapStateDescriptor<String,TableProcess> mapStateDispatcher;

    private OutputTag<JSONObject> dim_outputTag;

    private Connection conn;

    public TableProcessFunction(MapStateDescriptor<String, TableProcess> mapStateDispatcher, OutputTag<JSONObject> dim_outputTag) {
        this.mapStateDispatcher = mapStateDispatcher;
        this.dim_outputTag = dim_outputTag;

    }


    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
        conn = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
    }

    @Override
    public void processElement(JSONObject jsonObject, ReadOnlyContext readOnlyContext, Collector<JSONObject> collector) throws Exception {




        ReadOnlyBroadcastState<String, TableProcess> broadcastState = readOnlyContext.getBroadcastState(mapStateDispatcher);

        String tableName = jsonObject.getString("table");
        String type = jsonObject.getString("type");

        if (type.equals("bootstrap-insert")) {
            type = "insert";
            jsonObject.put("type", type);
        }



        String key = tableName + ": " + type;

        TableProcess tableProcess = broadcastState.get(key);

        if(tableProcess != null){

            //无论是维度表还是事实表在向下游传递的时候都需要知道目的地
            jsonObject.put("sink_table",tableProcess.getSinkTable());

            String sinkType = tableProcess.getSinkType();

            //无论是事实表还是维度表都要进行字段过滤
            String sinkColumns = tableProcess.getSinkColumns();
            JSONObject dataJsonObj = jsonObject.getJSONObject("data");
            filterColumns(dataJsonObj,sinkColumns);

            if(TableProcess.SINK_TYPE_HBASE.equals(sinkType)){
                readOnlyContext.output(dim_outputTag,jsonObject);
            }else if(TableProcess.SINK_TYPE_KAFKA.equals(sinkType)){
                collector.collect(jsonObject);
            }
        }else{
            System.out.println("No this key in process" + key);
        }

    }

    private void filterColumns(JSONObject dataJsonObj, String sinkColumns) {
        String[] fieldArr = sinkColumns.split(",");
        List<String> fieldList = Arrays.asList(fieldArr);

        Set<Map.Entry<String, Object>> entries = dataJsonObj.entrySet();
        entries.removeIf(entry -> !fieldList.contains(entry.getKey()));
    }

    @Override
    public void processBroadcastElement(String s, Context context, Collector<JSONObject> collector) throws Exception {
        JSONObject processObj = JSON.parseObject(s);

        JSONObject dataJsonObj = processObj.getJSONObject("data");

        TableProcess tableProcess = dataJsonObj.toJavaObject(TableProcess.class);

        String sourceTable = tableProcess.getSourceTable();

        String operateType = tableProcess.getOperateType();

        String sinkColumns = tableProcess.getSinkColumns();

        String extend = tableProcess.getSinkExtend();

        String pk = tableProcess.getSinkPk();

        String sinkType = tableProcess.getSinkType();

        String sinkTable = tableProcess.getSinkTable();

        String key = sourceTable + ": " + operateType;

        if(TableProcess.SINK_TYPE_HBASE.equals(sinkType)){
            checkTable(sinkTable,sinkColumns,pk,extend);
        }

        context.getBroadcastState(mapStateDispatcher).put(key, tableProcess);
    }

    private void checkTable(String tableName, String fields, String pk, String ext) {
        if(pk == null){
            pk = "id";
        }
        if(ext == null){
            ext = "";
        }

        StringBuilder createTableSql = new StringBuilder();
        createTableSql.append("CREATE TABLE IF NOT EXISTS " + GmallConfig.HBASE_SCHEMA + "." + tableName + "(\n");
        String[] fieldArr = fields.split(",");

        for(int i = 0;i<fieldArr.length;i++){
            String field = fieldArr[i];
            if(pk.equals(field)){
                createTableSql.append(field).append(" varchar primary key");
            }else {
                createTableSql.append(field).append(" varchar");
            }

            if(i != fieldArr.length -1){
                createTableSql.append(",\n");
            }

        }

        createTableSql.append(")");
        createTableSql.append(ext);
        System.out.println("Phoenix的建表语句: " + createTableSql);
        //jdbc操作
        PreparedStatement ps = null;

        try {
            ps = conn.prepareStatement(createTableSql.toString());

            ps.execute();
        } catch (SQLException e) {
            e.printStackTrace();
            throw  new RuntimeException("在phoenix中创建维度表失败");
        }finally {
            if(ps != null){
                try {
                    ps.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }




    }
}
