package com.atguigu.gmall.realtime.app.func;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.ververica.cdc.debezium.DebeziumDeserializationSchema;
import io.debezium.data.Envelope;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import java.util.List;

public class MyCustomSchema implements DebeziumDeserializationSchema<String> {


    @Override
    public void deserialize(SourceRecord sourceRecord, Collector<String> collector) throws Exception {
        Struct valueStruct = (Struct)sourceRecord.value();
        Struct sourceStruct = valueStruct.getStruct("source");

        //获取数据库名
        String database = sourceStruct.getString("db");
        //获取表名
        String table = sourceStruct.getString("table");


        //获取类型
        //Envelope类中有一个内部静态枚举类
            /*
            public static enum Operation {
                               READ("r"),
                               CREATE("c"),
                               UPDATE("u"),
                               DELETE("d");
             */
        String type = Envelope.operationFor(sourceRecord).toString().toLowerCase();
        if("create".equals(type)){
            type = "insert";
        }

        Struct afterStruct = valueStruct.getStruct("after");
        JSONObject datajsonObj = new JSONObject();

        if(afterStruct != null){
            List<Field> fieldList = afterStruct.schema().fields();

            for (Field field : fieldList) {
                String fieldName = field.name();
                Object fieldValue = afterStruct.get(field);
                datajsonObj.put(fieldName,fieldValue);
            }

        }

        JSONObject resJsonObj = new JSONObject();
        resJsonObj.put("database",database);
        resJsonObj.put("table",table);
        resJsonObj.put("type",type);
        resJsonObj.put("data",datajsonObj);

        collector.collect(resJsonObj.toJSONString());

    }

    @Override
    public TypeInformation<String> getProducedType() {
        return TypeInformation.of(String.class);
    }
}

