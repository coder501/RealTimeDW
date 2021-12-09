package com.atguigu.cdc;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.alibaba.ververica.cdc.debezium.DebeziumDeserializationSchema;

import io.debezium.data.Envelope;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import java.util.List;

public class FlinkCDC03_Custom {

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);


        MySQLSource.<String>builder()
                .hostname("hadoop102")
                .port(3306)
                .databaseList("gmall_cdc")
                .tableList("gmall_cdc.Student")
                .username("root")
                .password("123456")
                .startupOptions(StartupOptions.initial())
                .deserializer(new MyCustomSchema())
                .build();
    }



    //自定义反序列化类实现DebeziumDeserializationSchema<T>接口  实现deserialize和getProducedType方法
    public static class MyCustomSchema implements DebeziumDeserializationSchema<String>{

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
}
