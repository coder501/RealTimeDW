package com.atguigu.gmall.realtime.app.dwd;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;
import com.atguigu.gmall.realtime.app.func.DimSink;
import com.atguigu.gmall.realtime.app.func.MyCustomSchema;
import com.atguigu.gmall.realtime.app.func.TableProcessFunction;
import com.atguigu.gmall.realtime.bean.TableProcess;
import com.atguigu.gmall.realtime.util.MyKafkaUtil;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.util.Properties;

public class BasedbApp {
    public static void main(String[] args) throws Exception {
        //TODO 1.?????????????????????
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        //TODO 2.?????????????????????
        /*env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE); //??????????????????????????????
        env.getCheckpointConfig().setCheckpointTimeout(60000L);
        //env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3,3000L));
        env.setStateBackend(new FsStateBackend("hdfs://hadoop102:8020/gmall/ck"));
        System.setProperty("HADOOP_USER_NAME","atguigu");*/

       //TODO 3.???kafka ods_base_db_m ????????????
       String topic = "ods_base_db_m";
       String groupId = "base_db_m_group_id";

       FlinkKafkaConsumer<String> kafkaSource = MyKafkaUtil.getKafkaConsumer(topic, groupId);

        DataStreamSource<String> kafkaDS = env.addSource(kafkaSource);

        //TODO 4.???json??????????????????jsonObj
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.map(new MapFunction<String, JSONObject>() {
            @Override
            public JSONObject map(String s) throws Exception {
                return JSON.parseObject(s);
            }
        });

        //TODO 5.ETL ??????????????????
        SingleOutputStreamOperator<JSONObject> etlDS = jsonObjDS.filter(new FilterFunction<JSONObject>() {
            @Override
            public boolean filter(JSONObject jsonObject) throws Exception {
                boolean flag = jsonObject.getString("table") != null &&
                        jsonObject.getString("table").length() > 0 &&
                        jsonObject.getJSONObject("data") != null &&
                        jsonObject.getString("data").length() > 3;

                return flag;
            }
        });


        //TODO 6.1??????flinkCDC???????????????
        DebeziumSourceFunction<String> sourceFunction = MySQLSource.<String>builder()
                .hostname("hadoop102")
                .port(3306)
                .databaseList("gmall_cdc")
                .tableList("gmall_cdc.table_process")
                .username("root")
                .password("123456")
                .startupOptions(StartupOptions.initial())  //??????????????????????????? ????????????binlog?????????
                .deserializer(new MyCustomSchema())
                .build();
        //TODO 6.2 ???DebeziumSourceFunction???????????????????????????????????????
        DataStreamSource<String> tableProcessDS = env.addSource(sourceFunction);

        //TODO 6.3 ??????mapstate?????????  ?????????????????????map??????
        MapStateDescriptor<String, TableProcess> mapStateDispatcher = new MapStateDescriptor<>(
                "tableprocess_broadcastState",
                Types.STRING,
                Types.POJO(TableProcess.class)
        );

        //TODO 6.4 ??????????????????????????????
        BroadcastStream<String> configDS = tableProcessDS.broadcast(mapStateDispatcher);
        //TODO 6.5 ????????????????????????????????????
        OutputTag<JSONObject> dim_outputTag = new OutputTag<JSONObject>("dim"){};

        //TODO 7.???etlDS??????????????????????????????
        SingleOutputStreamOperator<JSONObject> realDS = etlDS
                .connect(configDS)
                .process(new TableProcessFunction(mapStateDispatcher,dim_outputTag));

        //TODO 8. ????????????????????????????????????
        DataStream<JSONObject> dimDS = realDS.getSideOutput(dim_outputTag);

      /*  realDS.print("?????????");
        dimDS.print("?????????");*/

        //TODO 9.???????????????????????????phoenix??????????????????????????????sinkFunction
        dimDS.addSink(new DimSink());

        //TODO 10.???????????????????????????kafka?????????????????? ???????????????KafkaSerializationSchema<T>?????? ??????serialize????????????produceRecord
        realDS.addSink(
                MyKafkaUtil.getKafkaProduceWithSchema(new KafkaSerializationSchema<JSONObject>() {
                    @Override
                    public ProducerRecord<byte[], byte[]> serialize(JSONObject jsonObject, @Nullable Long aLong) {
                        //String topic ="dwd_" + jsonObject.getString("table");
                        String topic = jsonObject.getString("sink_table");
                        JSONObject dataJsonObj = jsonObject.getJSONObject("data");
                        return new ProducerRecord<>(topic,dataJsonObj.toJSONString().getBytes());
                    }
                })
        );




        env.execute();

    }
}
