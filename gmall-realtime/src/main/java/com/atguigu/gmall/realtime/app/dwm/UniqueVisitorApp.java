package com.atguigu.gmall.realtime.app.dwm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONAware;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.util.MyKafkaUtil;
import org.apache.commons.io.filefilter.FalseFileFilter;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.text.SimpleDateFormat;


public class UniqueVisitorApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(60000L);
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3,3000L));
        env.setStateBackend(new FsStateBackend("hdfs://hadoop102:8020/gmall/ck"));
        System.setProperty("HADOOP_USER_NAME","atguigu");

        //3.从kafka消费数据
        String topic = "dwd_page_info";
        String groupId = "page_info_group_id";

        FlinkKafkaConsumer<String> kafkaConsumer = MyKafkaUtil.getKafkaConsumer(topic, groupId);

        DataStreamSource<String> kafkaDS = env.addSource(kafkaConsumer);

        //4.转换数据类型
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.map(new RichMapFunction<String, JSONObject>() {
            @Override
            public JSONObject map(String value) throws Exception {
                return JSON.parseObject(value);
            }
        });

        //5.对转换后的数据按照mid分组
        KeyedStream<JSONObject, String> midDS = jsonObjDS.keyBy(jsonObj -> jsonObj.getJSONObject("common").getString("mid"));

        //6.对页面数据进行过滤计算独立访客数
        SingleOutputStreamOperator<JSONObject> uniqueVisitorDS = midDS.filter(new RichFilterFunction<JSONObject>() {
            private ValueState<String> lastLoginDate;

            private SimpleDateFormat sdf;

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                //设置状态过期时间时需要先创建一个StateTtkConfig配置对象
                //然后调用状态描述符的enableTimeToLive(stateConfig)方法启用ttl
                StateTtlConfig ttlConfig = StateTtlConfig
                        .newBuilder(Time.days(1))
                        .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                        .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                        .build();


                ValueStateDescriptor<String> stateDescriptor = new ValueStateDescriptor<>("lastLoginDate", Types.STRING);
                //启用ttl
                stateDescriptor.enableTimeToLive(ttlConfig);

                this.lastLoginDate = getRuntimeContext().getState(stateDescriptor);
                sdf = new SimpleDateFormat("yyyyMMdd");
            }

            @Override
            public boolean filter(JSONObject jsonObject) throws Exception {
                String last_page_id = jsonObject.getJSONObject("page").getString("last_page_id");


                if (last_page_id != null) {
                    return false;
                }

                String lastloginDate = this.lastLoginDate.value();
                String currDate = sdf.format(jsonObject.getLong("ts"));

                if (lastloginDate != null && lastloginDate.length() > 0 && currDate.equals(lastLoginDate)) {
                    return false;
                } else {
                    lastLoginDate.update(currDate);
                    return true;
                }
            }
        });

        //7.将独立访客数据发送到dwm_unique_visit
        uniqueVisitorDS
                .map(JSONAware::toJSONString)
                .addSink(MyKafkaUtil.getKafkaProducer("dwm_unique_visit"));

        env.execute();
    }
}
