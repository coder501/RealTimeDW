package com.atguigu.gmall.realtime.app;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.util.MyKafkaUtil;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.text.SimpleDateFormat;

/*
   实现日志数据分流操作
 */
public class BaseLogApp {


    public static void main(String[] args) throws Exception {

        //1.环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        //2.设置检查点
        //2.1设置检查点保存的频率以及级别
        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);
        //2.2设置检查点的超时时间
        env.getCheckpointConfig().setCheckpointTimeout(60000L);
        //2.3取消job是否保留检查点
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        //2.4设置重启策略
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3,3000L));
        //2.5设置状态后端
        env.setStateBackend(new FsStateBackend("hdfs://hadoop102:8020/gmall/ck"));
        //2.6设置hadoop用户
        System.setProperty("HADOOP_USER_NAME","atguigu");
        //3.从kafka读取日志数据
        String topic = "ods_base_log";
        String group_id = "base_log_group_id";
        FlinkKafkaConsumer<String> kafkaSource = MyKafkaUtil.getKafkaConsumer(topic, group_id);

        DataStreamSource<String> kafkaDs = env.addSource(kafkaSource);
        //4.对读取的数据进行类型转换
        SingleOutputStreamOperator<JSONObject> jsonObjDs = kafkaDs.map(JSON::parseObject);

        //5.新老访客标记修复  --状态编程

        KeyedStream<JSONObject, String> keyedDS = jsonObjDs.keyBy(r -> r.getJSONObject("common").getString("mid"));


        SingleOutputStreamOperator<JSONObject> josnObjWithIsNewDs = keyedDS.map(new RichMapFunction<JSONObject, JSONObject>() {

            private ValueState<String> lastVisitDateState;
            private SimpleDateFormat sdf;

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                lastVisitDateState =
                        getRuntimeContext().getState(new ValueStateDescriptor<String>(
                                "lastVisitDateState",
                                Types.STRING
                        ));

                sdf = new SimpleDateFormat("yyyyMMdd");
            }

            @Override
            public JSONObject map(JSONObject jsonObject) throws Exception {

                String isNew = jsonObject.getJSONObject("common").getString("isNew");
                if ("1".equals(isNew)) {
                    String lastVisitDate = lastVisitDateState.value();

                    String currentVisitDate = sdf.format(jsonObject.getLong("ts"));

                    if (lastVisitDate != null && lastVisitDate.length() > 0) {
                        isNew = "0";
                        if (!currentVisitDate.equals(lastVisitDate)) {
                            jsonObject.getJSONObject("common").put("isNew", isNew);
                        }
                    } else {
                        lastVisitDateState.update(currentVisitDate);
                    }
                }
                return jsonObject;
            }
        });
        //6.日志数据分流  启动日志--启动侧输出流  曝光日志--曝光侧输出流  页面日志--页面侧输出流
        //6.1定义侧输出流标签
        OutputTag<String> startTag = new OutputTag<String>("startTag"){};
        OutputTag<String> displayTag = new OutputTag<String>("displayTag"){};

        SingleOutputStreamOperator<String> pageDs = josnObjWithIsNewDs.process(new ProcessFunction<JSONObject, String>() {
            @Override
            public void processElement(JSONObject jsonObject, Context context, Collector<String> out) throws Exception {
                //从json对象中抽取start
                JSONObject startJsonObj = jsonObject.getJSONObject("start");
                String jsonString = jsonObject.toJSONString();

                if(startJsonObj != null && startJsonObj.size() > 0){
                    //说明是启动日志 则写到startTag侧输出流中
                    context.output(startTag,jsonString);
                }else {
                    out.collect(jsonString);

                    JSONArray displayArr = jsonObject.getJSONArray("displays");

                    if(displayArr != null && displayArr.size() > 0){
                        Long ts = jsonObject.getLong("ts");
                        String page_id = jsonObject.getJSONObject("page").getString("page_id");

                        for(int i = 0;i < displayArr.size();i++){
                            JSONObject displayJsonObj = displayArr.getJSONObject(i);
                            displayJsonObj.put("page_id",page_id);
                            displayJsonObj.put("ts",ts);

                            context.output(displayTag,displayJsonObj.toJSONString());
                        }
                    }
                }



            }
        });

        DataStream<String> startDS = pageDs.getSideOutput(startTag);
        DataStream<String> displayDS = pageDs.getSideOutput(displayTag);

        pageDs.print("page");
        startDS.print("start");
        displayDS.print("displays");


        //7.将不同流中的数据写到kafka的dwd主题中



        env.execute();
    }
}
