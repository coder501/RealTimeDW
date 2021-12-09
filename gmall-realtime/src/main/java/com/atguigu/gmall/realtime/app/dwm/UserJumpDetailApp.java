package com.atguigu.gmall.realtime.app.dwm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.app.func.MyPatternProcessFunction;
import com.atguigu.gmall.realtime.util.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.OutputTag;

import java.time.Duration;

/**
 * 使用Flink CEP 处理用户跳出问题
 */

public class UserJumpDetailApp {
    public static void main(String[] args) throws Exception {
        //TODO 1.设置流处理环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        //TODO 2.设置检查点
        /*env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(60000L);
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 3000L));
        env.setStateBackend(new FsStateBackend("hdfs://hadoop102:8020/gmall/ck"));
        System.setProperty("HADOOP_USER_NAME", "atguigu");*/

        //TODO 3.从kafka  dwd_page_log主题 读取数据
        String topic = "dwd_page_log";
        String groupId = "first_page_jump";
        FlinkKafkaConsumer<String> kafkaConsumer = MyKafkaUtil.getKafkaConsumer(topic, groupId);
        //DataStreamSource<String> kafkaDS = env.addSource(kafkaConsumer);

        DataStream<String> kafkaDS = env
                .fromElements(
                        "{\"common\":{\"mid\":\"101\"},\"page\":{\"page_id\":\"home\"},\"ts\":10000} ",
                        "{\"common\":{\"mid\":\"102\"},\"page\":{\"page_id\":\"home\"},\"ts\":12000}",
                        "{\"common\":{\"mid\":\"102\"},\"page\":{\"page_id\":\"good_list\",\"last_page_id\":" +
                                "\"home\"},\"ts\":15000} ",
                        "{\"common\":{\"mid\":\"102\"},\"page\":{\"page_id\":\"good_list\",\"last_page_id\":" +
                                "\"detail\"},\"ts\":30000} "
                );

        //TODO 4.将流中的数据类型转换为JSONObject
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.map(JSON::parseObject);

        //TODO 5.设置watermark和提取事件时间字段
        SingleOutputStreamOperator<JSONObject> jsonObjWithWatermarkDS = jsonObjDS.assignTimestampsAndWatermarks(
                WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                        .withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
                            @Override
                            public long extractTimestamp(JSONObject jsonObj, long recordTimestamp) {
                                return jsonObj.getLong("ts");
                            }
                        })
        );


        //TODO 6.根据mid字段进行分组
        KeyedStream<JSONObject, String> jsonObjectKeyedStream = jsonObjWithWatermarkDS.keyBy(jsonObj -> jsonObj.getJSONObject("common").getString("mid"));

        //TODO 7.设置CEP匹配规则
        Pattern<JSONObject, JSONObject> pattern = Pattern.<JSONObject>begin("first").where(
                new SimpleCondition<JSONObject>() {
                    @Override
                    public boolean filter(JSONObject jsonObj) {
                        String last_page_id = jsonObj.getJSONObject("page").getString("last_page_id");
                        return last_page_id == null;
                    }
                }
        ).next("next").where(
                new SimpleCondition<JSONObject>() {
                    @Override
                    public boolean filter(JSONObject jsonObj) {
                        String pageId = jsonObj.getJSONObject("page").getString("page_id");
                        if(pageId != null && pageId.length() > 0){
                            return true;
                        }else{
                            return false;
                        }
                    }
                }
        ).within(Time.seconds(10));

        //TODO 8.将 匹配规则应用到流上
        PatternStream<JSONObject> patternStream = CEP.pattern(jsonObjectKeyedStream, pattern);

        //TODO 9. 设置 超时事件的侧输出流标签
        OutputTag<String> outputTag = new OutputTag<String>("timeOutTag") {
        };

        //TODO 10.  自定义PatternProcessFunction 实现TimedOutPartialMatchHandler接口 处理超时事件
        SingleOutputStreamOperator<String> resDS = patternStream.process(new MyPatternProcessFunction(outputTag));

        //TODO 11. 从结果流的侧输出流中得到超时事件
        DataStream<String> timeOutDS = resDS.getSideOutput(outputTag);

        //timeOutDS.print();
        //TODO 12.  将结果写到kafka dwm_user_jump_detail主题
        timeOutDS.addSink(MyKafkaUtil.getKafkaProducer("dwm_user_jump_detail"));

        env.execute();
    }
}


