package com.atguigu.gmall.realtime.app.dws;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.bean.VisitorStats;
import com.atguigu.gmall.realtime.util.DateTimeUtil;
import com.atguigu.gmall.realtime.util.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;


import java.time.Duration;
import java.util.Date;

/**
 * 访客主题统计
 */
public class VisitorStatsApp {
    public static void main(String[] args) throws Exception {
        //TODO 1.设置流处理环境和并行度
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        //TODO 2.设置检查点相关
        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(60000L);
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 3000L));
        env.setStateBackend(new FsStateBackend("hdfs://hadoop102:8020/gmall/ck"));
        System.setProperty("HADOOP_USER_NAME", "atguigu");

        //TODO 3.从kafka中读取数据源  dwd_page_log dwm_unique_visit dwm_user_jump_detail
        String pageViewSourceTopic = "dwd_page_log";
        String uniqueVisitorSourceTopic = "dwm_unique_visit";
        String userJumpdetailSourceTopic = "dwm_user_jump_detail";

        String groupId = "visitor_stats_app";
        FlinkKafkaConsumer<String> pageViewSource = MyKafkaUtil.getKafkaConsumer(pageViewSourceTopic, groupId);
        FlinkKafkaConsumer<String> uniqueVisitorSource = MyKafkaUtil.getKafkaConsumer(uniqueVisitorSourceTopic, groupId);
        FlinkKafkaConsumer<String> userJumpdetailSource = MyKafkaUtil.getKafkaConsumer(userJumpdetailSourceTopic, groupId);

        DataStreamSource<String> pageViewDS = env.addSource(pageViewSource);
        DataStreamSource<String> uniqueVisitorDS = env.addSource(uniqueVisitorSource);
        DataStreamSource<String> userJumpdatailDS = env.addSource(userJumpdetailSource);

        //TODO 4.对每条流中的数据类型进行转换  用VisitorStats实体类进行封装
        SingleOutputStreamOperator<VisitorStats> pageLogStatsDS = pageViewDS.map(new MapFunction<String, VisitorStats>() {
            @Override
            public VisitorStats map(String jsonStr) throws Exception {

                JSONObject jsonObj = JSON.parseObject(jsonStr);

                JSONObject commonJsonObj = jsonObj.getJSONObject("common");

                JSONObject pageJsonObj = jsonObj.getJSONObject("page");

                VisitorStats visitorStats = new VisitorStats("", "",
                        commonJsonObj.getString("vc"),
                        commonJsonObj.getString("ch"),
                        commonJsonObj.getString("ar"),
                        commonJsonObj.getString("is_new"),
                        0L,
                        1L,
                        0L,
                        0L,
                        pageJsonObj.getLong("during_time"),
                        jsonObj.getLong("ts")
                );

                String last_page_id = pageJsonObj.getString("last_page_id");

                if (last_page_id != null || last_page_id.length() > 0) {
                    visitorStats.setSv_ct(1L);
                }


                return visitorStats;
            }
        });

        SingleOutputStreamOperator<VisitorStats> uvStatsDS = uniqueVisitorDS.map(new MapFunction<String, VisitorStats>() {
            @Override
            public VisitorStats map(String jsonStr) throws Exception {

                JSONObject jsonObj = JSON.parseObject(jsonStr);

                JSONObject commonJsonObj = jsonObj.getJSONObject("common");
                VisitorStats visitorStats = new VisitorStats("", "",
                        commonJsonObj.getString("vc"),
                        commonJsonObj.getString("ch"),
                        commonJsonObj.getString("ar"),
                        commonJsonObj.getString("is_new"),
                        1L,
                        0L,
                        0L,
                        0L,
                        0L,
                        jsonObj.getLong("ts"));
                return visitorStats;
            }
        });

        SingleOutputStreamOperator<VisitorStats> ujdStatsDS = userJumpdatailDS.map(new MapFunction<String, VisitorStats>() {
            @Override
            public VisitorStats map(String jsonStr) throws Exception {

                JSONObject jsonObj = JSON.parseObject(jsonStr);

                JSONObject commonJsonObj = jsonObj.getJSONObject("common");

                VisitorStats visitorStats = new VisitorStats("", "",
                        commonJsonObj.getString("vc"),
                        commonJsonObj.getString("ch"),
                        commonJsonObj.getString("ar"),
                        commonJsonObj.getString("is_new"),
                        0L,
                        0L,
                        0L,
                        1L,
                        0L,
                        jsonObj.getLong("ts"));

                return visitorStats;
            }
        });

        //TODO 5. 将三条流数据进行合并   union合并的流  数据类型必须一致

        DataStream<VisitorStats> unionDS = pageLogStatsDS.union(
                uvStatsDS,
                ujdStatsDS
        );

        //TODO 6. 指定watermark以及时间戳字段
        SingleOutputStreamOperator<VisitorStats> visitorStatsWithWatermarkDS = unionDS.assignTimestampsAndWatermarks(
                WatermarkStrategy.<VisitorStats>forBoundedOutOfOrderness(Duration.ofSeconds(3L))
                        .withTimestampAssigner(new SerializableTimestampAssigner<VisitorStats>() {
                            @Override
                            public long extractTimestamp(VisitorStats element, long recordTimestamp) {
                                return element.getTs();
                            }
                        })
        );

        //TODO 7. 按照 版本  渠道  地区  新老访客四个维度进行分组  如果按照mid分组  粒度太小达不到聚合的效果

        KeyedStream<VisitorStats, Tuple4<String, String, String, String>> keyedDS = visitorStatsWithWatermarkDS.keyBy(new KeySelector<VisitorStats, Tuple4<String, String, String, String>>() {
            @Override
            public Tuple4<String, String, String, String> getKey(VisitorStats visitorStats) throws Exception {
                return Tuple4.of(visitorStats.getVc(),
                        visitorStats.getCh(),
                        visitorStats.getAr(),
                        visitorStats.getIs_new());
            }
        });

        //TODO 8. 开窗
        WindowedStream<VisitorStats, Tuple4<String, String, String, String>, TimeWindow> windowDS = keyedDS.window(TumblingEventTimeWindows.of(Time.seconds(10)));

        //TODO 9. 聚合

        SingleOutputStreamOperator<VisitorStats> reduceDS = windowDS.reduce(
                new ReduceFunction<VisitorStats>() {
        @Override
        public VisitorStats reduce(VisitorStats stats1, VisitorStats stats2) throws Exception {
            stats1.setPv_ct(stats1.getPv_ct() + stats2.getPv_ct());
            stats1.setSv_ct(stats1.getSv_ct() + stats2.getSv_ct());
            stats1.setUj_ct(stats1.getUj_ct() + stats2.getUj_ct());
            stats1.setUv_ct(stats1.getUv_ct() + stats2.getUv_ct());

            return stats1;
        }
               },
                new WindowFunction<VisitorStats, VisitorStats, Tuple4<String, String, String, String>, TimeWindow>() {
                    @Override
                    public void apply(Tuple4<String, String, String, String> stringStringStringStringTuple4, TimeWindow window, Iterable<VisitorStats> elements, Collector<VisitorStats> out) throws Exception {
                        for (VisitorStats visitorStats : elements) {
                            visitorStats.setStt(DateTimeUtil.toYmdhms(new Date(window.getStart())));
                            visitorStats.setEdt(DateTimeUtil.toYmdhms(new Date(window.getEnd())));
                            visitorStats.setTs(System.currentTimeMillis());
                            out.collect(visitorStats);
                        }
                    }
                }
        );

        env.execute();

    }
}
