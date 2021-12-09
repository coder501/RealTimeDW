package com.atguigu.gmall.realtime.app.dwm;

/*
将order和order_detail进行双流join并和用户，地区，商品，品牌，品类，spu等维度进行关联
 */

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.app.func.DimAsyncFunction;
import com.atguigu.gmall.realtime.bean.OrderDetail;
import com.atguigu.gmall.realtime.bean.OrderInfo;
import com.atguigu.gmall.realtime.bean.OrderWide;
import com.atguigu.gmall.realtime.util.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;


import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Date;
import java.util.concurrent.TimeUnit;

public class OrderWideApp {
    public static void main(String[] args) throws Exception {
        //TODO 1.设置流处理环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        //TODO 2.设置检查点相关
     /*   env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(60000L);
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 3000L));
        env.setStateBackend(new FsStateBackend("hdfs://hadoop102:8020/gmall/ck"));
        System.setProperty("HADOOP_USER_NAME", "atguigu");*/

        //TODO 3.分别从kafka  dwd_order_info 和 dwd_order_detail中读取订单信息和订单详情信息
        String order_topic = "dwd_order_info";
        String order_detail_topic = "dwd_order_detail";
        String groupId = "order_wide_group";
        FlinkKafkaConsumer<String> orderInfoSource = MyKafkaUtil.getKafkaConsumer(order_topic, groupId);
        FlinkKafkaConsumer<String> orderDetailSource = MyKafkaUtil.getKafkaConsumer(order_detail_topic, groupId);

        DataStreamSource<String> orderInfoDS = env.addSource(orderInfoSource);
        DataStreamSource<String> orderDetailDS = env.addSource(orderDetailSource);

        //TODO 4.将jsonStr转换为对应的实体类对象并填充创建时间戳字段   创建时间戳字段作为事件字段使用
        SingleOutputStreamOperator<OrderInfo> orderInfoWithCreatetsDS = orderInfoDS.map(new RichMapFunction<String, OrderInfo>() {

            private SimpleDateFormat sdf;

            @Override
            public void open(Configuration parameters) throws Exception {
                sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

            }

            @Override
            public OrderInfo map(String jsonStr) throws Exception {
                OrderInfo orderInfo = JSON.parseObject(jsonStr, OrderInfo.class);
                //OrderInfo orderInfo = JSON.parseObject(jsonStr).toJavaObject(OrderInfo.class);
                orderInfo.setCreate_ts(sdf.parse(orderInfo.getCreate_time()).getTime());
                return orderInfo;
            }
        });


        SingleOutputStreamOperator<OrderDetail> orderDetailWithCreatetsDS = orderDetailDS.map(new RichMapFunction<String, OrderDetail>() {
            private SimpleDateFormat sdf;

            @Override
            public void open(Configuration parameters) throws Exception {
                sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            }

            @Override
            public OrderDetail map(String jsonStr) throws Exception {

                OrderDetail orderDetail = JSON.parseObject(jsonStr,OrderDetail.class);
                //OrderDetail orderDetail = JSON.parseObject(jsonStr).toJavaObject(OrderDetail.class);
                orderDetail.setCreate_ts(sdf.parse(orderDetail.getCreate_time()).getTime());
                return orderDetail;
            }
        });

        //TODO 5.设置watermark和提取事件时间字段
        SingleOutputStreamOperator<OrderInfo> orderInfoWithWatermarkDS = orderInfoWithCreatetsDS.assignTimestampsAndWatermarks(
                WatermarkStrategy.<OrderInfo>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                        .withTimestampAssigner(new SerializableTimestampAssigner<OrderInfo>() {
                            @Override
                            public long extractTimestamp(OrderInfo orderInfo, long recordTimestamp) {
                                return orderInfo.getCreate_ts();
                            }
                        })
        );

        SingleOutputStreamOperator<OrderDetail> orderDetailWithWatermarkDS = orderDetailWithCreatetsDS.assignTimestampsAndWatermarks(
                WatermarkStrategy.<OrderDetail>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                        .withTimestampAssigner(new SerializableTimestampAssigner<OrderDetail>() {
                            @Override
                            public long extractTimestamp(OrderDetail orderDetail, long recordTimestamp) {
                                return orderDetail.getCreate_ts();
                            }
                        })
        );

        //TODO 6. 通过keyby对两条流进行分组 指定连接字段
        KeyedStream<OrderInfo, Long> orderInfoKeyedStream = orderInfoWithWatermarkDS.keyBy(orderInfo -> orderInfo.getId());
        KeyedStream<OrderDetail, Long> orderDetailKeyedStream = orderDetailWithWatermarkDS.keyBy(orderDetail -> orderDetail.getOrder_id());




        //TODO 7.将两条流进行intervalJoin
        SingleOutputStreamOperator<OrderWide> orderWideDS = orderInfoKeyedStream
                .intervalJoin(orderDetailKeyedStream)
                .between(Time.seconds(-5), Time.seconds(5))
                .process(new ProcessJoinFunction<OrderInfo, OrderDetail, OrderWide>() {
                    @Override
                    public void processElement(OrderInfo orderInfo, OrderDetail orderDetail, Context ctx, Collector<OrderWide> out) throws Exception {
                        out.collect(new OrderWide(orderInfo, orderDetail));
                    }
                });

        //TODO 8.关联用户维度
        SingleOutputStreamOperator<OrderWide> orderWideWithUserIdDS= AsyncDataStream.unorderedWait(
                orderWideDS,
                new DimAsyncFunction<OrderWide>("DIM_USER_INFO") {
                    @Override
                    public String getKey(OrderWide orderWide) {

                        return orderWide.getUser_id().toString();
                    }

                    @Override
                    public void join(OrderWide orderWide, JSONObject jsonObj) throws Exception {

                        String gender = jsonObj.getString("GENDER");

                        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
                        //System.out.println("sdf对象为" + sdf);
                        String birthdayDate = jsonObj.getString("BIRTHDAY");

                        Long birthdayTime = sdf.parse(birthdayDate).getTime();
                        Long curTime = System.currentTimeMillis();
                        Long ageLong = (curTime - birthdayTime) / 1000L / 60L / 60L / 24L / 365L;
                        orderWide.setUser_gender(gender);
                        orderWide.setUser_age(ageLong.intValue());

                    }

        }, 60, TimeUnit.SECONDS);

        //orderWideWithUserIdDS.print(">>>");

        //TODO 9关联省市维度
        SingleOutputStreamOperator<OrderWide> orderWideWithPriovinceDS = AsyncDataStream.unorderedWait(orderWideWithUserIdDS,
                new DimAsyncFunction<OrderWide>("DIM_BASE_PROVINCE") {
                    @Override
                    public String getKey(OrderWide orderWide) {
                        return orderWide.getProvince_id().toString();
                    }

                    @Override
                    public void join(OrderWide orderWide, JSONObject jsonObj) {
                            orderWide.setProvince_3166_2_code(jsonObj.getString("ISO_3166_2"));
                            orderWide.setProvince_area_code(jsonObj.getString("AREA_CODE"));
                            orderWide.setProvince_iso_code(jsonObj.getString("ISO_CODE"));
                            orderWide.setProvince_name(jsonObj.getString("NAME"));
                    }
                }, 60, TimeUnit.SECONDS);
        //TODO 10.关联sku维度
        SingleOutputStreamOperator<OrderWide> orderWideWithSkuDS = AsyncDataStream.unorderedWait(orderWideWithPriovinceDS,
                new DimAsyncFunction<OrderWide>("DIM_SKU_INFO") {
                    @Override
                    public String getKey(OrderWide orderWide) {
                        return orderWide.getSku_id().toString();
                    }

                    @Override
                    public void join(OrderWide orderWide, JSONObject jsonObj) throws Exception {
                        orderWide.setSpu_id(jsonObj.getLong("SPU_ID"));
                        orderWide.setTm_id(jsonObj.getLong("TM_ID"));
                        orderWide.setCategory3_id(jsonObj.getLong("CATEGORY3_ID"));
                    }
                }, 60, TimeUnit.SECONDS);

        //TODO 11.关联spu维度
        SingleOutputStreamOperator<OrderWide> orderWideWithSpuDS = AsyncDataStream.unorderedWait(orderWideWithSkuDS,
                new DimAsyncFunction<OrderWide>("DIM_SPU_INFO") {
                    @Override
                    public String getKey(OrderWide orderWide) {
                        return orderWide.getSpu_id().toString();
                    }

                    @Override
                    public void join(OrderWide orderWide, JSONObject jsonObj) throws Exception {
                        orderWide.setSpu_name(jsonObj.getString("SPU_NAME"));
                    }
                }, 60, TimeUnit.SECONDS);

        //TODO 12. 关联品类维度
        SingleOutputStreamOperator<OrderWide> orderWideWithCategory3DS = AsyncDataStream.unorderedWait(orderWideWithSpuDS,
                new DimAsyncFunction<OrderWide>("DIM_BASE_CATEGORY3") {
                    @Override
                    public String getKey(OrderWide orderWide) {
                        return orderWide.getCategory3_id().toString();
                    }

                    @Override
                    public void join(OrderWide orderWide, JSONObject jsonObj) throws Exception {
                         orderWide.setCategory3_name(jsonObj.getString("NAME"));
                    }
                }, 60, TimeUnit.SECONDS);

        //TODO 13. 关联品牌维度
        SingleOutputStreamOperator<OrderWide> orderWideFullDS = AsyncDataStream.unorderedWait(orderWideWithCategory3DS,
                new DimAsyncFunction<OrderWide>("DIM_BASE_TRADEMARK") {
                    @Override
                    public String getKey(OrderWide orderWide) {
                        return orderWide.getTm_id().toString();
                    }

                    @Override
                    public void join(OrderWide orderWide, JSONObject jsonObj) throws Exception {
                            orderWide.setTm_name(jsonObj.getString("TM_NAME"));
                    }
                }, 60, TimeUnit.SECONDS);

        orderWideFullDS.print(">>>");

        env.execute();

    }
}
