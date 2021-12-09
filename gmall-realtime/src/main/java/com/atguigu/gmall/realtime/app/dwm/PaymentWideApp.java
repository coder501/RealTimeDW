package com.atguigu.gmall.realtime.app.dwm;

import com.alibaba.fastjson.JSON;
import com.atguigu.gmall.realtime.bean.OrderWide;
import com.atguigu.gmall.realtime.bean.PaymentInfo;
import com.atguigu.gmall.realtime.bean.PaymentWide;
import com.atguigu.gmall.realtime.util.DateTimeUtil;
import com.atguigu.gmall.realtime.util.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * desc: 支付宽表
 */
public class PaymentWideApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        env.enableCheckpointing(5, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(60000L);
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3,5000L));
        env.setStateBackend(new FsStateBackend("hdfs://hadoop102:8020/gmall/ck/"));
        System.setProperty("HADOOP_USER_NAME","atguigu");


        String paymentInfoSourceTopic = "dwd_payment_info";
        String orderWideSourceTopic = "dwm_order_wide";
        String groupId = "payment_wide_group";

        //3.2 创建消费者对象
        FlinkKafkaConsumer<String> paymentInfoKafkaSource = MyKafkaUtil.getKafkaConsumer(paymentInfoSourceTopic, groupId);
        FlinkKafkaConsumer<String> orderWideKafkaSource = MyKafkaUtil.getKafkaConsumer(orderWideSourceTopic, groupId);
        //3.3 消费数据 封装为流
        DataStreamSource<String> paymentInfoStrDS = env.addSource(paymentInfoKafkaSource);
        DataStreamSource<String> orderWideStrDS = env.addSource(orderWideKafkaSource);

        //TODO 4.对读取的数据进行类型的转换  jsonStr--->实体类型对象
        //4.1 支付
        SingleOutputStreamOperator<PaymentInfo> paymentInfoDS = paymentInfoStrDS.map(jsonStr -> JSON.parseObject(jsonStr, PaymentInfo.class));

        //4.2 订单宽表
        SingleOutputStreamOperator<OrderWide> orderWideDS = orderWideStrDS.map(jsonStr -> JSON.parseObject(jsonStr, OrderWide.class));

        //paymentInfoDS.print(">>>");
        //orderWideDS.print("####");

        //TODO 5.指定Watermark以及提取事件时间字段
        //5.1 支付
        SingleOutputStreamOperator<PaymentInfo> paymentInfoWithWatermarkDS = paymentInfoDS.assignTimestampsAndWatermarks(
                WatermarkStrategy
                        .<PaymentInfo>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                        .withTimestampAssigner(
                                new SerializableTimestampAssigner<PaymentInfo>() {
                                    @Override
                                    public long extractTimestamp(PaymentInfo paymentInfo, long recordTimestamp) {
                                        return DateTimeUtil.toTs(paymentInfo.getCallback_time());
                                    }
                                }
                        )
        );

        //5.2 订单宽表
        SingleOutputStreamOperator<OrderWide> orderWideWithWatermarkDS = orderWideDS.assignTimestampsAndWatermarks(
                WatermarkStrategy
                        .<OrderWide>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                        .withTimestampAssigner(
                                new SerializableTimestampAssigner<OrderWide>() {
                                    @Override
                                    public long extractTimestamp(OrderWide orderWide, long recordTimestamp) {
                                        return DateTimeUtil.toTs(orderWide.getCreate_time());
                                    }
                                }
                        )
        );

        //TODO 6.通过keyby分组  指定连接字段
        //6.1 支付
        KeyedStream<PaymentInfo, Long> paymentInfoKeyedDS = paymentInfoWithWatermarkDS.keyBy(PaymentInfo::getOrder_id);

        //6.2 订单宽表
        KeyedStream<OrderWide, Long> orderWideKeyedDS = orderWideWithWatermarkDS.keyBy(OrderWide::getOrder_id);

        //TODO 7.使用intervalJoin完成支付和订单宽表的双流join
        SingleOutputStreamOperator<PaymentWide> paymentWideDS = paymentInfoKeyedDS
                .intervalJoin(orderWideKeyedDS)
                .between(Time.seconds(-1800), Time.seconds(0))
                .process(
                        new ProcessJoinFunction<PaymentInfo, OrderWide, PaymentWide>() {
                            @Override
                            public void processElement(PaymentInfo paymentInfo, OrderWide orderWide, Context ctx, Collector<PaymentWide> out) throws Exception {
                                out.collect(new PaymentWide(paymentInfo, orderWide));
                            }
                        }
                );

        paymentWideDS.print(">>>>>");

        //TOOD 8.将支付宽表数据写到kafka的dwm层
        paymentWideDS
                .map(paymentInfo-> JSON.toJSONString(paymentInfo))
                .addSink(MyKafkaUtil.getKafkaProducer("dwm_payment_wide"));

        env.execute();


    }
}
