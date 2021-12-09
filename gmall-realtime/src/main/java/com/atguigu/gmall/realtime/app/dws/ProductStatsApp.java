package com.atguigu.gmall.realtime.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.app.func.DimAsyncFunction;
import com.atguigu.gmall.realtime.bean.OrderWide;
import com.atguigu.gmall.realtime.bean.PaymentWide;
import com.atguigu.gmall.realtime.bean.ProductStats;
import com.atguigu.gmall.realtime.common.GmallConstant;
import com.atguigu.gmall.realtime.util.ClickHouseUtil;
import com.atguigu.gmall.realtime.util.DateTimeUtil;
import com.atguigu.gmall.realtime.util.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.concurrent.TimeUnit;

public class ProductStatsApp {
    public static void main(String[] args) throws Exception {
        //TODO 1.基本环境准备
        //1.1 设置流处理环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //1.2 设置并行度
        env.setParallelism(4);

        //TODO 2.检查点相关的设置(略)
        //TODO 3.从Kafka中读取数据
        //3.1 声明消费的主题以及消费者组
        String groupId = "product_stats_app";

        String pageViewSourceTopic = "dwd_page_log";
        String favorInfoSourceTopic = "dwd_favor_info";
        String cartInfoSourceTopic = "dwd_cart_info";
        String orderWideSourceTopic = "dwm_order_wide";
        String paymentWideSourceTopic = "dwm_payment_wide";
        String refundInfoSourceTopic = "dwd_order_refund_info";
        String commentInfoSourceTopic = "dwd_comment_info";

        //3.2 创建消费者对象
        FlinkKafkaConsumer<String> pageViewSource  = MyKafkaUtil.getKafkaConsumer(pageViewSourceTopic,groupId);
        FlinkKafkaConsumer<String> orderWideSource  = MyKafkaUtil.getKafkaConsumer(orderWideSourceTopic,groupId);
        FlinkKafkaConsumer<String> paymentWideSource  = MyKafkaUtil.getKafkaConsumer(paymentWideSourceTopic,groupId);
        FlinkKafkaConsumer<String> favorInfoSourceSouce  = MyKafkaUtil.getKafkaConsumer(favorInfoSourceTopic,groupId);
        FlinkKafkaConsumer<String> cartInfoSource  = MyKafkaUtil.getKafkaConsumer(cartInfoSourceTopic,groupId);
        FlinkKafkaConsumer<String> refundInfoSource  = MyKafkaUtil.getKafkaConsumer(refundInfoSourceTopic,groupId);
        FlinkKafkaConsumer<String> commentInfoSource  = MyKafkaUtil.getKafkaConsumer(commentInfoSourceTopic,groupId);

        //3.3 消费数据 封装为流
        DataStreamSource<String> pageViewDS = env.addSource(pageViewSource);
        DataStreamSource<String> favorInfoDS = env.addSource(favorInfoSourceSouce);
        DataStreamSource<String> cartInfoDS= env.addSource(cartInfoSource);
        DataStreamSource<String> orderWideDS= env.addSource(orderWideSource);
        DataStreamSource<String> paymentWideDS= env.addSource(paymentWideSource);
        DataStreamSource<String> refundInfoDS= env.addSource(refundInfoSource);
        DataStreamSource<String> commentInfoDS= env.addSource(commentInfoSource);


        //TODO 4.对各个流中的数据类型进行转换     jsonStr->ProductStats实体类对象
        //4.1 点击和曝光
        SingleOutputStreamOperator<ProductStats> clickAndDisplayStatsDS = pageViewDS.process(
                new ProcessFunction<String, ProductStats>() {
                    @Override
                    public void processElement(String jsonStr, Context ctx, Collector<ProductStats> out) throws Exception {
                        //为了操作方便  先将json字符串转换为json对象
                        JSONObject jsonObj = JSON.parseObject(jsonStr);
                        //获取事件时间
                        Long ts = jsonObj.getLong("ts");
                        //获取json的page
                        JSONObject pageJsonObj = jsonObj.getJSONObject("page");
                        //获取当前操作的页面id
                        String pageId = pageJsonObj.getString("page_id");
                        //判断是否为商品的点击行为
                        if ("good_detail".equals(pageId)) {
                            // 是点击行为
                            // 获取点击的商品的id
                            Long skuId = pageJsonObj.getLong("item");
                            // 封装ProductStats对象
                            ProductStats productStats = ProductStats.builder()
                                    .sku_id(skuId)
                                    .click_ct(1L)
                                    .ts(ts)
                                    .build();
                            out.collect(productStats);
                        }

                        //判断页面上是否有商品的曝光行为
                        JSONArray displayArr = jsonObj.getJSONArray("displays");
                        if (displayArr != null && displayArr.size() > 0) {
                            //如果页面上存在曝光行为，将所有的曝光行为遍历出来
                            for (int i = 0; i < displayArr.size(); i++) {
                                JSONObject displayJsonObj = displayArr.getJSONObject(i);
                                //判断曝光的是不是商品
                                if ("sku_id".equals(displayJsonObj.getString("item_type"))) {
                                    //曝光的是商品
                                    //获取曝光的商品的id
                                    Long skuId = displayJsonObj.getLong("item");
                                    // 将曝光信息封装为一个ProductStats对象
                                    ProductStats productStats = ProductStats.builder()
                                            .sku_id(skuId)
                                            .display_ct(1L)
                                            .ts(ts)
                                            .build();
                                    out.collect(productStats);
                                }
                            }
                        }
                    }
                }
        );
        //4.2 收藏
        SingleOutputStreamOperator<ProductStats> favorInfoStatsDS = favorInfoDS.map(
                new MapFunction<String, ProductStats>() {
                    @Override
                    public ProductStats map(String jsonStr) throws Exception {
                        JSONObject favorInfojsonObj = JSON.parseObject(jsonStr);
                        String createTime = favorInfojsonObj.getString("create_time");
                        Long ts = DateTimeUtil.toTs(createTime);
                        ProductStats productStats = ProductStats.builder()
                                .sku_id(favorInfojsonObj.getLong("sku_id"))
                                .favor_ct(1L)
                                .ts(ts)
                                .build();
                        return productStats;
                    }
                }
        );
        //4.3 加购
        SingleOutputStreamOperator<ProductStats> cartInfoStatsDS = cartInfoDS.map(
                new MapFunction<String, ProductStats>() {
                    @Override
                    public ProductStats map(String jsonStr) throws Exception {
                        JSONObject cartInfoJsonObj = JSON.parseObject(jsonStr);
                        return ProductStats.builder()
                                .sku_id(cartInfoJsonObj.getLong("sku_id"))
                                .cart_ct(1L)
                                .ts(DateTimeUtil.toTs(cartInfoJsonObj.getString("create_time")))
                                .build();
                    }
                }
        );
        //4.4 订单
        SingleOutputStreamOperator<ProductStats> orderWideStatsDS = orderWideDS.map(
                new MapFunction<String, ProductStats>() {
                    @Override
                    public ProductStats map(String jsonStr) throws Exception {
                        //将json格式字符串转换为订单宽表实体对象
                        OrderWide orderWide = JSON.parseObject(jsonStr, OrderWide.class);

                        return ProductStats.builder()
                                .sku_id(orderWide.getSku_id())
                                .order_sku_num(orderWide.getSku_num())
                                .order_amount(orderWide.getSplit_total_amount())
                                .orderIdSet(new HashSet(Collections.singleton(orderWide.getOrder_id())))
                                .ts(DateTimeUtil.toTs(orderWide.getCreate_time()))
                                .build();
                    }
                }
        );
        //4.5 支付
        SingleOutputStreamOperator<ProductStats> paymentStatsDS = paymentWideDS.map(
                new MapFunction<String, ProductStats>() {
                    @Override
                    public ProductStats map(String json) throws Exception {
                        PaymentWide paymentWide = JSON.parseObject(json, PaymentWide.class);
                        Long ts = DateTimeUtil.toTs(paymentWide.getCallback_time());
                        return ProductStats.builder().sku_id(paymentWide.getSku_id())
                                .payment_amount(paymentWide.getSplit_total_amount())
                                .paidOrderIdSet(new HashSet(Collections.singleton(paymentWide.getOrder_id())))
                                .ts(ts).build();
                    }
                });

        //4.6 退单
        SingleOutputStreamOperator<ProductStats> refundStatsDS = refundInfoDS.map(
                new MapFunction<String, ProductStats>() {
                    @Override
                    public ProductStats map(String json) throws Exception {
                        JSONObject refundJsonObj = JSON.parseObject(json);
                        Long ts = DateTimeUtil.toTs(refundJsonObj.getString("create_time"));
                        ProductStats productStats = ProductStats.builder()
                                .sku_id(refundJsonObj.getLong("sku_id"))
                                .refund_amount(refundJsonObj.getBigDecimal("refund_amount"))
                                .refundOrderIdSet(
                                        new HashSet(Collections.singleton(refundJsonObj.getLong("order_id"))))
                                .ts(ts).build();
                        return productStats;
                    }
                });

        //4.7 评论
        SingleOutputStreamOperator<ProductStats> commonInfoStatsDS = commentInfoDS.map(
                new MapFunction<String, ProductStats>() {
                    @Override
                    public ProductStats map(String json) throws Exception {
                        JSONObject commonJsonObj = JSON.parseObject(json);
                        Long ts = DateTimeUtil.toTs(commonJsonObj.getString("create_time"));
                        Long goodCt = GmallConstant.APPRAISE_GOOD.equals(commonJsonObj.getString("appraise")) ? 1L : 0L;
                        ProductStats productStats = ProductStats.builder()
                                .sku_id(commonJsonObj.getLong("sku_id"))
                                .comment_ct(1L).good_comment_ct(goodCt).ts(ts).build();
                        return productStats;
                    }
                });

        //TODO 5.使用union将7条流的数据进行合并
        DataStream<ProductStats> unionDS = clickAndDisplayStatsDS.union(
                favorInfoStatsDS,
                cartInfoStatsDS,
                orderWideStatsDS,
                paymentStatsDS,
                refundStatsDS,
                commonInfoStatsDS
        );

        //unionDS.print(">>>>>");

        //TODO 6.指定Watermark以及提取事件时间字段
        SingleOutputStreamOperator<ProductStats> productStatsWithWatermarkDS = unionDS.assignTimestampsAndWatermarks(
                WatermarkStrategy
                        .<ProductStats>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                        .withTimestampAssigner(
                                new SerializableTimestampAssigner<ProductStats>() {
                                    @Override
                                    public long extractTimestamp(ProductStats productStats, long recordTimestamp) {
                                        return productStats.getTs();
                                    }
                                }
                        )
        );

        //TODO 7.分组
        KeyedStream<ProductStats, Long> keyedDS = productStatsWithWatermarkDS.keyBy(
                new KeySelector<ProductStats, Long>() {
                    @Override
                    public Long getKey(ProductStats productStats) throws Exception {
                        return productStats.getSku_id();
                    }
                }
        );

        //TODO 8.开窗
        WindowedStream<ProductStats, Long, TimeWindow> windowDS = keyedDS.window(TumblingEventTimeWindows.of(Time.seconds(10)));

        //TODO 9.聚合计算
        SingleOutputStreamOperator<ProductStats> reduceDS = windowDS.reduce(
                new ReduceFunction<ProductStats>() {
                    @Override
                    public ProductStats reduce(ProductStats stats1, ProductStats stats2) throws Exception {
                        stats1.setDisplay_ct(stats1.getDisplay_ct() + stats2.getDisplay_ct());
                        stats1.setClick_ct(stats1.getClick_ct() + stats2.getClick_ct());
                        stats1.setCart_ct(stats1.getCart_ct() + stats2.getCart_ct());
                        stats1.setFavor_ct(stats1.getFavor_ct() + stats2.getFavor_ct());
                        stats1.setOrder_amount(stats1.getOrder_amount().add(stats2.getOrder_amount()));
                        stats1.getOrderIdSet().addAll(stats2.getOrderIdSet());
                        stats1.setOrder_ct(stats1.getOrderIdSet().size() + 0L);
                        stats1.setOrder_sku_num(stats1.getOrder_sku_num() + stats2.getOrder_sku_num());
                        stats1.setPayment_amount(stats1.getPayment_amount().add(stats2.getPayment_amount()));

                        stats1.getRefundOrderIdSet().addAll(stats2.getRefundOrderIdSet());
                        stats1.setRefund_order_ct(stats1.getRefundOrderIdSet().size() + 0L);
                        stats1.setRefund_amount(stats1.getRefund_amount().add(stats2.getRefund_amount()));

                        stats1.getPaidOrderIdSet().addAll(stats2.getPaidOrderIdSet());
                        stats1.setPaid_order_ct(stats1.getPaidOrderIdSet().size() + 0L);

                        stats1.setComment_ct(stats1.getComment_ct() + stats2.getComment_ct());
                        stats1.setGood_comment_ct(stats1.getGood_comment_ct() + stats2.getGood_comment_ct());

                        return stats1;

                    }
                },
                new ProcessWindowFunction<ProductStats, ProductStats, Long, TimeWindow>() {
                    @Override
                    public void process(Long aLong, Context context, Iterable<ProductStats> elements, Collector<ProductStats> out) throws Exception {
                        for (ProductStats productStats : elements) {
                            productStats.setStt(DateTimeUtil.toYmdhms(new Date(context.window().getStart())));
                            productStats.setEdt(DateTimeUtil.toYmdhms(new Date(context.window().getEnd())));
                            productStats.setTs(System.currentTimeMillis());
                            out.collect(productStats);
                        }
                    }
                }
        );

        //reduceDS.print(">>>>>");

        //TODO 10.补充商品的维度信息
        //10.1 和sku维度进行关联
        SingleOutputStreamOperator<ProductStats> productStatsWithSkuInfoDS = AsyncDataStream.unorderedWait(
                reduceDS,
                new DimAsyncFunction<ProductStats>("DIM_SKU_INFO") {
                    @Override
                    public String getKey(ProductStats productStats) {
                        return productStats.getSku_id().toString();
                    }

                    @Override
                    public void join(ProductStats productStats,JSONObject jsonObject) throws Exception {
                        productStats.setSku_name(jsonObject.getString("SKU_NAME"));
                        productStats.setSku_price(jsonObject.getBigDecimal("PRICE"));
                        productStats.setCategory3_id(jsonObject.getLong("CATEGORY3_ID"));
                        productStats.setSpu_id(jsonObject.getLong("SPU_ID"));
                        productStats.setTm_id(jsonObject.getLong("TM_ID"));
                    }
                },
                60, TimeUnit.SECONDS
        );

        //10.2 补充SPU维度
        SingleOutputStreamOperator<ProductStats> productStatsWithSpuDS =
                AsyncDataStream.unorderedWait(productStatsWithSkuInfoDS,
                        new DimAsyncFunction<ProductStats>("DIM_SPU_INFO") {
                            @Override
                            public void join(ProductStats productStats,JSONObject jsonObject) throws Exception {
                                productStats.setSpu_name(jsonObject.getString("SPU_NAME"));
                            }
                            @Override
                            public String getKey(ProductStats productStats) {
                                return String.valueOf(productStats.getSpu_id());
                            }
                        }, 60, TimeUnit.SECONDS);


        //10.3 补充品类维度
        SingleOutputStreamOperator<ProductStats> productStatsWithCategory3DS =
                AsyncDataStream.unorderedWait(productStatsWithSpuDS,
                        new DimAsyncFunction<ProductStats>("DIM_BASE_CATEGORY3") {
                            @Override
                            public void join(ProductStats productStats,JSONObject jsonObject) throws Exception {
                                productStats.setCategory3_name(jsonObject.getString("NAME"));
                            }
                            @Override
                            public String getKey(ProductStats productStats) {
                                return String.valueOf(productStats.getCategory3_id());
                            }
                        }, 60, TimeUnit.SECONDS);

        //10.4 补充品牌维度
        SingleOutputStreamOperator<ProductStats> productStatsWithTmDS =
                AsyncDataStream.unorderedWait(productStatsWithCategory3DS,
                        new DimAsyncFunction<ProductStats>("DIM_BASE_TRADEMARK") {
                            @Override
                            public void join(ProductStats productStats,JSONObject jsonObject) throws Exception {
                                productStats.setTm_name(jsonObject.getString("TM_NAME"));
                            }
                            @Override
                            public String getKey(ProductStats productStats) {
                                return String.valueOf(productStats.getTm_id());
                            }
                        }, 60, TimeUnit.SECONDS);

        productStatsWithTmDS.print(">>>>>");

        //TODO 11.将计算结果  保存到CK
        productStatsWithTmDS.addSink(
                ClickHouseUtil.<ProductStats>getJdbcsink("insert into product_stats_0609 values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)")
        );


        env.execute();
    }
}
