package com.atguigu.gmall.realtime.util;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.util.Properties;

public class MyKafkaUtil {
    private static final String KAFKA_SERVER = "hadoop102:9092,hadoop103:9092,hadoop104:9092";

    public static FlinkKafkaConsumer<String> getKafkaConsumer(String topic,String groupId){

        Properties properties = new Properties();

        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,KAFKA_SERVER);
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,groupId);



        FlinkKafkaConsumer<String> kafkaSource = new FlinkKafkaConsumer<String>(topic,new SimpleStringSchema(),properties);

        return kafkaSource;
    }


    public static FlinkKafkaProducer<String> getKafkaProducer(String topic){
        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,KAFKA_SERVER);
        props.setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG,15*60*1000 + "");

        //return new FlinkKafkaProducer<String>(KAFKA_SERVER,topic,new SimpleStringSchema());

        //return new FlinkKafkaProducer<String>(topic,new SimpleStringSchema(),props);

        //设置FlinkKafkaProducer.Semantic.EXACTLY_ONCE 真正实现精确一次性消费
        return new FlinkKafkaProducer<String>(
                "default_topic",
                new KafkaSerializationSchema<String>() {
                    @Override
                    public ProducerRecord<byte[], byte[]> serialize(String element, @Nullable Long timestamp) {
                        return new ProducerRecord<>(topic,element.getBytes());
                    }
                },
                props,
                FlinkKafkaProducer.Semantic.EXACTLY_ONCE
        );


    }


    public static <T>FlinkKafkaProducer<T> getKafkaProduceWithSchema(KafkaSerializationSchema<T> kafkaSerializationSchema){
        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,KAFKA_SERVER);
        props.setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG,15*60*1000 + "");
        return new FlinkKafkaProducer<T>(
                "default_topic",
                kafkaSerializationSchema,
                props,
                FlinkKafkaProducer.Semantic.EXACTLY_ONCE
        );

    }


}
