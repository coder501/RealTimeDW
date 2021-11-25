package com.atguigu.gmall.realtime.util;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;

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

        return new FlinkKafkaProducer<String>(KAFKA_SERVER,topic,new SimpleStringSchema());

        //return new FlinkKafkaProducer<String>(topic,new SimpleStringSchema(),props);
    }
}
