package com.zll.utils;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Properties;

/**
 * @ClassName MyKafkaUtils
 * @Description TODO
 * @Author 17588
 * @Date 2021-08-09 15:35
 * @Version 1.0
 */
public class MyKafkaUtils {
    private static String bootStrap_server = "hadoop102:9092, hadoop103:90092";
    private static Properties props = new Properties();
    public static FlinkKafkaConsumer<String> getKafkaSource(String topic, String groupId) {
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrap_server);
        return new FlinkKafkaConsumer<String>(topic, new SimpleStringSchema(), props);
    }
    public static FlinkKafkaProducer<String> kafkaSink(String topic) {
        return new FlinkKafkaProducer<String>(bootStrap_server, topic, new SimpleStringSchema());
    }
}
