package com.anshul.consumer;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Properties;

public class Consumer {
    public static FlinkKafkaConsumer011<String> createStringConsumerForTopic(
            String topic, String address, String groupId, String clientId) {

        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, address);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, clientId);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        FlinkKafkaConsumer011<String> consumer = new FlinkKafkaConsumer011<>(
                topic, new SimpleStringSchema(), props);

        return consumer;
    }
}
