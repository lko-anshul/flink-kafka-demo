package com.anshul.producer;

import org.apache.kafka.common.serialization.StringSerializer;

import java.util.UUID;

public class StartProducer {
    public static String TOPIC_IN = "message_stream_topic";
    public static String TOPIC_IN2 = "message_stream_topic2";
    public static String BOOTSTRAP_SERVER = "localhost:9092";

    public static void main(String[] args) {
        Producer<String> producer = new Producer<String>(BOOTSTRAP_SERVER, StringSerializer.class.getName());
        for (int i = 0; i < 10; i++) {
            producer.send(TOPIC_IN, i + "");
        }
        Producer<String> producer2 = new Producer<String>(BOOTSTRAP_SERVER, StringSerializer.class.getName());
        for (int i = 0; i < 10; i++) {
            producer2.send(TOPIC_IN2, UUID.randomUUID().toString());
        }
    }
}
