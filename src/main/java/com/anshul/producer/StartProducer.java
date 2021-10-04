package com.anshul.producer;

import org.apache.kafka.common.serialization.StringSerializer;

public class StartProducer {
    public static String TOPIC_IN = "message_stream_topic";
    public static String BOOTSTRAP_SERVER = "localhost:9092";

    public static void main(String[] args) {
        Producer<String> p = new Producer<String>(BOOTSTRAP_SERVER, StringSerializer.class.getName());

        for (int i = 0; i < 10; i++) {
            p.send(TOPIC_IN, i + "");
        }
    }
}
