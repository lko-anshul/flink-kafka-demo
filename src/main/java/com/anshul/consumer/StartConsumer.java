package com.anshul.consumer;

import com.anshul.producer.StartProducer;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;

import java.util.Properties;
import java.util.UUID;

public class StartConsumer {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        FlinkKafkaConsumer011<String> consumer = Consumer.createStringConsumerForTopic(StartProducer.TOPIC_IN, StartProducer.BOOTSTRAP_SERVER, UUID.randomUUID().toString(), UUID.randomUUID().toString());
        DataStream<String> stream = env.addSource(consumer);
        stream.map(data -> {
            System.out.println(data);
            return data;
        });
        env.execute();
    }
}
