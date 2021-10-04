package com.anshul.consumer;

import com.anshul.producer.StartProducer;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;

import java.util.UUID;

public class StartConsumer2Sources {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        FlinkKafkaConsumer011<String> consumer = Consumer.createStringConsumerForTopic(StartProducer.TOPIC_IN, StartProducer.BOOTSTRAP_SERVER, UUID.randomUUID().toString(), UUID.randomUUID().toString());
        FlinkKafkaConsumer011<String> consumer2 = Consumer.createStringConsumerForTopic(StartProducer.TOPIC_IN2, StartProducer.BOOTSTRAP_SERVER, UUID.randomUUID().toString(), UUID.randomUUID().toString());
        DataStream<String> stream = env.addSource(consumer);
        DataStream<String> stream2 = env.addSource(consumer2);

        stream.map(data -> {
            System.out.println("consume 1 :" + data);
            return data;
        });

        stream2.map(data -> {
            System.out.println("consume 2 :" + data);
            return data;
        });
        env.execute();
    }
}
