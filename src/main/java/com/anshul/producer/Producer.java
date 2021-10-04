package com.anshul.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class Producer<T> {

    String bootstrapServers;
    KafkaProducer<String, T> producer;

    public Producer(String kafkaServer, String serializerName) {
        this.bootstrapServers = kafkaServer;
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, serializerName);

        producer = new KafkaProducer<String, T>(properties);
    }

    public void send(String topic, T message) {
        ProducerRecord<String, T> record = new ProducerRecord<String, T>(topic, message);
        producer.send(record);
        producer.flush();
    }

    public void close() {
        producer.close();
    }
}

