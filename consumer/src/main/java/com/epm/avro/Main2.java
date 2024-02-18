package com.epm.avro;

import lombok.extern.java.Log;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.IntegerDeserializer;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

@Log
public class Main2 {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "avro");
        props.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.setProperty(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "100");
        props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        KafkaConsumer<Integer, User> consumer = new KafkaConsumer<>(props, new IntegerDeserializer(), new MyAvroDeserializer<>(User.class));
        consumer.subscribe(List.of("test-users"));

        while (true) {
            ConsumerRecords<Integer, User> records = consumer.poll(Duration.ofMillis(100));

            for (ConsumerRecord<Integer, User> rec : records) {
                log.info("Received message: (" + rec.key() + ", " + rec.value().toString() + ") at offset " + rec.offset());
            }
        }

    }
}