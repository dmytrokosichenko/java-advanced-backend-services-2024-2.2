package com.epm.avro;

import lombok.extern.java.Log;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerSerializer;

import java.util.Properties;

@Log
public class Main {
    public static void main(String[] args) {

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9093");
        props.put("schema.registry.url", "http://localhost:8081");

        KafkaProducer producer = new KafkaProducer(props,
                new IntegerSerializer(),
                new MyAvroSerializer());

        for (int i = 1; i <= 100000; i++){

            producer.send(new ProducerRecord<>("test-users", 0, i,
                    User.newBuilder()
                            .setId(i)
                            .setName("User " + i)
                            .setCountry("Poland")
                            .setCity("Krakow")
                            .build()));
        }
        producer.close();
    }
}