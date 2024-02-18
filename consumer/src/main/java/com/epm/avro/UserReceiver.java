package com.epm.avro;

import lombok.extern.java.Log;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.concurrent.CountDownLatch;

@Log
@Component
public class UserReceiver {

    private CountDownLatch latch = new CountDownLatch(1);

    @KafkaListener(topics = "test-users")
    public void consume(User user) {
        log.info(user.toString());
        latch.countDown();
    }
}