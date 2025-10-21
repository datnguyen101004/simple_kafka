package com.dat.backend.kafkasimple.listener.producer;

import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class ProducerListener implements IProducerListener<String, String> {

    @Override
    public void onSuccess(String key, String value) {
        log.info("Message sent successfully with key: {} and value: {}", key, value);
    }

    @Override
    public void onError(String key, String value, Exception exception) {
        log.error("Error in ProducerListener: {}", exception.getMessage());
        // TODO: implement retry logic or fallback mechanism
    }
}
