package com.dat.backend.kafkasimple.error;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.RetryListener;

@Slf4j
public class CustomRetryListener implements RetryListener {

    @Override
    public void failedDelivery(ConsumerRecord<?, ?> record, Exception ex, int deliveryAttempt) {
        log.warn("Send to DLT record: {}, ex: {}", record, ex.getMessage());
        // TODO: implement metrics here for monitoring
    }
}
