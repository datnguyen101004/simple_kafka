package com.dat.backend.kafkasimple.listener.consumer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.listener.RecordInterceptor;

@Slf4j
public class LoggingInterceptor<K, V> implements RecordInterceptor<K, V> {
    private static final String bannedWord = "banned";

    @Override
    public ConsumerRecord<K, V> intercept(ConsumerRecord<K, V> record, Consumer<K, V> consumer) {
        // TODO: implement logging logic such as sending logs to admin if necessary
        if (record.value().toString().contains(bannedWord)) {
            log.warn("Banned word detected in message: {}", record.value());
            // TODO: implement additional actions like sending alerts if necessary
            return null; // Skip handle this record
        }
        return record;
    }

    @Override
    public void success(ConsumerRecord<K, V> record, Consumer<K, V> consumer) {
        RecordInterceptor.super.success(record, consumer);
    }

    @Override
    public void failure(ConsumerRecord<K, V> record, Exception exception, Consumer<K, V> consumer) {
        log.error("Error in LoggingInterceptor: {}", exception.getMessage());
        // TODO: implement retry logic or fallback mechanism
    }
}
