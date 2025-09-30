package com.dat.backend.kafkasimple.service;

import com.dat.backend.kafkasimple.listener.ProducerListener;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

@Service
@Slf4j
@RequiredArgsConstructor
public class SendService {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ProducerListener producerListener;

    public String sendAsyncMessage(String message) {
        String key = "1";
        final ProducerRecord<String, String> record = new ProducerRecord<>("topic1", key, message);
        CompletableFuture<SendResult<String, String>> future = kafkaTemplate.send(record);
        future.whenComplete((res, ex) -> {
            if (ex == null) {
                log.info("Sent message: {}", message);
            }
            else {
                handleFail(key, message, ex);
            }
        });
        return "Message sent: " + message;
    }

    private void handleFail(String key, String message, Throwable ex) {
        producerListener.onError(key, message, new Exception(ex));
    }

    public String sendSyncMessage(String message) {
        String key = "1";
        try {
            final ProducerRecord<String, String> record = new ProducerRecord<>("topic1",key, message);
            kafkaTemplate.send(record).get(1, TimeUnit.MILLISECONDS);
            return "Message sent: " + message;
        } catch (Exception ex) {
            handleFail(key, message, ex);
            return "Failed to send message: " + message;
        }
    }
}
