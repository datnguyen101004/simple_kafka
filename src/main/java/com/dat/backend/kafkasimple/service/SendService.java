package com.dat.backend.kafkasimple.service;

import com.dat.backend.kafkasimple.dto.Message;
import com.dat.backend.kafkasimple.listener.producer.ProducerListener;
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

    //private final KafkaTemplate<String, String> kafkaTemplate;
    private final KafkaTemplate<String, Object> kafkaTemplateObj;
    private final ProducerListener producerListener;

    public String sendAsyncMessage(String message) {
//        String key = "1";
//        final ProducerRecord<String, String> record = new ProducerRecord<>("topic1", key, message);
//        CompletableFuture<SendResult<String, String>> future = kafkaTemplate.send(record);
//        future.whenComplete((res, ex) -> {
//            if (ex == null) {
//                log.info("Sent message: {}", message);
//            }
//            else {
//                handleFail(key, message, ex);
//            }
//        });
        return "Message sent: " + message;
    }

    private void handleFail(String key, String message, Throwable ex) {
        producerListener.onError(key, message, new Exception(ex));
    }

    public String sendSyncMessage(Message message) {
        String key = "1";
        try {
            ProducerRecord<String, Object> record = new ProducerRecord<>("topic2",key, message);
            kafkaTemplateObj.send(record).get(1000, TimeUnit.MILLISECONDS); // Max time for send message to kafka
            return "Message sent: " + message;
        } catch (Exception ex) {
            return "Failed to send message: " + message;
        }
    }
}
