package com.dat.backend.kafkasimple.service;

import com.dat.backend.kafkasimple.dto.Message;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.concurrent.CompletableFuture;

@Service
@Slf4j
public class ReceiveService {
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ThreadPoolTaskExecutor threadPoolTaskExecutor;

    public ReceiveService(KafkaTemplate<String, String> kafkaTemplate,
                          @Qualifier("threadPoolTaskExecutor") ThreadPoolTaskExecutor threadPoolTaskExecutor) {
        this.kafkaTemplate = kafkaTemplate;
        this.threadPoolTaskExecutor = threadPoolTaskExecutor;
    };

    @KafkaListener(topics = "topic3", groupId = "group4", containerFactory = "kafkaListenerContainerFactory")
    public void listenBatch(List<ConsumerRecord<String, String>> messages, Acknowledgment ack) {
        log.info("Handle batch process, size: {}", messages.size());
        for (ConsumerRecord<String, String> message : messages) {
            process(message.value());
        }
        ack.acknowledge(); // Manually acknowledge after processing the batch
    }

    private void process(String message) {
        log.info("Received message: {}", message);
    }

    @KafkaListener(topics = "topic4", groupId = "group4", containerFactory = "kafkaListenerContainerFactory")
    public CompletableFuture<String> listenBatchAsync(List<ConsumerRecord<String, String>> messages, Acknowledgment ack) {
        log.info("Handle batch process async, size: {}", messages.size());
        messages.forEach(message -> {
            processSleep(message.value());
        });
        ack.acknowledge(); // Manually acknowledge after processing the batch
        return CompletableFuture.completedFuture("Batch processed");
    }

    private void processSleep(String message) {
        try {
            Thread.sleep(1500); // Simulate processing time
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        log.info("Received message async: {}", message);
    }
}
