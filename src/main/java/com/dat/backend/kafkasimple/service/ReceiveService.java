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
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.support.converter.BatchMessagingMessageConverter;
import org.springframework.kafka.support.converter.RecordMessageConverter;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Headers;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.stereotype.Service;

import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

@Service
@Slf4j
public class ReceiveService {
    private final ThreadPoolTaskExecutor threadPoolTaskExecutor;

    public ReceiveService(@Qualifier("threadPoolTaskExecutor") ThreadPoolTaskExecutor threadPoolTaskExecutor) {
        this.threadPoolTaskExecutor = threadPoolTaskExecutor;
    };

    @KafkaListener(id = "product-listener", topics = "hi", groupId = "product-service", containerFactory = "kafkaListenerContainerFactory", clientIdPrefix = "batch-consumer-id")
    public CompletableFuture<String> listenWithBatchListener(List<ConsumerRecord<String, Object>> records,
                                                             Acknowledgment acknowledgment) {
        int startTime = (int) System.currentTimeMillis();
        int mill = 0;
        List<CompletableFuture<Void>> futures = new java.util.ArrayList<>();
        for (ConsumerRecord<String, Object> record : records) {
            String key = record.key();
            if ("1".equals(key)) {
                mill = 1000;
            } else if ("2".equals(key)) {
                mill = 1000;
            } else if ("3".equals(key)) {
                mill = 5000;
            } else if ("4".equals(key)) {
                mill = 2000;
            } else if ("5".equals(key)) {
                mill = 4000;
            } else {
                mill = 0;
            }
            int finalMill = mill;
            CompletableFuture<Void> future = CompletableFuture.runAsync(() -> pauseProcessing(finalMill, key), threadPoolTaskExecutor);
            futures.add(future);
        }
        return CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).thenApply(
                v -> {
                    int endTime = (int) System.currentTimeMillis();
                    log.info("Time to process batch: {} ms", endTime - startTime);
                    acknowledgment.acknowledge();
                    return "Processed " + records.size() + " messages";
                }
        );
    }

    private void pauseProcessing(int millis, String key) {
        try {
            Thread.sleep(millis);
            log.info("Handled message with key: {} after {} ms", key, millis);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
