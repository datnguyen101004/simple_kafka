package com.dat.backend.kafkasimple.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
@Slf4j
@RequiredArgsConstructor
public class ReceiveService {

    // Listen topic1
    @KafkaListener(topics = "topic1", groupId = "group1")
    public void listenTopic1(ConsumerRecord<String, String> record) {
        log.info("Received message in topic1: {}, offset: {}, partition: {}, key: {}", record.value(), record.offset(), record.partition(), record.key());
    }
}
