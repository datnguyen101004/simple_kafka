package com.dat.backend.kafkasimple.service;

import com.dat.backend.kafkasimple.dto.Message;
import com.dat.backend.kafkasimple.listener.producer.ProducerListener;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.concurrent.CompletableFuture;

@Service
@Slf4j
public class SendService {

    //private final KafkaTemplate<String, String> kafkaTemplate;
    private final KafkaTemplate<String, Object> kafkaTemplateObj;
    private final ProducerListener producerListener;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final KafkaTemplate<String, Object> transactionKafkaTemplate;

    public SendService(@Qualifier("transactionalTemplate") KafkaTemplate<String, Object> transactionKafkaTemplate ,@Qualifier("objectTemplate") KafkaTemplate<String, Object> kafkaTemplateObj, ProducerListener producerListener, KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplateObj = kafkaTemplateObj;
        this.transactionKafkaTemplate = transactionKafkaTemplate;
        this.producerListener = producerListener;
        this.kafkaTemplate = kafkaTemplate;
    }


    // Send message to topic with async mode
    public String sendMessage(List<Message> messages) {
        for (Message message : messages) {
            String key = message.getId();
            ProducerRecord<String, Object> producerRecord = new ProducerRecord<>(
                    message.getTopic(), // topic
                    key, // key
                    message.getMessage());// value
            CompletableFuture<SendResult<String, Object>> future = kafkaTemplateObj.send(producerRecord);
            future.whenComplete((result, ex) -> {
                if (ex == null) {
                    producerListener.onSuccess(key, message.getMessage());
                } else {
                    producerListener.onError(key, message.getMessage(), new Exception(ex));
                }
            });
        };
        return "Messages sent";
    }


    public String testTransaction(Message message) {
        boolean result = transactionKafkaTemplate.executeInTransaction(operations -> {
            String key = message.getId();
            ProducerRecord<String, Object> producerRecord = new ProducerRecord<>(
                    "transactions-topic", // topic
                    key, // key
                    message);// value
            operations.send(producerRecord);
            return true;
        });
        return result ? "Transaction successful" : "Transaction not successful";
    }
}
