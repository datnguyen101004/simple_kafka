package com.dat.backend.kafkasimple.config;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.ConsumerAwareRebalanceListener;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.support.converter.BatchMessagingMessageConverter;
import org.springframework.kafka.support.converter.JsonMessageConverter;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Configuration
@EnableKafka
@Slf4j
public class ConsumerConf {

    @Bean
    public KafkaListenerContainerFactory<ConcurrentMessageListenerContainer<String, Object>> kafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, Object> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory());
        factory.setConcurrency(2);
        factory.setBatchListener(true);
        factory.setBatchMessageConverter(new BatchMessagingMessageConverter(new JsonMessageConverter()));
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE); // Set ack mode to MANUAL_IMMEDIATE
        factory.getContainerProperties().setConsumerRebalanceListener(new ConsumerAwareRebalanceListener() {
            @Override
            public void onPartitionsRevokedBeforeCommit(Consumer<?, ?> consumer, Collection<TopicPartition> partitions) {
                // Handle partition revocation before committing offsets
                List<String> partitionList = partitions.stream()
                        .map(TopicPartition::partition)
                        .map(String::valueOf)
                        .toList();
                String consumerId = consumer.groupMetadata().memberId();
                String partitionIds = String.join(", ", partitionList);
                // TODO: send to monitoring service
                log.info("Consumer: {} - revoking partitions: {}", consumerId, partitionIds);
            }

            @Override
            public void onPartitionsRevokedAfterCommit(Consumer<?, ?> consumer, Collection<TopicPartition> partitions) {
                log.info("Partitions revoked after commit: {}", partitions);
            }

            @Override
            public void onPartitionsLost(Consumer<?, ?> consumer, Collection<TopicPartition> partitions) {
                log.warn("Partitions lost: {}", partitions);
            }
        });
        factory.getContainerProperties().setMicrometerEnabled(true);
        factory.getContainerProperties().setIdleEventInterval(60000L); // Set the interval to publish an IdleContainerEvent if no records are received. The listener can capture this event to perform some action when the container is idle.
        factory.getContainerProperties().setNoPollThreshold(2);
        factory.getContainerProperties().setPollTimeout(2000L);
        return factory;
    }

    @Bean
    public ConsumerFactory<String, Object> consumerFactory() {
        return new DefaultKafkaConsumerFactory<>(consumerConfig());
    }

    @Bean
    public Map<String, Object> consumerConfig() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.RECONNECT_BACKOFF_MS_CONFIG, 2000L);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.RECONNECT_BACKOFF_MAX_MS_CONFIG, 10000L);
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 10); // Max records per poll
        props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 20000);
        props.put(ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG, false);
        return props;
    }
}
