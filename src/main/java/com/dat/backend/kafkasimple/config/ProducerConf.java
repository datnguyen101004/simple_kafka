package com.dat.backend.kafkasimple.config;

import com.dat.backend.kafkasimple.error.CustomRetryListener;
import com.dat.backend.kafkasimple.tracing.ProducerTemplateTracing;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.jdbc.DataSourceTransactionManagerAutoConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.AfterRollbackProcessor;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultAfterRollbackProcessor;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.ExponentialBackOffWithMaxRetries;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.kafka.transaction.KafkaTransactionManager;
import org.springframework.util.backoff.FixedBackOff;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

@Configuration
@RequiredArgsConstructor
public class ProducerConf {
    private final ProducerTemplateTracing producerTemplateTracing;

    @Bean
    public ProducerFactory<String, String> producerFactory() {
        return new DefaultKafkaProducerFactory<>(producerConfigs());
    }

    @Bean
    public Map<String, Object> producerConfigs() {
        return Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092",
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.ACKS_CONFIG, "1",
                ProducerConfig.LINGER_MS_CONFIG, 100,
                ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 30000,
                ProducerConfig.RECONNECT_BACKOFF_MS_CONFIG, 2000L, //2s
                ProducerConfig.RECONNECT_BACKOFF_MAX_MS_CONFIG, 15000L // 15s
        );
    }

    @Bean
    public ProducerFactory<String, Object> objectProducerFactory() {
        return new DefaultKafkaProducerFactory<>(producerConfigs());
    }

    @Bean
    public Map<String, Object> objectProducerConfigs() {
        return Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092",
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class,
                ProducerConfig.ACKS_CONFIG, "1",
                ProducerConfig.LINGER_MS_CONFIG, 100,
                ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 1000,
                ProducerConfig.RECONNECT_BACKOFF_MS_CONFIG, 2000L, //2s
                ProducerConfig.RECONNECT_BACKOFF_MAX_MS_CONFIG, 15000L // 15ss
        );
    }

    @Bean
    public Map<String, Object> transactionProducerConfig() {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        props.put(ProducerConfig.RETRIES_CONFIG, 5);
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 5);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 100);
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 1000);
        props.put(ProducerConfig.RECONNECT_BACKOFF_MS_CONFIG, 2000L);
        props.put(ProducerConfig.RECONNECT_BACKOFF_MAX_MS_CONFIG, 15000L);
        props.put(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, 4000);
        return props;
    }

    @Bean
    public KafkaTemplate<String, Object> transactionalTemplate(@Qualifier("transactionObjectProducerFactory") ProducerFactory<String, Object> pf) {
        KafkaTemplate<String, Object> kafkaTemplate = new KafkaTemplate<>(pf);
        kafkaTemplate.setObservationEnabled(true);
        kafkaTemplate.setObservationConvention(producerTemplateTracing);
        return kafkaTemplate;
    }

    @Bean
    public KafkaTransactionManager<String, Object> kafkaTransactionManager(@Qualifier("transactionObjectProducerFactory") ProducerFactory<String, Object> pf) {
        return new KafkaTransactionManager<>(pf);
    }

    @Bean
    public ProducerFactory<String, Object> transactionObjectProducerFactory() {
        DefaultKafkaProducerFactory<String, Object> factory = new DefaultKafkaProducerFactory<>(transactionProducerConfig());
        factory.setTransactionIdPrefix("transaction-id-prefix-");
        return factory;
    }

    @Bean
    public KafkaTemplate<String, String> stringTemplate(ProducerFactory<String, String> pf) {
        return new KafkaTemplate<>(pf,
                Collections.singletonMap(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class)
                );
    }

    @Bean
    public KafkaTemplate<String, Object> objectTemplate(@Qualifier("objectProducerFactory") ProducerFactory<String, Object> pf) {
        KafkaTemplate<String, Object> kafkaTemplate = new KafkaTemplate<>(pf);
        kafkaTemplate.setObservationEnabled(true);
        kafkaTemplate.setObservationConvention(producerTemplateTracing);
        return kafkaTemplate;
    }
}
