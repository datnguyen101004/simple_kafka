package com.dat.backend.kafkasimple.config;

import com.dat.backend.kafkasimple.tracing.ProducerTemplateTracing;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.serializer.JsonSerializer;

import java.util.Collections;
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
                ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 100,
                ProducerConfig.RECONNECT_BACKOFF_MS_CONFIG, 2000L, //2s
                ProducerConfig.RECONNECT_BACKOFF_MAX_MS_CONFIG, 15000L // 15s
        );
    }

    @Bean
    public ProducerFactory<String, Object> objectProducerFactory() {
        return new DefaultKafkaProducerFactory<>(objectProducerConfigs());
    }

    @Bean
    public Map<String, Object> objectProducerConfigs() {
        return Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092",
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class,
                ProducerConfig.ACKS_CONFIG, "1",
                ProducerConfig.LINGER_MS_CONFIG, 1,
                ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 1000,
                ProducerConfig.RECONNECT_BACKOFF_MS_CONFIG, 2000L, //2s
                ProducerConfig.RECONNECT_BACKOFF_MAX_MS_CONFIG, 15000L // 15s
        );
    }

    @Bean
    public KafkaTemplate<String, String> stringTemplate(ProducerFactory<String, String> pf) {
        return new KafkaTemplate<>(pf,
                Collections.singletonMap(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class)
                );
    }

    @Bean
    public KafkaTemplate<String, Object> objectTemplate(ProducerFactory<String, Object> pf) {
        KafkaTemplate<String, Object> kafkaTemplate = new KafkaTemplate<>(pf);
        kafkaTemplate.setObservationEnabled(true);
        kafkaTemplate.setObservationConvention(producerTemplateTracing);
        return kafkaTemplate;
    }
}
