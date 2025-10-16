package com.dat.backend.kafkasimple.config;

import com.dat.backend.kafkasimple.listener.consumer.LoggingInterceptor;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerContainerFactory;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;

import java.util.List;

@Configuration
public class KafkaConfig {

//    @Bean
//    public NewTopic topic1() {
//        return TopicBuilder.name("topic1")
//                .partitions(3)
//                .replicas(2)
//                .config("cleanup.policy", "compact, delete")
//                .config("delete.retention.ms", "604800000") // max 7 days for retain the log
//                .config("segment.bytes", "1073741824") // max 1 GB per segment file for the log
//                .config("compression.type", "zstd")
//                .build();
//    }
//
//    @Bean
//    public NewTopic topic2() {
//        return TopicBuilder.name("topic2")
//                .assignReplicas(0, List.of(1,2))
//                .assignReplicas(1, List.of(2,3))
//                .assignReplicas(2, List.of(3,1))
//                .config("compression.type", "zstd")
//                .build();
//    }

    @Bean
    public NewTopic topic3() {
        return TopicBuilder.name("hi")
                .config("compression.type", "zstd")
                .build();
    }
}
