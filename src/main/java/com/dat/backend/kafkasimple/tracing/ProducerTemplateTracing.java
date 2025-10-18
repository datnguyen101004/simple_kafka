package com.dat.backend.kafkasimple.tracing;

import io.micrometer.common.KeyValues;
import lombok.NonNull;
import org.springframework.kafka.support.micrometer.KafkaRecordSenderContext;
import org.springframework.kafka.support.micrometer.KafkaTemplateObservationConvention;
import org.springframework.stereotype.Component;

@Component
public class ProducerTemplateTracing implements KafkaTemplateObservationConvention {

    @Override
    public String getName() {
        return "kafka.producer.template";
    }

    @Override
    public String getContextualName(KafkaRecordSenderContext context) {
        return "kafka-producer-topic-" + context.getDestination();
    }

    @Override
    @NonNull
    public KeyValues getLowCardinalityKeyValues(KafkaRecordSenderContext context) {
        return KeyValues.of(
                "topic", context.getRecord().topic(),
                "key", String.valueOf(context.getRecord().key())
        );
    }

    @Override
    @NonNull
    public KeyValues getHighCardinalityKeyValues(KafkaRecordSenderContext context) {
        return KeyValues.of(
                "partition", String.valueOf(context.getRecord().partition())
        );
    }
}
