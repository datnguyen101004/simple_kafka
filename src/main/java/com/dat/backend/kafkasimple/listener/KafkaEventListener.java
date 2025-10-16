package com.dat.backend.kafkasimple.listener;

import org.springframework.context.event.EventListener;
import org.springframework.kafka.event.ConsumerStoppedEvent;
import org.springframework.kafka.event.ListenerContainerIdleEvent;
import org.springframework.kafka.event.ListenerContainerPartitionIdleEvent;
import org.springframework.kafka.event.NonResponsiveConsumerEvent;
import org.springframework.stereotype.Component;

@Component
public class KafkaEventListener {

    @EventListener
    public void handleIdleListener(ListenerContainerIdleEvent event) {
        // Handle the idle event (e.g., log it, send a notification, etc.)
        System.out.println("Listener container is idle: " + event.getListenerId());
    }

    @EventListener
    public void handlePartitionIdle(ListenerContainerPartitionIdleEvent event) {
        // Handle the partition idle event (e.g., log it, send a notification, etc.)
        System.out.println("Partition is idle: " + event.getListenerId() + " for partition: " + event.getTopicPartition());
    }

    @EventListener
    public void handleNonResponsiveConsumer(NonResponsiveConsumerEvent event) {
        // Handle the non-responsive consumer event (e.g., log it, send a notification, etc.)
        System.out.println("Non-responsive consumer detected: " + event.getConsumer());
    }

    @EventListener
    public void handleKickedOutConsumer(ConsumerStoppedEvent event) {
        // Handle the non-responsive consumer event (e.g., log it, send a notification, etc.)
        System.out.println("Non-responsive consumer detected: " + event.getReason());
    }

}
