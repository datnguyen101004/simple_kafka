package com.dat.backend.kafkasimple.listener.producer;

public interface IProducerListener<K, V> {
    default void onSuccess(K key, V value) {
    };

    default void onError(K key, V value, Exception exception) {
    }
}
