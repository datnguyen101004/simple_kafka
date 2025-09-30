package com.dat.backend.kafkasimple.listener;

public interface IProducerListener<K, V> {
    default void onSuccess(K key, V value, long duration) {
    };

    default void onError(K key, V value, Exception exception) {
    }
}
