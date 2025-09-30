package com.dat.backend.kafkasimple.listener;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class ProducerListener implements IProducerListener<String, String> {

    @Override
    public void onError(String key, String value, Exception exception) {
        log.error("Error in ProducerListener: {}", exception.getMessage());
        // TODO: implement retry logic or fallback mechanism
    }
}
