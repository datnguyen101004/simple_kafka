package com.dat.backend.kafkasimple.controller;

import com.dat.backend.kafkasimple.service.SendService;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequiredArgsConstructor
public class KafkaController {
    private final SendService sendService;

    @GetMapping("/send-async")
    public String send(@RequestParam String message) {
        return sendService.sendAsyncMessage(message);
    }

    @GetMapping("/send-sync")
    public String sendSync(@RequestParam String message) {
        return sendService.sendSyncMessage(message);
    }
}
