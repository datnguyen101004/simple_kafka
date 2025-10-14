package com.dat.backend.kafkasimple.controller;

import com.dat.backend.kafkasimple.dto.Message;
import com.dat.backend.kafkasimple.service.SendService;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequiredArgsConstructor
public class KafkaController {
    private final SendService sendService;

    @GetMapping("/send")
    public String sendMessage(@RequestBody List<Message> messages) {
        return sendService.sendMessage(messages);
    }
}
