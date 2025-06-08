package org.sovliv.kafkatask.controller;

import lombok.RequiredArgsConstructor;
import org.sovliv.kafkatask.service.ProducerService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import static org.sovliv.kafkatask.utils.Constants.RESPONSE_ENTITY_BODY;

/**
 * @author Vladimir Solovyov
 * @project KafkaTask
 * @date on 31/05/2025
 */

@RestController
@RequestMapping("/produce")
@RequiredArgsConstructor
public class ProduceController {

    private final ProducerService producerService;

    @PostMapping
    public ResponseEntity<String> produce(@RequestParam(defaultValue = "20000000") int count) {
        producerService.sendMessages(count);
        return ResponseEntity.ok(String.format(RESPONSE_ENTITY_BODY, count));
    }

    @GetMapping("/ping")
    public ResponseEntity<String> ping() {
        return ResponseEntity.ok("pong");
    }
}

