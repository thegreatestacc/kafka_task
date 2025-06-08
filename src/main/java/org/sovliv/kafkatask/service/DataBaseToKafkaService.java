package org.sovliv.kafkatask.service;

import lombok.RequiredArgsConstructor;
import org.sovliv.kafkatask.repository.SomeDataRepository;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.util.UUID;

import static org.sovliv.kafkatask.utils.Constants.DATA_TOPIC;

/**
 * @author Vladimir Solovyov
 * @project KafkaTask
 * @date on 31/05/2025
 */

@Service
@RequiredArgsConstructor
public class DataBaseToKafkaService {

    private final SomeDataRepository repository;
    private final KafkaTemplate<UUID, String> kafkaTemplate;

    public void publishAllToKafka() {
        repository.findAll().forEach(entity ->
                kafkaTemplate.send(DATA_TOPIC, entity.getId(), entity.getValue())
        );
    }
}

