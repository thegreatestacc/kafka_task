package org.sovliv.kafkatask.service.implv2;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.sovliv.kafkatask.dto.SomeDataDTO;
import org.sovliv.kafkatask.entities.SomeData;
import org.sovliv.kafkatask.mapper.SomeDataMapper;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

import static org.sovliv.kafkatask.utils.Constants.DATA_TOPIC;

/**
 * @author Vladimir Solovyov
 * @project 20mln
 * @date on 18/06/2025
 */

@Slf4j
@Component
@RequiredArgsConstructor
public class KafkaBatchConsumer {

    private final MessageBuffer buffer;
    private final ObjectMapper objectMapper;
    private final SomeDataMapper mapper;

    @KafkaListener(topics = DATA_TOPIC)
    public void listen(List<String> messages, Acknowledgment ack) {
        List<SomeData> batch = new ArrayList<>(messages.size());

        for (String msg : messages) {
            try {
                SomeDataDTO dto = objectMapper.readValue(msg, SomeDataDTO.class);
                SomeData entity = mapper.toEntity(dto);
                batch.add(entity);
            } catch (Exception e) {
                log.error("Failed to deserialize: {}", msg);
                e.printStackTrace();
            }
        }
        buffer.addBatch(batch);
        ack.acknowledge();
    }
}
