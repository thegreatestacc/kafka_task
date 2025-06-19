package org.sovliv.kafkatask.service.implv1_deprecated;

import jakarta.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.sovliv.kafkatask.entities.SomeData;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.UUID;

import static org.sovliv.kafkatask.utils.Constants.DATA_TOPIC;

/**
 * @author Vladimir Solovyov
 * @project 20mln
 * @date on 17/06/2025
 */

@Service
//@Primary
@RequiredArgsConstructor
@Slf4j
public class MessageProcessorJDBCWithBuffer implements MessageProcessor {

    private final BufferManager bufferManager;

    @KafkaListener(topics = DATA_TOPIC)
    public void listener(List<ConsumerRecord<UUID, String>> records, Acknowledgment ack) {
        try {
            for (var record : records) {
                bufferManager.enqueue(new SomeData(record.key(), record.value()));
            }
            ack.acknowledge();
        } catch (Exception e) {
            Thread.currentThread().interrupt();
            log.error("enqueue error", e);
        }
    }

    @PreDestroy
    public void shutdown() {
        bufferManager.shutdown();
    }

    @Override
    public boolean isDbOverloaded() {
        return false;
    }

    @Override
    public void shutdownAndFlush() {

    }

    @Override
    public boolean isQueueDrained() {
        return false;
    }
}