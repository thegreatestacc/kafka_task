package org.sovliv.kafkatask.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.sovliv.kafkatask.dto.SomeDataDTO;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.sovliv.kafkatask.utils.Constants.*;

/**
 * @author Vladimir Solovyov
 * @project KafkaTask
 * @date on 31/05/2025
 */

@Slf4j
@Service
@RequiredArgsConstructor
public class ProducerService {

    private final AtomicInteger messageCount = new AtomicInteger();
    private final AtomicInteger sentCounter = new AtomicInteger(0); // глобальный счётчик отправленных сообщений
    private final ObjectMapper mapper;
    private final ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();
    private final KafkaTemplate<UUID, byte[]> kafkaTemplate;

        public void sendMessages(int totalCount) {
        log.info(STARTING_TO_SEND_MESSAGES, totalCount);

        CountDownLatch latch = new CountDownLatch(VIRTUAL_WORKERS);

        for (int i = 0; i < VIRTUAL_WORKERS; i++) {
            executor.submit(() -> {
                try {
                    while (true) {
                        int start = messageCount.getAndAdd(BATCH_SIZE);
                        if (start >= totalCount) break;
                        int end = Math.min(start + BATCH_SIZE, totalCount);
                        sendBatch(start, end);
                    }
                } finally {
                    latch.countDown();
                }
            });
        }

        try {
            latch.await();
            log.info(ALL_MESSAGES_SENT);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.error(MAIN_THREAD_INTERRUPTED);
        }
    }

    private void sendBatch(int from, int to) {
        List<CompletableFuture<SendResult<UUID, byte[]>>> futures = new ArrayList<>(to - from);

        for (int i = from; i < to; i++) {
            try {

                String value = SOME_DATA_VALUE + i;
                SomeDataDTO someDataDTO = new SomeDataDTO(value);
                byte[] json = mapper.writeValueAsBytes(someDataDTO);

                //todo спросить про kafkaTemplate.send(record).whenComplete()
                CompletableFuture<SendResult<UUID, byte[]>> future =
                        kafkaTemplate.send(DATA_TOPIC, json).toCompletableFuture();
                futures.add(future);

            } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }
        }

        for (CompletableFuture<SendResult<UUID, byte[]>> future : futures) {
            try {
                future.get(); // ждём подтверждение отправки
                int count = sentCounter.incrementAndGet();
                if (count % 100_000 == 0) {
                    log.info(SENT_MESSAGES, count);
                }
            } catch (Exception e) {
                log.error(SENT_FAILED, e.getMessage());
            }
        }
    }
}
