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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
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

    private final TpsLimiter limiter = new TpsLimiter(TPS_LIMIT); // 100k TPS
    private final AtomicInteger counter = new AtomicInteger();
    private final AtomicInteger sentCounter = new AtomicInteger(0); // глобальный счётчик отправленных сообщений
    private final ObjectMapper mapper;

    private final KafkaTemplate<UUID, String> kafkaTemplate;

    public void sendMessages(int totalCount) {
        log.info(String.format(STARTING_TO_SEND_MESSAGES, totalCount));

        ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();
        CountDownLatch latch = new CountDownLatch(VIRTUAL_WORKERS);

        for (int i = 0; i < VIRTUAL_WORKERS; i++) {
            executor.submit(() -> {
                try {
                    while (true) {
                        int start = counter.getAndAdd(BATCH_SIZE);
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
        List<CompletableFuture<SendResult<UUID, String>>> futures = new ArrayList<>(to - from);

        for (int i = from; i < to; i++) {
            try {
                limiter.acquire();

                String value = SOME_DATA_VALUE + i;
                SomeDataDTO someDataDTO = new SomeDataDTO(value);
                String json = mapper.writeValueAsString(someDataDTO);

                CompletableFuture<SendResult<UUID, String>> future =
                        kafkaTemplate.send(DATA_TOPIC, json).toCompletableFuture();
                futures.add(future);

            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.error(INTERRUPTED_DURING_SEND + "{}", e.getMessage());
                break;
            } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }
        }

        for (CompletableFuture<SendResult<UUID, String>> future : futures) {
            try {
                future.get(); // ждём подтверждение отправки
                int count = sentCounter.incrementAndGet();
                if (count % 100_000 == 0) {
                    log.info(SENT_MESSAGES + "{}", count);
                }
            } catch (Exception e) {
                log.error(SENT_FAILED + "{}", e.getMessage());
            }
        }
    }
}
