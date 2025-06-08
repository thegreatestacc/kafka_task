package org.sovliv.kafkatask.service;

import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Qualifier;
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

@Service
@RequiredArgsConstructor
public class ProducerService {

    private final TpsLimiter limiter = new TpsLimiter(TPS_LIMIT); // 100k TPS
    private final AtomicInteger counter = new AtomicInteger();
    private final AtomicInteger sentCounter = new AtomicInteger(0); // глобальный счётчик отправленных сообщений

    private final KafkaTemplate<UUID, String> kafkaTemplate;
    private final DataBaseWriter databaseWriter;

    public void sendMessages(int totalCount) {
        System.out.println(String.format(STARTING_TO_SEND_MESSAGES, totalCount));

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
            System.out.println(ALL_MESSAGES_SENT);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            System.err.println(MAIN_THREAD_INTERRUPTED);
        }

        databaseWriter.shutdownAndFlush();

        try {
            while (!databaseWriter.isQueueDrained()) {
                System.out.println(WAITING_FLUSH);
                Thread.sleep(1000);
            }
            System.out.println(FLUSH_COMPLETE);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private void sendBatch(int from, int to) {
        List<CompletableFuture<SendResult<UUID, String>>> futures = new ArrayList<>(to - from);

        for (int i = from; i < to; i++) {
            try {
                while (databaseWriter.isDbOverloaded()) {
                    Thread.sleep(50);
                }

                limiter.acquire();

                UUID key = UUID.randomUUID();
                String value = SOME_DATA_VALUE + i;

                CompletableFuture<SendResult<UUID, String>> future =
                        kafkaTemplate.send(DATA_TOPIC, key, value).toCompletableFuture();
                futures.add(future);

            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                System.err.println(INTERRUPTED_DURING_SEND + e.getMessage());
                break;
            }
        }

        for (CompletableFuture<SendResult<UUID, String>> future : futures) {
            try {
                future.get(); // ждём подтверждение отправки
                int count = sentCounter.incrementAndGet();
                if (count % 100_000 == 0) {
                    System.out.println(SENT_MESSAGES + count);
                }
            } catch (Exception e) {
                System.err.println(SENT_FAILED + e.getMessage());
            }
        }

        //commented prev implementation
/*    private final TpsLimiter limiter = new TpsLimiter(TPS_LIMIT); // 100k TPS
    private final AtomicInteger counter = new AtomicInteger();
    private final ExecutorService executor = Executors.newFixedThreadPool(PARALLEL_BATCHES);

    private final KafkaTemplate<UUID, String> kafkaTemplate;
    private final DataBaseWriter databaseWriter;

    public ProducerService(
            KafkaTemplate<UUID, String> kafkaTemplate,
            @Qualifier("dataBaseWriterJDBC")
            DataBaseWriter databaseWriter)
    {
        this.kafkaTemplate = kafkaTemplate;
        this.databaseWriter = databaseWriter;
    }

    public void sendMessages(int count) {
        counter.set(0);
        System.out.println("Starting to send " + count + " messages...");

        List<CompletableFuture<Void>> batchFutures = IntStream
                .iterate(0, i -> i < count, i -> i + BATCH_SIZE)
                .mapToObj(start -> CompletableFuture.runAsync(
                        () -> sendBatch(start, Math.min(start + BATCH_SIZE, count)), executor))
                .toList();

        CompletableFuture.allOf(batchFutures.toArray(new CompletableFuture[0])).join();

        System.out.println("Finished sending all messages.");
    }

    private void sendBatch(int from, int to) {
        List<CompletableFuture<SendResult<UUID, String>>> futures = new ArrayList<>();

        for (int i = from; i < to; i++) {
            try {
                // Ожидаем, если БД перегружена
                while (databaseWriter.isDbOverloaded()) {
                    Thread.sleep(100);
                }

                // Ограничиваем скорость
                limiter.acquire();

                // Отправка одного сообщения
                CompletableFuture<SendResult<UUID, String>> future =
                        kafkaTemplate.send(DATA_TOPIC, UUID.randomUUID(), "value" + i).toCompletableFuture();
                futures.add(future);

            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                System.err.println("Interrupted during send: " + e.getMessage());
            }
        }

        // Обработка результатов отправки
        futures.forEach(_ -> {
            int current = counter.incrementAndGet();
            if (current % 100_000 == 0) {
                System.out.println("Sent messages: " + current);
            }
        });
    }*/
    }
}
