package org.sovliv.kafkatask.service;

import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.sovliv.kafkatask.entities.SomeData;
import org.sovliv.kafkatask.repository.SomeDataRepository;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

import static org.sovliv.kafkatask.utils.Constants.*;

/**
 * @author Vladimir Solovyov
 * @project KafkaTask
 * @date on 31/05/2025
 */

@Service
@RequiredArgsConstructor
public class DataBaseWriterHiber implements DataBaseWriter {

    private final SomeDataRepository repository;
    private final DataBaseToKafkaService dataBaseToKafkaService;

    private final AtomicLong counter = new AtomicLong(0);
    private final BlockingQueue<SomeData> dbQueue = new LinkedBlockingQueue<>(QUEUE_CAPACITY);
    private final ExecutorService executorService = Executors.newVirtualThreadPerTaskExecutor();
    private volatile boolean running = true;

    @KafkaListener(topics = DATA_TOPIC)
    public void listener(List<ConsumerRecord<UUID, String>> records) {
        for (var record : records) {
            dbQueue.offer(new SomeData(record.value()));
        }
    }

    @PostConstruct
    public void startDbWriters() {
        for (int i = 0; i < WORKER_THREADS; i++) {
            executorService.submit(() -> {
                List<SomeData> buffer = new ArrayList<>(BATCH_SIZE);

                while (true) {
                    try {
                        SomeData data = dbQueue.poll(1, TimeUnit.SECONDS);
                        if (data != null) buffer.add(data);

                        if (buffer.size() >= BATCH_SIZE) {
                            repository.saveAll(new ArrayList<>(buffer));
                            counter.addAndGet(buffer.size());
                            buffer.clear();
                        }
                    } catch (Exception e) {
                        System.err.println(DB_ERROR + e.getMessage());
                    }
                }
            });
        }
    }

    public boolean isQueueDrained() {
        return dbQueue.isEmpty();
    }

    // Завершение записи — можно вызвать вручную в конце работы
    public void shutdownAndFlush() {
        this.running = false;
    }

    public boolean isDbOverloaded() {
        return !dbQueue.isEmpty();
    }
}

