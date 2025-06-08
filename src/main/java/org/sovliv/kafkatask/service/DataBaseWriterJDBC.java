package org.sovliv.kafkatask.service;

import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.sovliv.kafkatask.entities.SomeData;
import org.springframework.context.annotation.Primary;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

import static org.sovliv.kafkatask.utils.Constants.*;

/**
 * @author Vladimir Solovyov
 * @project KafkaTask
 * @date on 02/06/2025
 */

@Service
@Primary
@RequiredArgsConstructor
public class DataBaseWriterJDBC implements DataBaseWriter {
    private final JdbcTemplate jdbcTemplate;
    private final DataBaseToKafkaService dataBaseToKafkaService;

    private final AtomicLong counter = new AtomicLong(0);
    private final BlockingQueue<SomeData> dbQueue = new LinkedBlockingQueue<>(QUEUE_CAPACITY);
    private final ExecutorService executorService = Executors.newVirtualThreadPerTaskExecutor();
    private volatile boolean running = true;

    @KafkaListener(topics = DATA_TOPIC)
    public void listener(List<ConsumerRecord<UUID, String>> records) {
        for (var record : records) {
            try {
                dbQueue.put(new SomeData(record.key(), record.value()));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                System.err.println(INTERRUPTED_WHILE_PUTTING_TO_QUEUE + e.getMessage());
            }
        }
    }

    @PostConstruct
    public void startDbWriters() {
        for (int i = 0; i < WORKER_THREADS; i++) {
            executorService.submit(() -> {
                List<SomeData> buffer = new ArrayList<>(BATCH_SIZE);
                while (running || !dbQueue.isEmpty()) {
                    try {
                        SomeData data = dbQueue.poll(1, TimeUnit.SECONDS);
                        if (data != null) buffer.add(data);

                        if (buffer.size() >= BATCH_SIZE || (!running && !buffer.isEmpty())) {
                            saveBuffer(new ArrayList<>(buffer));
                            counter.addAndGet(buffer.size());
                            buffer.clear();
                        }
                    } catch (Exception e) {
                        System.err.println("DB error: " + e.getMessage());
                    }
                }
            });
        }
    }

    @Transactional
    public void saveBuffer(List<SomeData> bufferToWrite) {
        if (bufferToWrite.isEmpty()) return;
        String sql = "INSERT INTO some_data (id, value) VALUES (?, ?) ON CONFLICT (id) DO NOTHING";
        jdbcTemplate.batchUpdate(
                sql,
                bufferToWrite,
                bufferToWrite.size(),
                (ps, someData) -> {
                    ps.setObject(1, someData.getId());
                    ps.setString(2, someData.getValue());
                }
        );
    }

    public boolean isDbOverloaded() {
        return !dbQueue.isEmpty();
    }

    public boolean isQueueDrained() {
        return dbQueue.isEmpty();
    }

    public void shutdownAndFlush() {
        this.running = false;
    }
}
