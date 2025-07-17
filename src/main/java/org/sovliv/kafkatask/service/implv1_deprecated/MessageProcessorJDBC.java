package org.sovliv.kafkatask.service.implv1_deprecated;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.sovliv.kafkatask.entities.SomeData;
import org.sovliv.kafkatask.service.DBWriter;
import org.springframework.kafka.support.Acknowledgment;
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
 * @date on 02/06/2025
 */

@Slf4j
@Service
//@Primary
@RequiredArgsConstructor
public class MessageProcessorJDBC implements MessageProcessor {

    private final DBWriter dbWriter;

    private final Semaphore semaphore = new Semaphore(30);
    private final AtomicLong counter = new AtomicLong(0);
    private final BlockingQueue<AckData> dbQueue = new LinkedBlockingQueue<>(QUEUE_CAPACITY);
    private final ExecutorService executorService = Executors.newVirtualThreadPerTaskExecutor();
    private final List<Future<?>> workerFutures = new ArrayList<>();

    private volatile boolean running = true;

//    @KafkaListener(topics = DATA_TOPIC)
    public void listener(List<ConsumerRecord<UUID, String>> records, Acknowledgment ack) {
        try {
            for (var record : records) {
                SomeData data = new SomeData(record.key(), record.value());
                dbQueue.put(new AckData(data, ack));
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.error("Interrupted while putting to queue: {}", e.getMessage());
        }
    }

    @PostConstruct
    public void startDbWriters() {
        for (int i = 0; i < WORKER_THREADS; i++) {
            Future<?> future = executorService.submit(() -> {
                List<AckData> ackBuffer = new ArrayList<>(BATCH_SIZE);
                while (running || !dbQueue.isEmpty()) {
                    try {
                        AckData ackData = dbQueue.poll(1, TimeUnit.SECONDS);
                        if (ackData != null) ackBuffer.add(ackData);

                        if (ackBuffer.size() >= BATCH_SIZE || (!running && !ackBuffer.isEmpty())) {
                            flushWithAck(ackBuffer);
                        }
                    } catch (Exception e) {
                        log.error("Unexpected DB thread error: {}", e.getMessage());
                    }
                }

                if (!ackBuffer.isEmpty()) {
                    flushWithAck(ackBuffer);
                }
            });
            workerFutures.add(future);
        }
    }

    private void flushWithAck(List<AckData> ackBuffer) {
        List<SomeData> dataBatch = new ArrayList<>(ackBuffer.size());
        for (AckData ackData : ackBuffer) dataBatch.add(ackData.data());

        Acknowledgment ack = ackBuffer.getLast().ack();

        boolean success = false;
        int attempts = 3;

        while (attempts-- > 0 && !success) {
            try {
                semaphore.acquire();
//                dbWriter.saveBuffer(dataBatch);
                counter.addAndGet(dataBatch.size());
                success = true;
            } catch (Exception e) {
                log.warn("Retry DB write due to: {}", e.getMessage());
                try {
                    Thread.sleep(100);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    return;
                }
            } finally {
                semaphore.release();
            }
        }

        if (success) {
            ack.acknowledge();
            ackBuffer.clear();
        } else {
            log.error("Failed DB write, batch will retry on next pass.");
        }
    }

    @PreDestroy
    public void shutdownAndFlush() {
        this.running = false;
        executorService.shutdown();
        try {
            if (!executorService.awaitTermination(1, TimeUnit.MINUTES)) {
                executorService.shutdownNow();
            }
            for (Future<?> future : workerFutures) {
                try {
                    future.get();
                } catch (InterruptedException | ExecutionException e) {
                    log.error("Worker did not terminate cleanly: {}", e.getMessage());
                }
            }
        } catch (InterruptedException e) {
            executorService.shutdownNow();
            Thread.currentThread().interrupt();
        }
        log.info("All workers finished. Total inserted: {}", counter.get());
    }

    public boolean isDbOverloaded() {
        return dbQueue.remainingCapacity() < BATCH_SIZE * 2;
    }

    public boolean isQueueDrained() {
        return dbQueue.isEmpty();
    }

    public record AckData(SomeData data, Acknowledgment ack) {}
/*
    private final DBWriter dbWriter;
    private final DataBaseToKafkaService dataBaseToKafkaService;

    private final Semaphore semaphore = new Semaphore(30);
    private final AtomicLong counter = new AtomicLong(0);
    private final BlockingQueue<SomeData> dbQueue = new LinkedBlockingQueue<>(QUEUE_CAPACITY);
    private final ExecutorService executorService = Executors.newVirtualThreadPerTaskExecutor();
    private final List<Future<?>> workerFutures = new ArrayList<>();

    private volatile boolean running = true;

    @KafkaListener(topics = DATA_TOPIC)
    public void listener(List<ConsumerRecord<UUID, String>> records, Acknowledgment ack) {
        try {
            for (var record : records) {
                dbQueue.put(new SomeData(record.key(), record.value()));
            }
            ack.acknowledge();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.error(INTERRUPTED_WHILE_PUTTING_TO_QUEUE + "{}", e.getMessage());
        }
    }

    @PostConstruct
    public void startDbWriters() {
        for (int i = 0; i < WORKER_THREADS; i++) {
            Future<?> future = executorService.submit(() -> {
                List<SomeData> buffer = new ArrayList<>(BATCH_SIZE);
                while (running || !dbQueue.isEmpty()) {
                    try {
                        SomeData data = dbQueue.poll(1, TimeUnit.SECONDS);
                        if (data != null) buffer.add(data);

                        if (buffer.size() >= BATCH_SIZE || (!running && !buffer.isEmpty())) {
                            semaphore.acquire();
                            try {
                                dbWriter.saveBuffer(new ArrayList<>(buffer)); // новая копия
                                counter.addAndGet(buffer.size());
                                buffer.clear();
                            } catch (Exception e) {
                                log.error(String.format("DB error while writing %d records: %s%n", buffer.size(), e.getMessage()));
                            } finally {
                                semaphore.release();
                            }
                        }
                    } catch (Exception e) {
                        log.error("Unexpected DB thread error: {}", e.getMessage());
                    }
                }
            });
            workerFutures.add(future);
        }
    }

    @PreDestroy
    public void shutdownAndFlush() {
        this.running = false;
        executorService.shutdown();
        try {
            if (!executorService.awaitTermination(1, TimeUnit.MINUTES)) {
                executorService.shutdownNow();
            }
            for (Future<?> future : workerFutures) {
                try {
                    future.get(); // проверка завершения без исключений
                } catch (InterruptedException | ExecutionException e) {
                    log.error("Worker did not terminate cleanly: {}", e.getMessage());
                }
            }
        } catch (InterruptedException e) {
            executorService.shutdownNow();
            Thread.currentThread().interrupt();
        }
        log.info(String.format("All workers finished. Total inserted: %d%n", counter.get()));
    }

    public boolean isDbOverloaded() {
        return !dbQueue.isEmpty();
    }

    public boolean isQueueDrained() {
        return dbQueue.isEmpty();
    }*/
}
