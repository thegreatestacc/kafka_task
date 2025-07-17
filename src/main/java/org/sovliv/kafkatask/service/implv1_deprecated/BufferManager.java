package org.sovliv.kafkatask.service.implv1_deprecated;

import lombok.extern.slf4j.Slf4j;
import org.sovliv.kafkatask.entities.SomeData;
import org.sovliv.kafkatask.service.DBWriter;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

import static org.sovliv.kafkatask.utils.Constants.BATCH_SIZE;
import static org.sovliv.kafkatask.utils.Constants.BUFFER_LIMIT;

/**
 * @author Vladimir Solovyov
 * @project 20mln
 * @date on 17/06/2025
 */

@Slf4j
@Component
public class BufferManager {

    private final List<BlockingQueue<SomeData>> activeQueues = new CopyOnWriteArrayList<>();
    private volatile BlockingQueue<SomeData> currentQueue = new LinkedBlockingQueue<>();
    private final Semaphore dbWriteLimiter = new Semaphore(50); // ≤ maximumPoolSize

    private final ExecutorService pool = Executors.newVirtualThreadPerTaskExecutor();
    private final DBWriter dbWriter;

    public BufferManager(DBWriter dbWriter) {
        this.dbWriter = dbWriter;
        this.activeQueues.add(currentQueue);
    }

    public void enqueue(SomeData data) throws InterruptedException {
        currentQueue.put(data);
        if (currentQueue.size() >= BUFFER_LIMIT) {
            rotateQueue();
        }
    }

    private synchronized void rotateQueue() {
        BlockingQueue<SomeData> fullQueue = currentQueue;
        currentQueue = new LinkedBlockingQueue<>();
        activeQueues.add(currentQueue);

        while (!fullQueue.isEmpty()) {
            List<SomeData> batch = new ArrayList<>(BATCH_SIZE);
            fullQueue.drainTo(batch, BATCH_SIZE);
            if (!batch.isEmpty()) {
                pool.submit(() -> {
                    try {
                        dbWriteLimiter.acquire();
//                        dbWriter.saveBuffer(batch);
                    } catch (Exception e) {
                        log.error("Flush error: {}", e.getMessage());
                    } finally {
                        dbWriteLimiter.release();
                    }
                });
            }
        }
    }

    public void shutdown() {
        pool.shutdown();
    }
}
