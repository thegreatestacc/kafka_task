package org.sovliv.kafkatask.service.implv2;

import lombok.extern.slf4j.Slf4j;
import org.sovliv.kafkatask.entities.SomeData;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

import static org.sovliv.kafkatask.utils.Constants.QUEUE_CAPACITY;

/**
 * @author Vladimir Solovyov
 * @project 20mln
 * @date on 18/06/2025
 */

@Slf4j
@Component
public class MessageBuffer {
    private final BlockingQueue<SomeData> queue = new ArrayBlockingQueue<>(QUEUE_CAPACITY);

    public MessageBuffer() {
        ScheduledExecutorService monitor = Executors.newSingleThreadScheduledExecutor();
        monitor.scheduleAtFixedRate(() ->
                log.info("Queue size: {}", queue.size()), 0, 5, TimeUnit.SECONDS);
    }

    public void addBatch(List<SomeData> batch) {
        for (SomeData data : batch) {
            try {
                queue.put(data);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    public List<SomeData> drainBatch(int maxSize) {
        List<SomeData> batch = new ArrayList<>(maxSize);
        queue.drainTo(batch, maxSize);
        return batch;
    }
}
