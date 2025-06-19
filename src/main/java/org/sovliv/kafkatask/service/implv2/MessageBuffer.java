package org.sovliv.kafkatask.service.implv2;

import org.sovliv.kafkatask.entities.SomeData;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import static org.sovliv.kafkatask.utils.Constants.QUEUE_CAPACITY;

/**
 * @author Vladimir Solovyov
 * @project 20mln
 * @date on 18/06/2025
 */

@Component
public class MessageBuffer {
    private final BlockingQueue<SomeData> queue = new ArrayBlockingQueue<>(QUEUE_CAPACITY);

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
