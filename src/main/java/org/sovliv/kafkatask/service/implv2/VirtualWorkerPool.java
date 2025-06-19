package org.sovliv.kafkatask.service.implv2;

import lombok.RequiredArgsConstructor;
import org.sovliv.kafkatask.entities.SomeData;
import org.sovliv.kafkatask.service.DBWriter;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.sovliv.kafkatask.utils.Constants.BATCH_SIZE;
import static org.sovliv.kafkatask.utils.Constants.WORKER_THREADS;

/**
 * @author Vladimir Solovyov
 * @project 20mln
 * @date on 18/06/2025
 */

@Component
@RequiredArgsConstructor
public class VirtualWorkerPool implements InitializingBean {

    private final MessageBuffer buffer;
    private final DBWriter dbWriter;

    @Override
    public void afterPropertiesSet() {
        ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();
        for (int i = 0; i < WORKER_THREADS; i++) {
            executor.submit(() -> {
                while (true) {
                    List<SomeData> batch = buffer.drainBatch(BATCH_SIZE);
                    if (!batch.isEmpty()) {
                        dbWriter.saveBuffer(batch);
                    } else {
                        Thread.sleep(10); // короткая пауза, чтобы не крутилось в холостую
                    }
                }
            });
        }
    }
}
