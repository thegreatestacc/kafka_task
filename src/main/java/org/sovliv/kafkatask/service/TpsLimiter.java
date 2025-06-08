package org.sovliv.kafkatask.service;

import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

/**
 * @author Vladimir Solovyov
 * @project KafkaTask
 * @date on 02/06/2025
 */

public class TpsLimiter {
    private final Semaphore permits;
    private final int tps;

    public TpsLimiter(int tps) {
        this.tps = tps;
        this.permits = new Semaphore(tps);

        // Обновляет лимит каждую секунду
        Executors.newSingleThreadScheduledExecutor().scheduleAtFixedRate(() -> {
            int toRelease = this.tps - permits.availablePermits();
            if (toRelease > 0) {
                permits.release(toRelease);
            }
        }, 1, 1, TimeUnit.SECONDS);
    }

    public void acquire() throws InterruptedException {
        permits.acquire();
    }
}

