package org.sovliv.kafkatask.service;

import jakarta.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.sovliv.kafkatask.entities.SomeData;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionTemplate;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.sovliv.kafkatask.utils.Constants.*;

/**
 * @author Vladimir Solovyov
 * @project 20mln
 * @date on 17/06/2025
 */

@Slf4j
@Component
@RequiredArgsConstructor
public class DBWriter {

    private final JdbcTemplate jdbcTemplate;
    private final TransactionTemplate transactionTemplate;
    private final ExecutorService executor = Executors.newFixedThreadPool(WORKER_THREADS);

    public CompletableFuture<Void> saveBufferAsync(List<SomeData> bufferToWrite) {
        if (bufferToWrite.isEmpty()) {
            return CompletableFuture.completedFuture(null);
        }

        return CompletableFuture.runAsync(() -> {
            List<List<SomeData>> subBatches = new ArrayList<>();
            for (int i = 0; i < bufferToWrite.size(); i += BATCH_SIZE)
                subBatches.add(bufferToWrite.subList(i, Math.min(i + BATCH_SIZE, bufferToWrite.size())));

            for (List<SomeData> subBatch : subBatches) {
                transactionTemplate.execute(_ -> {
                    jdbcTemplate.batchUpdate(
                            INSERT_QUERY,
                            subBatch,
                            subBatch.size(),
                            (ps, someData) -> {
                                ps.setObject(1, someData.getId());
                                ps.setString(2, someData.getValue());
                            }
                    );
                    log.info(SAVE_SUB_BATCH, subBatch.size());
                    return null;
                });
            }
        }, executor);
    }

    @PreDestroy
    public void shutdown() {
        executor.shutdown();
    }
}

