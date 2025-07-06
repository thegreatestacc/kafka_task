package org.sovliv.kafkatask.service.implv2;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

/**
 * @author Vladimir Solovyov
 * @project 20mln
 * @date on 06/07/2025
 */

@Slf4j
@Component
@RequiredArgsConstructor
public class StagingFlusherService {

    private final StagingTransactionalFlusher stagingTransactionalFlusher;

    // Пока что отключил
    // Периодически вызывается автоматически
    // @Scheduled(fixedDelay = 5000) // каждые 5 секунд
    public void flush() {
        try {
            stagingTransactionalFlusher.mergeAndClean();
        } catch (Exception e) {
            log.error("Failed to flush staging: {}", e.getMessage(), e);
        }
    }
}
