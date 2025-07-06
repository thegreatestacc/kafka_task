package org.sovliv.kafkatask.service.implv2;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

/**
 * @author Vladimir Solovyov
 * @project 20mln
 * @date on 06/07/2025
 */

@Slf4j
@Component
@RequiredArgsConstructor
public class StagingTransactionalFlusher {

    private final StagingMerger stagingMerger;
    private final StagingCleaner stagingCleaner;

    // Отключен
    @Transactional
    public void mergeAndClean() {
        stagingMerger.mergeStagingIntoMain();
        stagingCleaner.cleanTable();
    }
}
