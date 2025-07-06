package org.sovliv.kafkatask.service.implv2;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import static org.sovliv.kafkatask.utils.Constants.INSERT_FROM_STAGING;

/**
 * @author Vladimir Solovyov
 * @project 20mln
 * @date on 06/07/2025
 */

@Slf4j
@Component
@RequiredArgsConstructor
public class StagingMerger {
    private final JdbcTemplate jdbcTemplate;

    // Отключен
    public void mergeStagingIntoMain() {
        int inserted = jdbcTemplate.update(INSERT_FROM_STAGING);
        log.info("Merged {} rows from staging", inserted);
    }
}
