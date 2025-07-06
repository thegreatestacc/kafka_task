package org.sovliv.kafkatask.service.implv2;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import static org.sovliv.kafkatask.utils.Constants.TRUNCATE_STAGING_TABLE;

/**
 * @author Vladimir Solovyov
 * @project 20mln
 * @date on 06/07/2025
 */

@Slf4j
@Component
@RequiredArgsConstructor
public class StagingCleaner {

    private final JdbcTemplate jdbcTemplate;

    public void cleanTable() {
        final int truncated = jdbcTemplate.update(TRUNCATE_STAGING_TABLE);
        log.info("Truncated {} rows from staging", truncated);
    }
}
