package org.sovliv.kafkatask.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.sovliv.kafkatask.entities.SomeData;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

import static org.sovliv.kafkatask.utils.Constants.STAGING_INSERT;

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

    @Transactional
    public void saveBuffer(List<SomeData> bufferToWrite) {
        if (bufferToWrite.isEmpty()) return;
        jdbcTemplate.batchUpdate(
                STAGING_INSERT,
                bufferToWrite,
                bufferToWrite.size(),
                (ps, someData) -> {
                    ps.setObject(1, someData.getId());
                    ps.setString(2, someData.getValue());
                }
        );
    }
}
