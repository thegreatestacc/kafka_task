package org.sovliv.kafkatask.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;
import org.sovliv.kafkatask.entities.SomeData;
import org.springframework.stereotype.Component;

import javax.sql.DataSource;
import java.io.StringReader;
import java.sql.Connection;
import java.util.List;

import static org.sovliv.kafkatask.utils.Constants.COPY_QUERY;

/**
 * @author Vladimir Solovyov
 * @project 20mln
 * @date on 17/06/2025
 */

@Slf4j
@Component
@RequiredArgsConstructor
public class DBWriter {

    private final DataSource dataSource;

    public void saveBuffer(List<SomeData> buffer) {
        if (buffer.isEmpty()) return;

        try (Connection conn = dataSource.getConnection()) {
            CopyManager copyManager = new CopyManager(conn.unwrap(BaseConnection.class));

            StringBuilder builder = new StringBuilder(buffer.size() * 50);
            for (SomeData data : buffer) {
                builder.append(data.getId())
                        .append('\t')
                        .append(data.getValue())
                        .append('\n');
            }

            try (StringReader reader = new StringReader(builder.toString())) {
                copyManager.copyIn(COPY_QUERY, reader);
            }

        } catch (Exception e) {
            log.error("COPY failed: {} ", e.getMessage());
            throw new RuntimeException(e);
        }
    }

    //оставил, просто для того что бы было понятно, что разбирался почему нельзя вызывать transactional внутри public методов
    /*private final JdbcTemplate jdbcTemplate;

    @Transactional
    public void saveBuffer(List<SomeData> bufferToWrite) {
        if (bufferToWrite.isEmpty()) return;

        jdbcTemplate.batchUpdate(
                INSERT_QUERY,
                bufferToWrite,
                bufferToWrite.size(),
                (ps, someData) -> {
                    ps.setObject(1, someData.getId());
                    ps.setString(2, someData.getValue());
                }
        );
    }*/
}
