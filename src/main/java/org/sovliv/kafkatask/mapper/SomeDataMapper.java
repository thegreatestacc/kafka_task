package org.sovliv.kafkatask.mapper;

import org.sovliv.kafkatask.dto.SomeDataDTO;
import org.sovliv.kafkatask.entities.SomeData;
import org.springframework.stereotype.Component;

import java.util.UUID;

/**
 * @author Vladimir Solovyov
 * @project 20mln
 * @date on 19/06/2025
 */

@Component
public class SomeDataMapper {

    public SomeData toEntity(SomeDataDTO dto) {
        return new SomeData(UUID.randomUUID(), dto.getValue());
    }
}
