package org.sovliv.kafkatask.mapper;

import org.mapstruct.Mapper;
import org.mapstruct.Mapping;
import org.sovliv.kafkatask.dto.SomeDataDTO;
import org.sovliv.kafkatask.entities.SomeData;

import java.util.UUID;

/**
 * @author Vladimir Solovyov
 * @project 20mln
 * @date on 19/06/2025
 */

@Mapper(componentModel = "spring", imports = UUID.class)
public interface SomeDataMapper {

    @Mapping(target = "id", expression = "java(UUID.randomUUID())")
    SomeData toEntity(SomeDataDTO dto);
}
