package org.sovliv.kafkatask.repository;

import org.sovliv.kafkatask.entities.SomeData;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.UUID;

/**
 * @author Vladimir Solovyov
 * @project KafkaTask
 * @date on 31/05/2025
 */

public interface SomeDataRepository extends JpaRepository<SomeData, UUID> {
}
