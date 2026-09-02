package com.clickhouse.examples.repository;

import com.clickhouse.examples.model.SignalEntity;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.UUID;

/**
 * Spring Data JPA repository for signals stored in ClickHouse.
 */
public interface SignalRepository extends JpaRepository<SignalEntity, UUID> {
}
