package com.clickhouse.demo_service.jpa;

import com.clickhouse.demo_service.data.UIEvent;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.UUID;

@Repository
public interface UIEventsDbRepository extends JpaRepository<UIEvent, UUID> {
}
