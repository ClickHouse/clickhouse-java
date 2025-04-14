package com.clickhouse.demo_service;

import com.clickhouse.demo_service.data.UIEvent;
import com.clickhouse.demo_service.jpa.UIEventsDbRepository;
import jakarta.annotation.PostConstruct;
import jakarta.persistence.EntityManager;
import jakarta.transaction.Transactional;
import lombok.extern.java.Log;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.web.bind.annotation.*;

import java.util.Collection;

/**
 * Class demonstrates usage of ClickHouse JDBC driver with JPA
 *
 */
@RestController
@RequestMapping("/events/")
@Log
public class JPAInsertController {

    @Autowired
    private UIEventsDbRepository uiEvents;

    @Autowired
    private PlatformTransactionManager transactionManager;

    @Autowired
    private EntityManager entityManager;

    private TransactionTemplate transactionTemplate;

    @PostConstruct
    public void setUp() {
        transactionTemplate = new TransactionTemplate(transactionManager);
    }

    @PostMapping("/ui_events")
    @Transactional(Transactional.TxType.NOT_SUPPORTED)
    public void addUIEvent(@RequestBody UIEvent event) {
        // do input validation and conversion
        transactionTemplate.executeWithoutResult(transactionStatus -> {
            entityManager.persist(event);
        });
    }

    @GetMapping("/ui_events")
    public Collection<UIEvent> lastUIEvents() {
        return uiEvents.findAll();
    }
}
