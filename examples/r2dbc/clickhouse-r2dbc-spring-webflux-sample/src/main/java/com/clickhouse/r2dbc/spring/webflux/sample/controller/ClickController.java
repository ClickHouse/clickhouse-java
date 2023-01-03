package com.clickhouse.r2dbc.spring.webflux.sample.controller;

import com.clickhouse.r2dbc.spring.webflux.sample.model.Click;
import com.clickhouse.r2dbc.spring.webflux.sample.model.ClickStats;
import com.clickhouse.r2dbc.spring.webflux.sample.repository.ClickRepository;
import org.reactivestreams.Publisher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.LocalDateTime;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

@RestController
@RequestMapping("/clicks")
public class ClickController {

    @Autowired
    ClickRepository clickRepository;

    @GetMapping("/{domain}")
    public Publisher<List<ClickStats>> getEmployeeById(@PathVariable("domain") String domain) {
        return Flux.from(clickRepository.getStatsByDomain(domain).collect(Collectors.toList()));
    }

    @PostMapping
    @ResponseStatus(HttpStatus.CREATED)
    public Mono<Void> add(@RequestBody  Click click){
        return Mono.from(clickRepository.add(click));


    }

}
