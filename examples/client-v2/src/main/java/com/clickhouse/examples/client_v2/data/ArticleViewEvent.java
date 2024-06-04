package com.clickhouse.examples.client_v2.data;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

/**
 * <p>POJO class representing an event emitted by UI to register a fact of an article being viewed.
 *  It is used to demonstrate how to insert data using new client API.</p>
 */
@AllArgsConstructor
@Data
@NoArgsConstructor
public class ArticleViewEvent {
    private Double postId;
    private LocalDateTime viewTime;
    private String clientId;

}
