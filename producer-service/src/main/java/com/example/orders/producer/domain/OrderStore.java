package com.example.orders.producer.domain;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

/**
 * Persistence port of the producer service.
 *
 * <p>Every method is its own transaction boundary (implemented by {@link JpaOrderStore}). Keeping the
 * port small is what allows {@code OrderApplicationService} to publish to Kafka <em>outside</em> a
 * database transaction, and it lets unit tests swap in an in-memory fake.</p>
 */
public interface OrderStore {

    Order save(Order order);

    Optional<Order> find(UUID id);

    /** Loads an order or fails with {@link java.util.NoSuchElementException}. */
    Order require(UUID id);

    Optional<Order> findByEventId(UUID eventId);

    List<Order> findAll(int limit);

    List<Order> findByCustomer(String customerId, int limit);

    /** Orders the broker never acknowledged, oldest first. */
    List<Order> findUnpublished(Instant createdBefore, int limit);

    /** Guards the relay against re-publishing an order another instance already handled. */
    boolean isStillUnpublished(UUID id);

    void markPublished(UUID id, Instant publishedAt);

    void markPublishFailed(UUID id);
}
