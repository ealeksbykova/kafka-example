package com.example.orders.producer.messaging;

import com.example.orders.producer.domain.Order;
import com.example.orders.producer.domain.OrderStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.List;

/**
 * Safety net for the dual-write problem.
 *
 * <p>A database transaction and a Kafka publish cannot be made atomic without an outbox. Instead of
 * pretending otherwise, every order is persisted as {@code PUBLISH_FAILED} and only flipped to
 * {@code PUBLISHED} after the broker acknowledged the event. This relay periodically re-publishes
 * orders whose publication failed, reusing the stored {@code eventId} - which keeps the
 * consumer-side idempotency intact.</p>
 *
 * <p>Created by {@code RelayConfig} only when {@code app.relay.enabled=true}, so tests never race an
 * asynchronous job against their assertions.</p>
 */
public class OrderEventRelay {

    private static final Logger log = LoggerFactory.getLogger(OrderEventRelay.class);

    private final OrderStore orders;
    private final OrderEventPublisher publisher;
    private final Clock clock;
    private final int batchSize;
    private final Duration minAge;

    public OrderEventRelay(OrderStore orders, OrderEventPublisher publisher, Clock clock,
                           int batchSize, Duration minAge) {
        this.orders = orders;
        this.publisher = publisher;
        this.clock = clock;
        this.batchSize = batchSize;
        this.minAge = minAge;
    }

    /** Runs every {@code app.relay.interval} (5s by default). */
    @Scheduled(fixedDelayString = "${app.relay.interval:5000}")
    public void relayUnpublishedOrders() {
        List<Order> pending = orders.findUnpublished(Instant.now(clock).minus(minAge), batchSize);
        if (pending.isEmpty()) {
            return;
        }
        log.info("Relaying {} order event(s) not acknowledged by the broker", pending.size());
        pending.forEach(this::republishQuietly);
    }

    private void republishQuietly(Order order) {
        // Another instance may have published it in the meantime; the check keeps duplicates rare.
        if (!orders.isStillUnpublished(order.getId())) {
            return;
        }
        try {
            publisher.publish(OrderCreatedEventFactory.from(order));
            orders.markPublished(order.getId(), Instant.now(clock));
        } catch (RuntimeException ex) {
            log.warn("Retry failed for order {} (event {}): {}", order.getId(), order.getEventId(), ex.getMessage());
        }
    }
}
