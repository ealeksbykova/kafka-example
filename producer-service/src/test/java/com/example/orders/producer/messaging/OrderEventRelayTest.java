package com.example.orders.producer.messaging;

import com.example.orders.contract.OrderCreatedEvent;
import com.example.orders.producer.domain.Order;
import com.example.orders.producer.support.InMemoryOrderStore;
import com.example.orders.producer.support.RecordingOrderEventPublisher;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The relay is the safety net for the dual-write problem: it must retry failed publications without
 * losing events and without changing the event id (which the consumer uses for deduplication).
 */
class OrderEventRelayTest {

    private static final Instant NOW = Instant.parse("2024-05-01T10:15:30Z");

    private InMemoryOrderStore orders;
    private RecordingOrderEventPublisher publisher;
    private OrderEventRelay relay;

    @BeforeEach
    void setUp() {
        orders = new InMemoryOrderStore();
        publisher = new RecordingOrderEventPublisher();
        relay = new OrderEventRelay(orders, publisher, Clock.fixed(NOW, ZoneOffset.UTC), 10, Duration.ofSeconds(5));
    }

    /** Stores an order that failed to publish 30s ago, i.e. well past the grace period. */
    private Order pendingOrder() {
        Order order = Order.create("customer-42", "desk", 1, BigDecimal.TEN, "EUR", NOW.minusSeconds(30));
        return orders.save(order);
    }

    @Test
    @DisplayName("does nothing when there is no backlog")
    void doesNothingWithoutBacklog() {
        relay.relayUnpublishedOrders();

        assertThat(publisher.publishedEvents()).isEmpty();
    }

    @Test
    @DisplayName("re-publishes pending orders and marks them as published")
    void republishesAndMarksPublished() {
        Order order = pendingOrder();

        relay.relayUnpublishedOrders();

        assertThat(publisher.publishedEvents()).singleElement()
                .extracting(OrderCreatedEvent::eventId)
                .isEqualTo(order.getEventId());
        assertThat(orders.require(order.getId()).getStatus())
                .isEqualTo(com.example.orders.producer.domain.OrderStatus.PUBLISHED);
    }

    @Test
    @DisplayName("keeps the order pending when the retry fails again, so the next run retries it")
    void keepsOrderPendingOnFailure() {
        Order order = pendingOrder();
        publisher.failWith(new EventPublishException("still down", new IllegalStateException("boom")));

        relay.relayUnpublishedOrders();

        assertThat(orders.require(order.getId()).getStatus())
                .isEqualTo(com.example.orders.producer.domain.OrderStatus.PUBLISH_FAILED);
        assertThat(publisher.publishedEvents()).isEmpty();

        // Once the broker is back the very next run succeeds.
        publisher.recover();
        relay.relayUnpublishedOrders();
        assertThat(publisher.publishedEvents()).hasSize(1);
    }

    @Test
    @DisplayName("skips orders another instance already published")
    void skipsAlreadyPublishedOrders() {
        Order order = pendingOrder();
        orders.markPublished(order.getId(), NOW);

        relay.relayUnpublishedOrders();

        assertThat(publisher.publishedEvents()).isEmpty();
    }

    @Test
    @DisplayName("respects the grace period so freshly created orders are not raced")
    void respectsGracePeriod() {
        // Created 1s ago: inside the 5s grace period, therefore not eligible yet.
        orders.save(Order.create("customer-1", "lamp", 1, BigDecimal.ONE, "EUR", NOW.minusSeconds(1)));

        relay.relayUnpublishedOrders();

        assertThat(publisher.publishedEvents()).isEmpty();
    }

    @Test
    @DisplayName("a relay attempt reuses the stored event id, keeping the consumer idempotent")
    void keepsEventIdStable() {
        Order order = pendingOrder();
        UUID eventId = order.getEventId();

        relay.relayUnpublishedOrders();
        publisher.failWith(new EventPublishException("down again", null));
        orders.markPublishFailed(order.getId());
        publisher.recover();
        relay.relayUnpublishedOrders();

        assertThat(publisher.publishedEvents())
                .isNotEmpty()
                .allSatisfy(event -> assertThat(event.eventId()).isEqualTo(eventId));
    }
}
