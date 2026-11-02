package com.example.orders.consumer.messaging;

import com.example.orders.contract.OrderCreatedEvent;
import com.example.orders.consumer.domain.ProcessingOutcome;
import com.example.orders.consumer.support.InMemoryOrderEventSink;
import com.example.orders.consumer.support.InMemoryProcessingStats;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.support.Acknowledgment;

import java.math.BigDecimal;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

/**
 * Behaviour of the consumer listener: idempotency, acknowledgement ordering and error propagation.
 *
 * <p>These are the properties that decide whether the pipeline loses or duplicates orders, so they
 * are asserted explicitly rather than assumed.</p>
 */
class OrderCreatedListenerTest {

    private static final Instant NOW = Instant.parse("2024-05-01T10:15:30Z");

    private InMemoryOrderEventSink sink;
    private InMemoryProcessingStats stats;
    private Acknowledgment acknowledgment;
    private OrderCreatedListener listener;

    @BeforeEach
    void setUp() {
        sink = new InMemoryOrderEventSink();
        stats = new InMemoryProcessingStats();
        acknowledgment = mock(Acknowledgment.class);
        listener = new OrderCreatedListener(sink, stats, Clock.fixed(NOW, ZoneOffset.UTC));
    }

    private static OrderCreatedEvent event(UUID eventId, UUID orderId) {
        return new OrderCreatedEvent(eventId, orderId, "customer-42", "Mechanical keyboard", 2,
                new BigDecimal("259.98"), "EUR", INSTANT);
    }

    private static final Instant INSTANT = Instant.parse("2024-05-01T10:15:00Z");

    private static ConsumerRecord<String, OrderCreatedEvent> record(OrderCreatedEvent payload) {
        return new ConsumerRecord<>("orders.created", 1, 42L,
                payload == null ? null : payload.orderId().toString(), payload);
    }

    @Test
    @DisplayName("stores the order, counts it and acknowledges after the write")
    void storesAndAcknowledges() {
        OrderCreatedEvent payload = event(UUID.randomUUID(), UUID.randomUUID());

        listener.onOrderCreated(record(payload), acknowledgment);

        assertThat(sink.accepted()).containsExactly(payload);
        assertThat(stats.count(ProcessingOutcome.CREATED)).isEqualTo(1);
        verify(acknowledgment).acknowledge();
    }

    @Test
    @DisplayName("a redelivered event is ignored but still acknowledged")
    void redeliveryIsIdempotent() {
        OrderCreatedEvent payload = event(UUID.randomUUID(), UUID.randomUUID());
        ConsumerRecord<String, OrderCreatedEvent> record = record(payload);

        listener.onOrderCreated(record, acknowledgment);
        listener.onOrderCreated(record, acknowledgment);

        // Exactly one row, two acknowledgements: no duplicate, no stuck offset.
        assertThat(sink.accepted()).hasSize(1);
        assertThat(stats.count(ProcessingOutcome.CREATED)).isEqualTo(1);
        assertThat(stats.count(ProcessingOutcome.DUPLICATE_IGNORED)).isEqualTo(1);
        verify(acknowledgment, org.mockito.Mockito.times(2)).acknowledge();
    }

    @Test
    @DisplayName("does not acknowledge when the database write fails, so the record is redelivered")
    void doesNotAcknowledgeOnTransientFailure() {
        sink.failNextWrites(1);
        OrderCreatedEvent payload = event(UUID.randomUUID(), UUID.randomUUID());

        assertThatThrownBy(() -> listener.onOrderCreated(record(payload), acknowledgment))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("simulated database outage");

        verify(acknowledgment, never()).acknowledge();
        assertThat(sink.accepted()).isEmpty();
    }

    @Test
    @DisplayName("rejects a record whose key does not match the payload without touching the database")
    void rejectsKeyPayloadMismatch() {
        OrderCreatedEvent payload = event(UUID.randomUUID(), UUID.randomUUID());
        var mismatched = new ConsumerRecord<>("orders.created", 0, 7L, UUID.randomUUID().toString(), payload);

        assertThatThrownBy(() -> listener.onOrderCreated(mismatched, acknowledgment))
                .isInstanceOf(InvalidRecordException.class)
                .hasMessageContaining("does not match");

        assertThat(sink.accepted()).isEmpty();
        verify(acknowledgment, never()).acknowledge();
    }

    @Test
    @DisplayName("rejects a null payload instead of storing garbage")
    void rejectsNullPayload() {
        var empty = new ConsumerRecord<String, OrderCreatedEvent>(
                "orders.created", 0, 8L, "some-key", null);

        assertThatThrownBy(() -> listener.onOrderCreated(empty, acknowledgment))
                .isInstanceOf(InvalidRecordException.class)
                .hasMessageContaining("payload is null");

        verify(acknowledgment, never()).acknowledge();
    }

    @Test
    @DisplayName("retry after a transient failure succeeds and counts the order once")
    void retrySucceedsAfterTransientFailure() {
        sink.failNextWrites(2);
        OrderCreatedEvent payload = event(UUID.randomUUID(), UUID.randomUUID());
        ConsumerRecord<String, OrderCreatedEvent> record = record(payload);

        assertThatThrownBy(() -> listener.onOrderCreated(record, acknowledgment)).isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(() -> listener.onOrderCreated(record, acknowledgment)).isInstanceOf(IllegalStateException.class);
        listener.onOrderCreated(record, acknowledgment);

        assertThat(sink.accepted()).containsExactly(payload);
        assertThat(stats.count(ProcessingOutcome.CREATED)).isEqualTo(1);
        verify(acknowledgment).acknowledge();
    }
}
