package com.example.orders.consumer.persistence;

import com.example.orders.contract.OrderCreatedEvent;
import com.example.orders.consumer.domain.OrderEventSink;
import com.example.orders.consumer.domain.ProcessingOutcome;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.sql.Timestamp;
import java.time.Clock;
import java.time.Instant;
import java.util.Objects;

/**
 * PostgreSQL adapter for {@link OrderEventSink}.
 *
 * <p><strong>Why native SQL instead of {@code repository.save()}?</strong>
 * {@code INSERT ... ON CONFLICT (order_id) DO NOTHING} lets the database decide in a single atomic
 * statement whether the event is new. A "select then insert" would race under the listener
 * concurrency of 3, and catching a {@code DataIntegrityViolationException} would mark the surrounding
 * transaction rollback-only. The affected-row count tells us which branch was taken.</p>
 */
@Component
public class JdbcOrderEventSink implements OrderEventSink {

    private static final String INSERT_ORDER = """
            INSERT INTO processed_order_events
                (order_id, event_id, customer_id, product, quantity, total_price, currency, occurred_at, processed_at)
            VALUES
                (:orderId, :eventId, :customerId, :product, :quantity, :totalPrice, :currency, :occurredAt, :processedAt)
            ON CONFLICT (order_id) DO NOTHING
            """;

    private static final String INSERT_REJECTED = """
            INSERT INTO dead_letter_events (topic, record_key, payload, reason, received_at)
            VALUES (:topic, :recordKey, :payload, :reason, :receivedAt)
            """;

    private final NamedParameterJdbcTemplate jdbc;
    private final Clock clock;
    private final String topic;

    public JdbcOrderEventSink(NamedParameterJdbcTemplate jdbc,
                              Clock clock,
                              @Value("${app.kafka.order-created-topic:orders.created}") String topic) {
        this.jdbc = jdbc;
        this.clock = clock;
        this.topic = topic;
    }

    @Override
    @Transactional
    public ProcessingOutcome insertIfAbsent(OrderCreatedEvent event) {
        MapSqlParameterSource parameters = new MapSqlParameterSource()
                .addValue("orderId", event.orderId())
                .addValue("eventId", event.eventId())
                .addValue("customerId", event.customerId())
                .addValue("product", event.product())
                .addValue("quantity", event.quantity())
                .addValue("totalPrice", event.totalPrice())
                .addValue("currency", event.currency())
                .addValue("occurredAt", Timestamp.from(event.occurredAt()))
                .addValue("processedAt", Timestamp.from(Instant.now(clock)));

        int inserted = jdbc.update(INSERT_ORDER, parameters);
        return inserted == 1 ? ProcessingOutcome.CREATED : ProcessingOutcome.DUPLICATE_IGNORED;
    }

    @Override
    @Transactional
    public void recordRejected(String recordKey, String payload, String reason) {
        Objects.requireNonNull(reason, "reason must not be null");
        jdbc.update(INSERT_REJECTED, new MapSqlParameterSource()
                .addValue("topic", topic)
                .addValue("recordKey", recordKey == null ? "" : recordKey)
                .addValue("payload", payload == null ? "" : payload)
                .addValue("reason", reason)
                .addValue("receivedAt", Timestamp.from(Instant.now(clock))));
    }
}
