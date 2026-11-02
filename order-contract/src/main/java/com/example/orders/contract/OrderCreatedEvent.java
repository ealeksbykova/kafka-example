package com.example.orders.contract;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

/**
 * Integration event published on {@link KafkaTopics#ORDER_CREATED}.
 *
 * <p>Notes on the shape of the contract:</p>
 * <ul>
 *   <li>{@code eventId} is a producer generated UUID. Consumers use it for idempotency: a
 *       redelivered record carries the same {@code eventId} and must not create a second copy.</li>
 *   <li>{@code orderId} is the business key. It is also used as the Kafka message key, which keeps
 *       all events of one order on the same partition and therefore ordered.</li>
 *   <li>{@code occurredAt} is a UTC {@link Instant}, serialized as an ISO-8601 string because both
 *       services register the {@code JavaTimeModule}.</li>
 *   <li>{@code currency} is an ISO-4217 code (3 letters).</li>
 * </ul>
 *
 * @param eventId    unique id of this event occurrence
 * @param orderId    id of the order that was created
 * @param customerId reference to the customer that placed the order
 * @param product    ordered product name
 * @param quantity   number of ordered items (&gt;= 1)
 * @param totalPrice order total, must be &gt;= 0
 * @param currency   ISO-4217 currency code
 * @param occurredAt moment the order was created
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record OrderCreatedEvent(
        UUID eventId,
        UUID orderId,
        String customerId,
        String product,
        int quantity,
        BigDecimal totalPrice,
        String currency,
        Instant occurredAt
) {

    public OrderCreatedEvent {
        Objects.requireNonNull(eventId, "eventId must not be null");
        Objects.requireNonNull(orderId, "orderId must not be null");
        Objects.requireNonNull(customerId, "customerId must not be null");
        Objects.requireNonNull(product, "product must not be null");
        Objects.requireNonNull(totalPrice, "totalPrice must not be null");
        Objects.requireNonNull(currency, "currency must not be null");
        Objects.requireNonNull(occurredAt, "occurredAt must not be null");
        if (quantity < 1) {
            throw new IllegalArgumentException("quantity must be >= 1 but was " + quantity);
        }
        if (totalPrice.signum() < 0) {
            throw new IllegalArgumentException("totalPrice must be >= 0 but was " + totalPrice);
        }
        if (currency.length() != 3) {
            throw new IllegalArgumentException("currency must be an ISO-4217 code but was " + currency);
        }
    }

    /** Compact single-line representation, handy for log statements. */
    public String toLogLine() {
        return "OrderCreatedEvent{eventId=%s, orderId=%s, customerId=%s, product=%s, quantity=%d, totalPrice=%s %s}"
                .formatted(eventId, orderId, customerId, product, quantity, totalPrice, currency);
    }
}
