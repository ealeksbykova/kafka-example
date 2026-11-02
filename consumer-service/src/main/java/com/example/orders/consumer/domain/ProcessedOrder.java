package com.example.orders.consumer.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

/**
 * Consumer side projection of an accepted order.
 *
 * <p>The primary key is the <strong>order id</strong>, not a generated id: that is what makes the
 * listener idempotent. A redelivered event (at-least-once delivery, rebalance, relay retry) hits the
 * same primary key, so it can be ignored instead of producing a duplicate row.</p>
 */
@Entity
@Table(name = "processed_order_events")
public class ProcessedOrder {

    @Id
    @Column(name = "order_id", nullable = false, updatable = false)
    private UUID orderId;

    @Column(name = "event_id", nullable = false, updatable = false)
    private UUID eventId;

    @Column(name = "customer_id", nullable = false, updatable = false, length = 64)
    private String customerId;

    @Column(name = "product", nullable = false, updatable = false, length = 255)
    private String product;

    @Column(name = "quantity", nullable = false, updatable = false)
    private int quantity;

    @Column(name = "total_price", nullable = false, updatable = false, precision = 19, scale = 2)
    private BigDecimal totalPrice;

    @Column(name = "currency", nullable = false, updatable = false, length = 3)
    private String currency;

    @Column(name = "occurred_at", nullable = false, updatable = false)
    private Instant occurredAt;

    @Column(name = "processed_at", nullable = false)
    private Instant processedAt;

    /** Required by JPA. */
    protected ProcessedOrder() {
    }

    private ProcessedOrder(UUID orderId, UUID eventId, String customerId, String product, int quantity,
                           BigDecimal totalPrice, String currency, Instant occurredAt, Instant processedAt) {
        this.orderId = orderId;
        this.eventId = eventId;
        this.customerId = customerId;
        this.product = product;
        this.quantity = quantity;
        this.totalPrice = totalPrice;
        this.currency = currency;
        this.occurredAt = occurredAt;
        this.processedAt = processedAt;
    }

    /**
     * Builds the projection. Field values are validated by the {@code OrderCreatedEvent} constructor,
     * so no additional validation is needed here.
     */
    public static ProcessedOrder of(UUID orderId, UUID eventId, String customerId, String product, int quantity,
                                    BigDecimal totalPrice, String currency, Instant occurredAt, Instant processedAt) {
        return new ProcessedOrder(
                Objects.requireNonNull(orderId, "orderId must not be null"),
                Objects.requireNonNull(eventId, "eventId must not be null"),
                Objects.requireNonNull(customerId, "customerId must not be null"),
                Objects.requireNonNull(product, "product must not be null"),
                quantity,
                Objects.requireNonNull(totalPrice, "totalPrice must not be null"),
                Objects.requireNonNull(currency, "currency must not be null"),
                Objects.requireNonNull(occurredAt, "occurredAt must not be null"),
                Objects.requireNonNull(processedAt, "processedAt must not be null"));
    }

    public UUID getOrderId() {
        return orderId;
    }

    public UUID getEventId() {
        return eventId;
    }

    public String getCustomerId() {
        return customerId;
    }

    public String getProduct() {
        return product;
    }

    public int getQuantity() {
        return quantity;
    }

    public BigDecimal getTotalPrice() {
        return totalPrice;
    }

    public String getCurrency() {
        return currency;
    }

    public Instant getOccurredAt() {
        return occurredAt;
    }

    public Instant getProcessedAt() {
        return processedAt;
    }
}
