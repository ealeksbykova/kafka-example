package com.example.orders.producer.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

/**
 * Order as persisted by the producer.
 *
 * <p>{@code eventId} is generated when the order is created and stored with it, so a retry of the
 * publication reuses the same event id (keeping the consumer-side idempotency effective).</p>
 */
@Entity
@Table(name = "orders")
public class Order {

    @Id
    @Column(name = "id", nullable = false, updatable = false)
    private UUID id;

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

    @Enumerated(EnumType.STRING)
    @Column(name = "status", nullable = false, length = 20)
    private OrderStatus status;

    @Column(name = "event_id", updatable = false)
    private UUID eventId;

    @Column(name = "created_at", nullable = false, updatable = false)
    private Instant createdAt;

    @Column(name = "published_at")
    private Instant publishedAt;

    /** Required by JPA. */
    protected Order() {
    }

    private Order(UUID id, String customerId, String product, int quantity,
                  BigDecimal totalPrice, String currency, UUID eventId, Instant createdAt) {
        this.id = id;
        this.customerId = customerId;
        this.product = product;
        this.quantity = quantity;
        this.totalPrice = totalPrice;
        this.currency = currency;
        this.eventId = eventId;
        this.createdAt = createdAt;
        this.status = OrderStatus.PUBLISH_FAILED;
    }

    /** Creates a brand new order in the "not published yet" state. */
    public static Order create(String customerId, String product, int quantity,
                               BigDecimal totalPrice, String currency, Instant now) {
        Objects.requireNonNull(customerId, "customerId must not be null");
        Objects.requireNonNull(product, "product must not be null");
        Objects.requireNonNull(totalPrice, "totalPrice must not be null");
        Objects.requireNonNull(currency, "currency must not be null");
        Objects.requireNonNull(now, "now must not be null");
        return new Order(UUID.randomUUID(), customerId, product, quantity,
                totalPrice, currency, UUID.randomUUID(), now);
    }

    /** Marks the order as successfully published; the same event id is kept for retries. */
    public void markPublished(Instant publishedAt) {
        this.status = OrderStatus.PUBLISHED;
        this.publishedAt = Objects.requireNonNull(publishedAt, "publishedAt must not be null");
    }

    /** Marks the order as still unpublished after a failed publication attempt. */
    public void markPublishFailed() {
        this.status = OrderStatus.PUBLISH_FAILED;
        this.publishedAt = null;
    }

    public UUID getId() {
        return id;
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

    public OrderStatus getStatus() {
        return status;
    }

    public UUID getEventId() {
        return eventId;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }

    public Instant getPublishedAt() {
        return publishedAt;
    }
}
