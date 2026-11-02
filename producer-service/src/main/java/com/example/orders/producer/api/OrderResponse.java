package com.example.orders.producer.api;

import com.example.orders.producer.domain.Order;
import com.example.orders.producer.domain.OrderStatus;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

/** Response body returned by the producer API. */
public record OrderResponse(
        UUID id,
        String customerId,
        String product,
        int quantity,
        BigDecimal totalPrice,
        String currency,
        OrderStatus status,
        UUID eventId,
        Instant createdAt,
        Instant publishedAt
) {

    public static OrderResponse from(Order order) {
        return new OrderResponse(
                order.getId(),
                order.getCustomerId(),
                order.getProduct(),
                order.getQuantity(),
                order.getTotalPrice(),
                order.getCurrency(),
                order.getStatus(),
                order.getEventId(),
                order.getCreatedAt(),
                order.getPublishedAt()
        );
    }
}
