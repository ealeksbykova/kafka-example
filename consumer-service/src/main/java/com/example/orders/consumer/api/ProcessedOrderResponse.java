package com.example.orders.consumer.api;

import com.example.orders.consumer.domain.ProcessedOrder;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

/** Read model returned by the consumer API. */
public record ProcessedOrderResponse(
        UUID orderId,
        UUID eventId,
        String customerId,
        String product,
        int quantity,
        BigDecimal totalPrice,
        String currency,
        Instant occurredAt,
        Instant processedAt
) {

    public static ProcessedOrderResponse from(ProcessedOrder order) {
        return new ProcessedOrderResponse(
                order.getOrderId(),
                order.getEventId(),
                order.getCustomerId(),
                order.getProduct(),
                order.getQuantity(),
                order.getTotalPrice(),
                order.getCurrency(),
                order.getOccurredAt(),
                order.getProcessedAt());
    }
}
