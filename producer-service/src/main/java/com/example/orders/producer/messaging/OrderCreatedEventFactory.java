package com.example.orders.producer.messaging;

import com.example.orders.contract.OrderCreatedEvent;
import com.example.orders.producer.domain.Order;

/** Maps a persisted order to the integration event published on Kafka. */
public final class OrderCreatedEventFactory {

    private OrderCreatedEventFactory() {
        throw new AssertionError("No instances of " + OrderCreatedEventFactory.class.getSimpleName());
    }

    public static OrderCreatedEvent from(Order order) {
        return new OrderCreatedEvent(
                order.getEventId(),
                order.getId(),
                order.getCustomerId(),
                order.getProduct(),
                order.getQuantity(),
                order.getTotalPrice(),
                order.getCurrency(),
                order.getCreatedAt()
        );
    }
}
