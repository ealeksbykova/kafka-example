package com.example.orders.producer.messaging;

import com.example.orders.contract.OrderCreatedEvent;
import com.example.orders.producer.api.CreateOrderRequest;
import com.example.orders.producer.domain.Order;
import com.example.orders.producer.domain.OrderStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.time.Clock;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

/**
 * Application service of the producer: persist the order, then publish the event.
 *
 * <p>Transaction boundaries are explicit and deliberately <em>not</em> spanning the Kafka call:</p>
 * <ol>
 *   <li>{@link OrderStore#save} commits the order before anything is sent to Kafka. A publish can
 *       therefore never survive a rolled back order (no phantom events).</li>
 *   <li>{@link OrderEventPublisher#publish} waits for the broker acknowledgement outside any
 *       transaction, so a slow broker cannot hold a database connection or a row lock.</li>
 *   <li>The outcome is recorded afterwards in a short transaction. On failure the order stays
 *       {@code PUBLISH_FAILED} and {@link OrderEventRelay} retries it, so no event is silently
 *       lost.</li>
 * </ol>
 */
@Service
public class OrderApplicationService {

    private static final Logger log = LoggerFactory.getLogger(OrderApplicationService.class);

    private final OrderStore orders;
    private final OrderEventPublisher publisher;
    private final Clock clock;

    public OrderApplicationService(OrderStore orders, OrderEventPublisher publisher, Clock clock) {
        this.orders = orders;
        this.publisher = publisher;
        this.clock = clock;
    }

    /**
     * Persists the order and publishes its event.
     *
     * <p>A broker outage does not fail the HTTP request: the order is already durable and the relay
     * takes over. The response exposes {@code status=PUBLISH_FAILED} so the caller can tell.</p>
     */
    public Order createOrder(CreateOrderRequest request) {
        Order order = orders.save(Order.create(
                request.customerId().trim(),
                request.product().trim(),
                request.quantity(),
                request.totalPrice(),
                request.currencyOrDefault(),
                Instant.now(clock)));

        OrderCreatedEvent event = OrderCreatedEventFactory.from(order);
        try {
            publisher.publish(event);
            orders.markPublished(order.getId(), Instant.now(clock));
            return orders.require(order.getId());
        } catch (EventPublishException ex) {
            log.warn("Order {} stored but event {} was not acknowledged ({}). The relay will retry.",
                    order.getId(), order.getEventId(), ex.getMessage());
            orders.markPublishFailed(order.getId());
            return orders.require(order.getId());
        }
    }

    public Optional<Order> findOrder(UUID id) {
        return orders.find(id);
    }

    public Order getOrder(UUID id) {
        return orders.require(id);
    }

    public List<Order> findOrders(String customerId, int limit) {
        if (customerId == null || customerId.isBlank()) {
            return orders.findAll(limit);
        }
        return orders.findByCustomer(customerId.trim(), limit);
    }

    public Optional<Order> findByEventId(UUID eventId) {
        return orders.findByEventId(eventId);
    }
}
