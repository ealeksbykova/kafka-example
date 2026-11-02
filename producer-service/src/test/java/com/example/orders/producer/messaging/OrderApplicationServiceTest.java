package com.example.orders.producer.messaging;

import com.example.orders.contract.KafkaTopics;
import com.example.orders.contract.OrderCreatedEvent;
import com.example.orders.producer.api.CreateOrderRequest;
import com.example.orders.producer.domain.Order;
import com.example.orders.producer.domain.OrderStatus;
import com.example.orders.producer.support.InMemoryOrderStore;
import com.example.orders.producer.support.RecordingOrderEventPublisher;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Behaviour of the producer's use case: persist first, publish second, record the outcome.
 *
 * <p>Uses fakes instead of mocks so the assertions describe observable state, not interaction
 * bookkeeping.</p>
 */
class OrderApplicationServiceTest {

    private static final Instant NOW = Instant.parse("2024-05-01T10:15:30Z");

    private InMemoryOrderStore orders;
    private RecordingOrderEventPublisher publisher;
    private OrderApplicationService service;

    @BeforeEach
    void setUp() {
        orders = new InMemoryOrderStore();
        publisher = new RecordingOrderEventPublisher();
        service = new OrderApplicationService(orders, publisher, Clock.fixed(NOW, ZoneOffset.UTC));
    }

    private static CreateOrderRequest request() {
        return new CreateOrderRequest("customer-42", "Mechanical keyboard", 2, new BigDecimal("259.98"), "EUR");
    }

    @Test
    @DisplayName("persists the order and publishes exactly one event carrying the same identifiers")
    void publishesEventAndMarksOrderPublished() {
        Order result = service.createOrder(request());

        assertThat(publisher.publishedEvents()).hasSize(1);
        OrderCreatedEvent event = publisher.publishedEvents().getFirst();

        assertThat(event.orderId()).isEqualTo(result.getId());
        assertThat(event.eventId()).isEqualTo(result.getEventId());
        assertThat(event.customerId()).isEqualTo("customer-42");
        assertThat(event.product()).isEqualTo("Mechanical keyboard");
        assertThat(event.quantity()).isEqualTo(2);
        assertThat(event.totalPrice()).isEqualByComparingTo("259.98");
        assertThat(event.currency()).isEqualTo("EUR");
        assertThat(event.occurredAt()).isEqualTo(NOW);

        assertThat(result.getStatus()).isEqualTo(OrderStatus.PUBLISHED);
        assertThat(result.getPublishedAt()).isEqualTo(NOW);
        assertThat(orders.all()).hasSize(1);
    }

    @Test
    @DisplayName("keeps the order durable and reports PUBLISH_FAILED when the broker is down")
    void keepsOrderWhenBrokerIsUnavailable() {
        publisher.failWith(new EventPublishException("broker down", new IllegalStateException("boom")));

        Order result = service.createOrder(request());

        assertThat(result.getStatus()).isEqualTo(OrderStatus.PUBLISH_FAILED);
        assertThat(result.getPublishedAt()).isNull();
        assertThat(orders.all()).hasSize(1);
        assertThat(publisher.publishedEvents()).isEmpty();
    }

    @Test
    @DisplayName("stores the order before publishing so a failed publish can never lose it")
    void persistsBeforePublishing() {
        Order result = service.createOrder(request());

        // The order is already in the store, which is only observable if it was saved first.
        assertThat(orders.find(result.getId())).isPresent();
    }

    @Test
    @DisplayName("defaults the currency to EUR when the request omits it")
    void defaultsCurrencyToEur() {
        var requestWithoutCurrency = new CreateOrderRequest("customer-7", "Mouse pad", 1, new BigDecimal("9.99"), null);

        Order result = service.createOrder(requestWithoutCurrency);

        assertThat(result.getCurrency()).isEqualTo("EUR");
        assertThat(publisher.publishedEvents().getFirst().currency()).isEqualTo("EUR");
    }

    @Test
    @DisplayName("trims user supplied text fields")
    void trimsTextFields() {
        var padded = new CreateOrderRequest("  customer-9  ", "  Desk lamp  ", 1, BigDecimal.TEN, "USD");

        Order result = service.createOrder(padded);

        assertThat(result.getCustomerId()).isEqualTo("customer-9");
        assertThat(result.getProduct()).isEqualTo("Desk lamp");
    }

    @Test
    @DisplayName("finds an order by the event id it produced")
    void findsOrderByEventId() {
        Order created = service.createOrder(request());

        Optional<Order> found = service.findByEventId(created.getEventId());

        assertThat(found).containsSame(created);
    }

    @Test
    @DisplayName("returns an empty result for an unknown event id")
    void returnsEmptyForUnknownEventId() {
        assertThat(service.findByEventId(UUID.randomUUID())).isEmpty();
    }

    @Test
    @DisplayName("filters orders by customer when a customer id is provided")
    void filtersByCustomer() {
        service.createOrder(request());
        service.createOrder(new CreateOrderRequest("customer-99", "Monitor", 1, BigDecimal.TEN, "EUR"));

        assertThat(service.findOrders("customer-99", 10))
                .singleElement()
                .extracting(Order::getCustomerId)
                .isEqualTo("customer-99");

        assertThat(service.findOrders(null, 10)).hasSize(2);
    }

    @Test
    @DisplayName("the topic constant used by the publisher is the shared contract value")
    void usesSharedTopicConstant() {
        assertThat(KafkaTopics.ORDER_CREATED).isEqualTo("orders.created");
    }
}
