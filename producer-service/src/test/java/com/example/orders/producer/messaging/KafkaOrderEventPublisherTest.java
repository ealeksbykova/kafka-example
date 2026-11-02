package com.example.orders.producer.messaging;

import com.example.orders.contract.OrderCreatedEvent;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Contract of the Kafka adapter: keying, timeout handling and error translation. */
@ExtendWith(MockitoExtension.class)
class KafkaOrderEventPublisherTest {

    private static final String TOPIC = "orders.created";

    @Mock
    private KafkaTemplate<String, OrderCreatedEvent> kafkaTemplate;

    private static OrderCreatedEvent event() {
        return new OrderCreatedEvent(UUID.randomUUID(), UUID.randomUUID(), "customer-42", "desk", 1,
                new BigDecimal("10.00"), "EUR", Instant.parse("2024-05-01T10:15:30Z"));
    }

    private KafkaOrderEventPublisher publisher(Duration timeout) {
        return new KafkaOrderEventPublisher(kafkaTemplate, TOPIC, timeout);
    }

    @Test
    @DisplayName("uses the order id as record key and returns the acknowledge partition")
    void sendsKeyedByOrderId() {
        OrderCreatedEvent event = event();
        SendResult<String, OrderCreatedEvent> sendResult = mock(SendResult.class);
        var metadata = mock(org.apache.kafka.clients.producer.RecordMetadata.class);
        when(metadata.partition()).thenReturn(2);
        when(sendResult.getRecordMetadata()).thenReturn(metadata);
        when(kafkaTemplate.send(eq(TOPIC), eq(event.orderId().toString()), eq(event)))
                .thenReturn(CompletableFuture.completedFuture(sendResult));

        int partition = publisher(Duration.ofSeconds(2)).publish(event);

        assertThat(partition).isEqualTo(2);
        verify(kafkaTemplate).send(TOPIC, event.orderId().toString(), event);
    }

    @Test
    @DisplayName("wraps broker failures into EventPublishException")
    void wrapsBrokerFailure() {
        OrderCreatedEvent event = event();
        CompletableFuture<SendResult<String, OrderCreatedEvent>> failed = new CompletableFuture<>();
        failed.completeExceptionally(new IllegalStateException("no brokers"));
        when(kafkaTemplate.send(eq(TOPIC), any(String.class), any(OrderCreatedEvent.class))).thenReturn(failed);

        assertThatThrownBy(() -> publisher(Duration.ofSeconds(2)).publish(event))
                .isInstanceOf(EventPublishException.class)
                .hasMessageContaining(event.eventId().toString());
    }

    @Test
    @DisplayName("fails fast instead of hanging when the broker never answers")
    void failsFastOnTimeout() {
        OrderCreatedEvent event = event();
        when(kafkaTemplate.send(eq(TOPIC), any(String.class), any(OrderCreatedEvent.class)))
                .thenReturn(new CompletableFuture<>());

        assertThatThrownBy(() -> publisher(Duration.ofMillis(50)).publish(event))
                .isInstanceOf(EventPublishException.class)
                .hasMessageContaining("Failed to publish");
    }
}
