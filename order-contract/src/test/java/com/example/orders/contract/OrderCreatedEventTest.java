package com.example.orders.contract;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Guards the wire format shared by producer and consumer. These tests must stay green even if a
 * service is refactored, because a change here breaks the other side of the pipeline.
 */
class OrderCreatedEventTest {

    /** Mirrors the ObjectMapper configuration both services apply for Kafka payloads. */
    private final ObjectMapper objectMapper = new ObjectMapper()
            .registerModule(new JavaTimeModule())
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
            .disable(DeserializationFeature.ADJUST_DATES_TO_CONTEXT_TIME_ZONE);

    private static OrderCreatedEvent sampleEvent() {
        return new OrderCreatedEvent(
                UUID.fromString("11111111-1111-1111-1111-111111111111"),
                UUID.fromString("22222222-2222-2222-2222-222222222222"),
                "customer-42",
                "Mechanical keyboard",
                2,
                new BigDecimal("259.98"),
                "EUR",
                Instant.parse("2024-05-01T10:15:30Z")
        );
    }

    @Test
    @DisplayName("survives a JSON round-trip without losing precision")
    void jsonRoundTripKeepsAllFields() throws Exception {
        OrderCreatedEvent original = sampleEvent();

        String json = objectMapper.writeValueAsString(original);
        OrderCreatedEvent restored = objectMapper.readValue(json, OrderCreatedEvent.class);

        assertThat(restored).isEqualTo(original);
        assertThat(restored.totalPrice()).isEqualByComparingTo("259.98");
        assertThat(json).contains("\"occurredAt\":\"2024-05-01T10:15:30Z\"");
    }

    @Test
    @DisplayName("tolerates fields added by a newer producer version")
    void ignoresUnknownProperties() throws Exception {
        String json = """
                {
                  "eventId": "11111111-1111-1111-1111-111111111111",
                  "orderId": "22222222-2222-2222-2222-222222222222",
                  "customerId": "customer-42",
                  "product": "Mechanical keyboard",
                  "quantity": 2,
                  "totalPrice": 259.98,
                  "currency": "EUR",
                  "occurredAt": "2024-05-01T10:15:30Z",
                  "schemaVersion": 2,
                  "warehouse": "BER-1"
                }
                """;

        OrderCreatedEvent restored = objectMapper.readValue(json, OrderCreatedEvent.class);

        assertThat(restored).isEqualTo(sampleEvent());
    }

    @Test
    @DisplayName("rejects an invalid quantity")
    void rejectsNonPositiveQuantity() {
        assertThatThrownBy(() -> new OrderCreatedEvent(
                UUID.randomUUID(), UUID.randomUUID(), "customer-42", "desk", 0,
                BigDecimal.ONE, "EUR", Instant.now()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("quantity");
    }

    @Test
    @DisplayName("rejects a currency that is not an ISO-4217 code")
    void rejectsInvalidCurrency() {
        assertThatThrownBy(() -> new OrderCreatedEvent(
                UUID.randomUUID(), UUID.randomUUID(), "customer-42", "desk", 1,
                BigDecimal.ONE, "EURO", Instant.now()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("currency");
    }

    @Test
    @DisplayName("dead-letter topic is derived from the main topic, not hardcoded")
    void deadLetterTopicDerivesFromMainTopic() {
        assertThat(KafkaTopics.ORDER_CREATED_DLT).isEqualTo(KafkaTopics.ORDER_CREATED + ".DLT");
    }
}
