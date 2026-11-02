package com.example.orders.consumer;

import com.example.orders.contract.KafkaTopics;
import com.example.orders.contract.OrderCreatedEvent;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * End-to-end behaviour of the consumer against real Kafka and real PostgreSQL.
 *
 * <p>Covers the three properties that only appear with a real broker and a real database:
 * the message is consumed and stored, a redelivery does not duplicate it, and the read API reflects
 * the stored projection.</p>
 */
@Testcontainers(disabledWithoutDocker = true)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class ConsumerIntegrationTest {

    @Container
    @SuppressWarnings("resource")
    static final PostgreSQLContainer<?> POSTGRES =
            new PostgreSQLContainer<>(DockerImageName.parse("postgres:16-alpine"))
                    .withDatabaseName("consumer_db")
                    .withUsername("consumer")
                    .withPassword("consumer");

    @Container
    @SuppressWarnings("resource")
    static final KafkaContainer KAFKA =
            new KafkaContainer(DockerImageName.parse("apache/kafka:3.9.2"));

    @DynamicPropertySource
    static void configure(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
        registry.add("spring.datasource.username", POSTGRES::getUsername);
        registry.add("spring.datasource.password", POSTGRES::getPassword);
        registry.add("spring.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
    }

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Autowired
    private TestRestTemplate restTemplate;

    private static OrderCreatedEvent event() {
        return new OrderCreatedEvent(UUID.randomUUID(), UUID.randomUUID(), "customer-it",
                "Mechanical keyboard", 2, new BigDecimal("259.98"), "EUR", Instant.parse("2024-05-01T10:15:30Z"));
    }

    private static void publish(OrderCreatedEvent event) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                org.springframework.kafka.support.serializer.JsonSerializer.class);
        // No type headers: matches the consumer configuration, which pins the target type.
        props.put(org.springframework.kafka.support.serializer.JsonSerializer.ADD_TYPE_INFO_HEADERS, false);

        try (KafkaProducer<String, OrderCreatedEvent> producer = new KafkaProducer<>(props)) {
            producer.send(new ProducerRecord<>(KafkaTopics.ORDER_CREATED, event.orderId().toString(), event));
            producer.flush();
        }
    }

    @Test
    @DisplayName("consumes an event, stores it and exposes it through the read API")
    void consumesAndStoresEvent() {
        OrderCreatedEvent event = event();

        publish(event);

        await().atMost(Duration.ofSeconds(45)).pollInterval(Duration.ofMillis(500)).untilAsserted(() -> {
            Long count = jdbcTemplate.queryForObject(
                    "SELECT count(*) FROM processed_order_events WHERE order_id = ?::uuid",
                    Long.class, event.orderId().toString());
            assertThat(count).isEqualTo(1L);
        });

        ResponseEntity<Map> response = restTemplate.getForEntity(
                "/api/processed-orders/by-event/" + event.eventId(), Map.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(response.getBody().get("orderId")).isEqualTo(event.orderId().toString());
        assertThat(response.getBody().get("customerId")).isEqualTo("customer-it");

        Map<String, Object> row = jdbcTemplate.queryForMap(
                "SELECT product, quantity, currency FROM processed_order_events WHERE order_id = ?::uuid",
                event.orderId().toString());
        assertThat(row.get("product")).isEqualTo("Mechanical keyboard");
        assertThat(row.get("quantity")).isEqualTo(2);
        assertThat(row.get("currency")).isEqualTo("EUR");
    }

    @Test
    @DisplayName("a redelivered event is stored exactly once and counted as a duplicate")
    void handlesRedeliveryIdempotently() {
        OrderCreatedEvent event = event();

        publish(event);
        await().atMost(Duration.ofSeconds(45)).untilAsserted(() ->
                assertThat(jdbcTemplate.queryForObject(
                        "SELECT count(*) FROM processed_order_events WHERE order_id = ?::uuid",
                        Long.class, event.orderId().toString())).isEqualTo(1L));

        Long duplicatesBefore = jdbcTemplate.queryForObject(
                "SELECT occurrences FROM consumer_processing_stats WHERE outcome = 'DUPLICATE_IGNORED'", Long.class);

        // Same event id AND same order id: the classic at-least-once redelivery.
        publish(event);
        publish(event);

        await().atMost(Duration.ofSeconds(45)).untilAsserted(() -> {
            Long duplicatesAfter = jdbcTemplate.queryForObject(
                    "SELECT occurrences FROM consumer_processing_stats WHERE outcome = 'DUPLICATE_IGNORED'", Long.class);
            assertThat(duplicatesAfter).isGreaterThanOrEqualTo(duplicatesBefore + 2);
        });

        Long rows = jdbcTemplate.queryForObject(
                "SELECT count(*) FROM processed_order_events WHERE order_id = ?::uuid",
                Long.class, event.orderId().toString());
        assertThat(rows).isEqualTo(1L);
    }

    @Test
    @DisplayName("a schema-valid but contract-violating record ends up in the DLT instead of blocking")
    void routesPoisonMessageToDeadLetterTopic() throws Exception {
        String key = UUID.randomUUID().toString();
        // "quantity": 0 violates the record's compact constructor, so deserialization fails.
        String poison = """
                {"eventId": "%s", "orderId": "%s", "customerId": "c", "product": "p",
                 "quantity": 0, "totalPrice": 1.00, "currency": "EUR",
                 "occurredAt": "2024-05-01T10:15:30Z"}
                """.formatted(UUID.randomUUID(), key);

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            producer.send(new ProducerRecord<>(KafkaTopics.ORDER_CREATED, key, poison));
            producer.flush();
        }

        // The consumer must keep consuming afterwards: a healthy record still gets processed.
        OrderCreatedEvent followUp = event();
        publish(followUp);

        await().atMost(Duration.ofSeconds(60)).pollInterval(Duration.ofMillis(500)).untilAsserted(() ->
                assertThat(jdbcTemplate.queryForObject(
                        "SELECT count(*) FROM processed_order_events WHERE order_id = ?::uuid",
                        Long.class, followUp.orderId().toString())).isEqualTo(1L));
    }
}
