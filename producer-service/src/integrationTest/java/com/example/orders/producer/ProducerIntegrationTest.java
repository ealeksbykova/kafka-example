package com.example.orders.producer;

import com.example.orders.contract.KafkaTopics;
import com.example.orders.contract.OrderCreatedEvent;
import com.example.orders.producer.domain.OrderStatus;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
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

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Full round-trip against real PostgreSQL and a real Kafka broker.
 *
 * <p>Verifies what unit tests cannot: that the Flyway migration matches the JPA mapping, and that a
 * REST call actually produces a record on {@code orders.created}.</p>
 */
@Testcontainers(disabledWithoutDocker = true)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class ProducerIntegrationTest {

    @Container
    @SuppressWarnings("resource")
    static final PostgreSQLContainer<?> POSTGRES =
            new PostgreSQLContainer<>(DockerImageName.parse("postgres:16-alpine"))
                    .withDatabaseName("producer_db")
                    .withUsername("producer")
                    .withPassword("producer");

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
        // Keep the relay out of the way: this test asserts the synchronous publish path.
        registry.add("app.relay.enabled", () -> "false");
    }

    @Autowired
    private TestRestTemplate restTemplate;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Autowired
    private ObjectMapper objectMapper;

    @Test
    @DisplayName("POST /api/orders persists the row and publishes a keyed event to Kafka")
    void createsOrderAndPublishesEvent() {
        Map<String, Object> body = Map.of(
                "customerId", "customer-it",
                "product", "Mechanical keyboard",
                "quantity", 2,
                "totalPrice", 259.98,
                "currency", "EUR");

        ResponseEntity<Map> response = restTemplate.postForEntity("/api/orders", body, Map.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.CREATED);
        String orderId = (String) response.getBody().get("id");
        String eventId = (String) response.getBody().get("eventId");
        assertThat(orderId).isNotBlank();
        assertThat(eventId).isNotBlank();
        assertThat(response.getBody().get("status")).isEqualTo(OrderStatus.PUBLISHED.name());

        // 1. Row committed with the expected values.
        Map<String, Object> row = jdbcTemplate.queryForMap(
                "SELECT customer_id, product, quantity, total_price, currency, status FROM orders WHERE id = ?::uuid",
                orderId);
        assertThat(row.get("customer_id")).isEqualTo("customer-it");
        assertThat(row.get("product")).isEqualTo("Mechanical keyboard");
        assertThat(row.get("status")).isEqualTo(OrderStatus.PUBLISHED.name());

        // 2. The event really reached the broker, keyed by the order id.
        ConsumerRecord<String, String> record = awaitRecordFor(orderId);
        assertThat(record.key()).isEqualTo(orderId);
        OrderCreatedEvent event = read(record.value());
        assertThat(event.orderId().toString()).isEqualTo(orderId);
        assertThat(event.eventId().toString()).isEqualTo(eventId);
        assertThat(event.customerId()).isEqualTo("customer-it");
    }

    @Test
    @DisplayName("the REST API returns 400 for an invalid payload and stores nothing")
    void rejectsInvalidPayload() {
        long before = jdbcTemplate.queryForObject("SELECT count(*) FROM orders", Long.class);

        ResponseEntity<Map> response = restTemplate.postForEntity("/api/orders",
                Map.of("customerId", "", "product", "", "quantity", 0, "totalPrice", -5), Map.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
        assertThat(jdbcTemplate.queryForObject("SELECT count(*) FROM orders", Long.class)).isEqualTo(before);
    }

    @Test
    @DisplayName("GET /api/orders/{id} returns the stored order and 404 for an unknown id")
    void exposesStoredOrder() {
        ResponseEntity<Map> created = restTemplate.postForEntity("/api/orders", Map.of(
                "customerId", "customer-get", "product", "Monitor", "quantity", 1,
                "totalPrice", 199.00, "currency", "USD"), Map.class);
        String orderId = (String) created.getBody().get("id");

        ResponseEntity<Map> found = restTemplate.getForEntity("/api/orders/" + orderId, Map.class);
        assertThat(found.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(found.getBody().get("product")).isEqualTo("Monitor");

        ResponseEntity<String> missing = restTemplate.getForEntity("/api/orders/" + UUID.randomUUID(), String.class);
        assertThat(missing.getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
    }

    private OrderCreatedEvent read(String json) {
        try {
            return objectMapper.readValue(json, OrderCreatedEvent.class);
        } catch (Exception ex) {
            throw new IllegalStateException("Broker payload is not a valid OrderCreatedEvent: " + json, ex);
        }
    }

    /** Polls the topic with a dedicated consumer until the record for {@code orderId} shows up. */
    private ConsumerRecord<String, String> awaitRecordFor(String orderId) {
        List<ConsumerRecord<String, String>> collected = new ArrayList<>();
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "it-producer-" + UUID.randomUUID());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(List.of(KafkaTopics.ORDER_CREATED));
            await().atMost(Duration.ofSeconds(30)).pollInterval(Duration.ofMillis(500)).until(() -> {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
                records.forEach(collected::add);
                return collected.stream().anyMatch(r -> orderId.equals(r.key()));
            });
        }
        return collected.stream()
                .filter(r -> orderId.equals(r.key()))
                .findFirst()
                .orElseThrow(() -> new AssertionError("No Kafka record for order " + orderId));
    }
}
