package com.example.orders.producer.config;

import com.example.orders.contract.OrderCreatedEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.boot.ssl.SslBundles;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.serializer.JsonSerializer;

import java.util.HashMap;
import java.util.Map;

/**
 * Kafka producer wiring.
 *
 * <p>Deliberate choices:</p>
 * <ul>
 *   <li><strong>Typed template</strong> - {@code KafkaTemplate<String, OrderCreatedEvent>} makes it
 *       impossible to publish an arbitrary object on this topic by accident.</li>
 *   <li><strong>JSON value</strong> - keeps the record readable in Kafka UI and usable by non-JVM
 *       consumers. The application {@link ObjectMapper} is reused, so {@code Instant} is written as
 *       ISO-8601 rather than as an epoch number.</li>
 *   <li><strong>No type headers</strong> - the Java class name of the producer would otherwise be
 *       embedded in the message, coupling the consumer to the producer's package layout.</li>
 * </ul>
 * Connection settings (bootstrap servers, acks, retries, timeouts) come from {@code application.yml}
 * through {@link KafkaProperties}.
 */
@Configuration
public class KafkaProducerConfig {

    @Bean
    ProducerFactory<String, OrderCreatedEvent> orderEventProducerFactory(KafkaProperties properties,
                                                                         ObjectMapper objectMapper,
                                                                         SslBundles sslBundles) {
        Map<String, Object> config = new HashMap<>(properties.buildProducerProperties(sslBundles));
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);

        JsonSerializer<OrderCreatedEvent> valueSerializer = new JsonSerializer<>(objectMapper);
        valueSerializer.setAddTypeInfo(false);

        return new DefaultKafkaProducerFactory<>(config, new StringSerializer(), valueSerializer);
    }

    @Bean
    KafkaTemplate<String, OrderCreatedEvent> kafkaTemplate(
            ProducerFactory<String, OrderCreatedEvent> orderEventProducerFactory) {
        KafkaTemplate<String, OrderCreatedEvent> template = new KafkaTemplate<>(orderEventProducerFactory);
        template.setObservationEnabled(true);
        return template;
    }
}
