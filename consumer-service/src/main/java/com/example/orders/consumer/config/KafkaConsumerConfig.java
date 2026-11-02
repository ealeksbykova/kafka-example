package com.example.orders.consumer.config;

import com.example.orders.contract.KafkaTopics;
import com.example.orders.contract.OrderCreatedEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.boot.ssl.SslBundles;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.util.backoff.ExponentialBackOff;

import java.util.HashMap;
import java.util.Map;

/**
 * Consumer wiring: typed deserialization, manual acknowledgement, bounded retries and a DLT.
 *
 * <p>Design notes:</p>
 * <ul>
 *   <li>{@link ErrorHandlingDeserializer} wraps the JSON deserializer, so a single corrupt record does
 *       not kill the listener container. Such records are handed to the error handler and end up in
 *       the DLT instead of blocking the partition.</li>
 *   <li>The deserializer is pinned to {@link OrderCreatedEvent}; no Java type headers are trusted,
 *       which prevents a malicious producer from instantiating arbitrary classes.</li>
 *   <li>Retries are bounded and exponential: transient failures (database down) are retried, while a
 *       permanently broken record is published to {@code orders.created.DLT}.</li>
 * </ul>
 */
@Configuration
public class KafkaConsumerConfig {

    @Bean
    ConsumerFactory<String, OrderCreatedEvent> orderCreatedConsumerFactory(KafkaProperties properties,
                                                                          ObjectMapper objectMapper,
                                                                          SslBundles sslBundles) {
        Map<String, Object> config = new HashMap<>(properties.buildConsumerProperties(sslBundles));
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
        // The delegate deserializers read these two properties.
        config.put(ErrorHandlingDeserializer.KEY_DESERIALIZER_CLASS, StringDeserializer.class.getName());
        config.put(ErrorHandlingDeserializer.VALUE_DESERIALIZER_CLASS, JsonDeserializer.class.getName());

        JsonDeserializer<OrderCreatedEvent> valueDeserializer =
                new JsonDeserializer<>(OrderCreatedEvent.class, objectMapper, false);
        valueDeserializer.setUseTypeHeaders(false);

        return new DefaultKafkaConsumerFactory<>(config, new ErrorHandlingDeserializer<>(new StringDeserializer()),
                new ErrorHandlingDeserializer<>(valueDeserializer));
    }

    @Bean
    ConcurrentKafkaListenerContainerFactory<String, OrderCreatedEvent> orderCreatedListenerContainerFactory(
            ConsumerFactory<String, OrderCreatedEvent> orderCreatedConsumerFactory,
            DefaultErrorHandler orderCreatedErrorHandler) {

        var factory = new ConcurrentKafkaListenerContainerFactory<String, OrderCreatedEvent>();
        factory.setConsumerFactory(orderCreatedConsumerFactory);
        // Manual acks: the offset must not be committed before the row is durable.
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        factory.setCommonErrorHandler(orderCreatedErrorHandler);
        return factory;
    }

    /**
     * Bounded retries with exponential backoff, then the DLT.
     *
     * <p>{@link DeadLetterPublishingRecoverer} keeps the original partition/offset in headers, so a
     * message can be replayed deliberately after the bug is fixed.</p>
     */
    @Bean
    DefaultErrorHandler orderCreatedErrorHandler(
            org.springframework.kafka.core.KafkaTemplate<String, Object> kafkaTemplate,
            @Value("${app.kafka.order-created-dlt:" + KafkaTopics.ORDER_CREATED_DLT + "}") String deadLetterTopic,
            @Value("${app.kafka.retry.max-attempts:3}") long maxAttempts,
            @Value("${app.kafka.retry.initial-interval:500ms}") java.time.Duration initialInterval,
            @Value("${app.kafka.retry.multiplier:2.0}") double multiplier) {

        var recoverer = new DeadLetterPublishingRecoverer(kafkaTemplate,
                (record, exception) -> new org.apache.kafka.common.TopicPartition(deadLetterTopic, -1));

        var backOff = new ExponentialBackOff(initialInterval.toMillis(), multiplier);
        // maxAttempts counts the initial delivery, hence the -1.
        // ExponentialBackOff counts retries after the first attempt, which setMaxAttempts(int) reflects.
        backOff.setMaxAttempts((int) Math.max(1, maxAttempts - 1));

        return new DefaultErrorHandler(recoverer, backOff);
    }

    /** Provisioned here as well so the consumer can publish DLT records even if the producer lagged. */
    @Bean
    NewTopic orderCreatedDeadLetterTopic(
            @Value("${app.kafka.order-created-dlt:" + KafkaTopics.ORDER_CREATED_DLT + "}") String topic) {
        return TopicBuilder.name(topic).partitions(3).replicas(1).build();
    }
}
