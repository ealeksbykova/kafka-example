package com.example.orders.producer.messaging;

import com.example.orders.contract.KafkaTopics;
import com.example.orders.contract.OrderCreatedEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Kafka adapter for {@link OrderEventPublisher}.
 *
 * <p>The send is synchronous on purpose: the HTTP response must report whether the event actually
 * reached the broker. The record key is the order id, so all events of one order land on the same
 * partition and keep their order.</p>
 */
@Component
public class KafkaOrderEventPublisher implements OrderEventPublisher {

    private static final Logger log = LoggerFactory.getLogger(KafkaOrderEventPublisher.class);

    private final KafkaTemplate<String, OrderCreatedEvent> kafkaTemplate;
    private final String topic;
    private final Duration sendTimeout;

    public KafkaOrderEventPublisher(KafkaTemplate<String, OrderCreatedEvent> kafkaTemplate,
                                    @Value("${app.kafka.order-created-topic:" + KafkaTopics.ORDER_CREATED + "}") String topic,
                                    @Value("${app.kafka.send-timeout:10s}") Duration sendTimeout) {
        this.kafkaTemplate = kafkaTemplate;
        this.topic = topic;
        this.sendTimeout = sendTimeout;
    }

    @Override
    public int publish(OrderCreatedEvent event) {
        try {
            SendResult<String, OrderCreatedEvent> result = kafkaTemplate
                    .send(topic, event.orderId().toString(), event)
                    .get(sendTimeout.toMillis(), TimeUnit.MILLISECONDS);

            int partition = result.getRecordMetadata().partition();
            log.info("Published {} to topic={} partition={} offset={}",
                    event.toLogLine(), topic, partition, result.getRecordMetadata().offset());
            return partition;
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new EventPublishException("Interrupted while publishing event " + event.eventId(), ex);
        } catch (ExecutionException | TimeoutException ex) {
            throw new EventPublishException(
                    "Failed to publish event %s for order %s".formatted(event.eventId(), event.orderId()), ex);
        }
    }
}
