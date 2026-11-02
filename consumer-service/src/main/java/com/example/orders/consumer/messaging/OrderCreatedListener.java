package com.example.orders.consumer.messaging;

import com.example.orders.contract.KafkaTopics;
import com.example.orders.contract.OrderCreatedEvent;
import com.example.orders.consumer.domain.OrderEventSink;
import com.example.orders.consumer.domain.ProcessingOutcome;
import com.example.orders.consumer.domain.ProcessingStats;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import java.time.Clock;
import java.time.Instant;

/**
 * Listens to {@code orders.created} and stores every order exactly once.
 *
 * <p>Reliability properties:</p>
 * <ul>
 *   <li><strong>At-least-once delivery</strong> is assumed. Idempotency comes from the database
 *       ({@code ON CONFLICT DO NOTHING}), not from hoping the broker delivers once.</li>
 *   <li><strong>Acknowledgement per record</strong>: the offset is committed only after
 *       {@link OrderEventSink#insertIfAbsent} returned, i.e. after the row is durable.</li>
 *   <li><strong>Transient failures are rethrown</strong> so the configured retry/backoff and finally
 *       the {@code DefaultErrorHandler} can park the record in the DLT. Swallowing an exception here
 *       would acknowledge the offset and silently lose the event.</li>
 *   <li><strong>Contract violations</strong> (null payload, key/payload mismatch) are also thrown
 *       {@link InvalidRecordException} and therefore land in the DLT, where they are inspectable.</li>
 * </ul>
 */
@Component
public class OrderCreatedListener {

    private static final Logger log = LoggerFactory.getLogger(OrderCreatedListener.class);

    private final OrderEventSink sink;
    private final ProcessingStats stats;
    private final Clock clock;

    public OrderCreatedListener(OrderEventSink sink, ProcessingStats stats, Clock clock) {
        this.sink = sink;
        this.stats = stats;
        this.clock = clock;
    }

    @KafkaListener(
            topics = "${app.kafka.order-created-topic:" + KafkaTopics.ORDER_CREATED + "}",
            groupId = "${spring.kafka.consumer.group-id:consumer-service}",
            concurrency = "${app.kafka.concurrency:3}",
            containerFactory = "orderCreatedListenerContainerFactory"
    )
    public void onOrderCreated(ConsumerRecord<String, OrderCreatedEvent> record, Acknowledgment acknowledgment) {
        OrderCreatedEvent event = record.value();
        validate(record, event);

        ProcessingOutcome outcome = sink.insertIfAbsent(event);
        stats.increment(outcome, Instant.now(clock));
        acknowledgment.acknowledge();

        if (outcome == ProcessingOutcome.DUPLICATE_IGNORED) {
            log.info("Duplicate delivery ignored: eventId={} orderId={} partition={} offset={}",
                    event.eventId(), event.orderId(), record.partition(), record.offset());
        } else {
            log.info("Stored {} from partition={} offset={}",
                    event.toLogLine(), record.partition(), record.offset());
        }
    }

    private void validate(ConsumerRecord<String, OrderCreatedEvent> record, OrderCreatedEvent event) {
        if (event == null) {
            throw new InvalidRecordException(
                    "Event payload is null (partition=%d, offset=%d)".formatted(record.partition(), record.offset()));
        }
        if (record.key() != null && !record.key().equals(event.orderId().toString())) {
            throw new InvalidRecordException(
                    "Record key %s does not match payload orderId %s".formatted(record.key(), event.orderId()));
        }
    }
}
