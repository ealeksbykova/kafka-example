package com.example.orders.contract;

/**
 * Single source of truth for every Kafka topic used by the sample.
 *
 * <p>Both services depend on this class, so a typo can never silently break the pipeline:
 * producer and consumer are compiled against the very same constant.</p>
 */
public final class KafkaTopics {

    /** Events emitted by the producer when an order has been persisted. */
    public static final String ORDER_CREATED = "orders.created";

    /** Dead-letter topic used by the consumer when a record cannot be processed. */
    public static final String ORDER_CREATED_DLT = ORDER_CREATED + ".DLT";

    private KafkaTopics() {
        throw new AssertionError("No instances of " + KafkaTopics.class.getSimpleName());
    }
}
