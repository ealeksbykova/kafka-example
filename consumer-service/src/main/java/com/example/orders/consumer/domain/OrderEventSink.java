package com.example.orders.consumer.domain;

import com.example.orders.contract.OrderCreatedEvent;

/**
 * Write port for consumed events.
 *
 * <p>Implementations must be idempotent: calling {@link #insertIfAbsent} twice with the same event
 * must not create a second row.</p>
 */
public interface OrderEventSink {

    /**
     * Stores the event projection unless the order is already known.
     *
     * @return {@link ProcessingOutcome#CREATED} when the projection was inserted,
     *         {@link ProcessingOutcome#DUPLICATE_IGNORED} for a redelivered event
     */
    ProcessingOutcome insertIfAbsent(OrderCreatedEvent event);

    /** Parks a record that could not be processed, so it can be inspected later. */
    void recordRejected(String recordKey, String payload, String reason);
}
