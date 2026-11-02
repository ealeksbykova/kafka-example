package com.example.orders.producer.messaging;

import com.example.orders.contract.OrderCreatedEvent;

/**
 * Publishing port of the producer service.
 *
 * <p>The abstraction lets unit tests record publishes without a broker, and keeps the Kafka client
 * details confined to the adapter.</p>
 */
public interface OrderEventPublisher {

    /**
     * Publishes the event and waits for the broker acknowledgement.
     *
     * @return the partition the record was written to
     * @throws EventPublishException when the broker does not acknowledge in time
     */
    int publish(OrderCreatedEvent event);
}
