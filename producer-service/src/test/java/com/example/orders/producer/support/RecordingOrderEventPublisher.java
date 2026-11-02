package com.example.orders.producer.support;

import com.example.orders.contract.OrderCreatedEvent;
import com.example.orders.producer.messaging.OrderEventPublisher;

import java.util.ArrayList;
import java.util.List;

/**
 * Recording fake of the {@link OrderEventPublisher} port.
 *
 * <p>Lets a test decide whether publishing succeeds or fails, without a broker.</p>
 */
public class RecordingOrderEventPublisher implements OrderEventPublisher {

    private final List<OrderCreatedEvent> published = new ArrayList<>();
    private RuntimeException failure;

    /** Makes every subsequent publish fail, mirroring an unavailable broker. */
    public void failWith(RuntimeException failure) {
        this.failure = failure;
    }

    /** Simulates the broker coming back. */
    public void recover() {
        this.failure = null;
    }

    @Override
    public int publish(OrderCreatedEvent event) {
        if (failure != null) {
            throw failure;
        }
        published.add(event);
        return 0;
    }

    public List<OrderCreatedEvent> publishedEvents() {
        return List.copyOf(published);
    }
}
