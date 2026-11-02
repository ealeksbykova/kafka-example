package com.example.orders.consumer.support;

import com.example.orders.contract.OrderCreatedEvent;
import com.example.orders.consumer.domain.OrderEventSink;
import com.example.orders.consumer.domain.ProcessingOutcome;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

/**
 * In-memory fake of the {@link OrderEventSink} port.
 *
 * <p>Mirrors the production SQL semantics: the first write of an order creates it, every later write
 * of the same order is reported as {@link ProcessingOutcome#DUPLICATE_IGNORED}.</p>
 */
public class InMemoryOrderEventSink implements OrderEventSink {

    private final Set<UUID> knownOrders = new LinkedHashSet<>();
    private final List<OrderCreatedEvent> accepted = new ArrayList<>();
    private final List<String> rejected = new ArrayList<>();
    private int failedWrites;

    /** Makes the next {@code failedWrites} calls to {@link #insertIfAbsent} fail. */
    public void failNextWrites(int times) {
        this.failedWrites = times;
    }

    @Override
    public ProcessingOutcome insertIfAbsent(OrderCreatedEvent event) {
        if (failedWrites > 0) {
            failedWrites--;
            throw new IllegalStateException("simulated database outage");
        }
        if (!knownOrders.add(event.orderId())) {
            return ProcessingOutcome.DUPLICATE_IGNORED;
        }
        accepted.add(event);
        return ProcessingOutcome.CREATED;
    }

    @Override
    public void recordRejected(String recordKey, String payload, String reason) {
        rejected.add(reason);
    }

    public List<OrderCreatedEvent> accepted() {
        return List.copyOf(accepted);
    }

    public List<String> rejectedReasons() {
        return List.copyOf(rejected);
    }
}
