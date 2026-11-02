package com.example.orders.producer.support;

import com.example.orders.producer.domain.Order;
import com.example.orders.producer.domain.OrderStatus;
import com.example.orders.producer.domain.OrderStore;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

/**
 * In-memory fake of the {@link OrderStore} port.
 *
 * <p>Reproduces the behaviour the service layer depends on (save, load, status transitions) so unit
 * tests exercise the real application logic without a database.</p>
 */
public class InMemoryOrderStore implements OrderStore {

    private final Map<UUID, Order> orders = new LinkedHashMap<>();

    @Override
    public Order save(Order order) {
        orders.put(order.getId(), order);
        return order;
    }

    @Override
    public Optional<Order> find(UUID id) {
        return Optional.ofNullable(orders.get(id));
    }

    @Override
    public Order require(UUID id) {
        return find(id).orElseThrow(() -> new NoSuchElementException("Order " + id + " not found"));
    }

    @Override
    public Optional<Order> findByEventId(UUID eventId) {
        return orders.values().stream()
                .filter(order -> Objects.equals(order.getEventId(), eventId))
                .findFirst();
    }

    @Override
    public List<Order> findAll(int limit) {
        return orders.values().stream().limit(limit).toList();
    }

    @Override
    public List<Order> findByCustomer(String customerId, int limit) {
        return orders.values().stream()
                .filter(order -> order.getCustomerId().equals(customerId))
                .limit(limit)
                .toList();
    }

    @Override
    public List<Order> findUnpublished(Instant createdBefore, int limit) {
        return orders.values().stream()
                .filter(order -> order.getStatus() == OrderStatus.PUBLISH_FAILED)
                .filter(order -> order.getCreatedAt().isBefore(createdBefore))
                .sorted(Comparator.comparing(Order::getCreatedAt))
                .limit(limit)
                .toList();
    }

    @Override
    public boolean isStillUnpublished(UUID id) {
        return find(id).map(order -> order.getStatus() == OrderStatus.PUBLISH_FAILED).orElse(false);
    }

    @Override
    public void markPublished(UUID id, Instant publishedAt) {
        require(id).markPublished(publishedAt);
    }

    @Override
    public void markPublishFailed(UUID id) {
        require(id).markPublishFailed();
    }

    /** Test helper: all stored orders in insertion order. */
    public List<Order> all() {
        return new ArrayList<>(orders.values());
    }
}
