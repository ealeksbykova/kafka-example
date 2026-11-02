package com.example.orders.producer.domain;

import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.UUID;

/**
 * JPA adapter for {@link OrderStore}.
 *
 * <p>Read methods are {@code readOnly} transactions, write methods are short read-modify-write
 * transactions. Returning detached entities is intentional: the service layer only reads scalar
 * fields, and detached entities make the transaction boundaries impossible to blur by accident.</p>
 */
@Component
public class JpaOrderStore implements OrderStore {

    private final OrderRepository repository;

    public JpaOrderStore(OrderRepository repository) {
        this.repository = repository;
    }

    @Override
    @Transactional
    public Order save(Order order) {
        return repository.save(order);
    }

    @Override
    @Transactional(readOnly = true)
    public Optional<Order> find(UUID id) {
        return repository.findById(id);
    }

    @Override
    @Transactional(readOnly = true)
    public Order require(UUID id) {
        return repository.findById(id)
                .orElseThrow(() -> new NoSuchElementException("Order %s not found".formatted(id)));
    }

    @Override
    @Transactional(readOnly = true)
    public Optional<Order> findByEventId(UUID eventId) {
        return repository.findByEventId(eventId);
    }

    @Override
    @Transactional(readOnly = true)
    public List<Order> findAll(int limit) {
        return repository.findAll(Pageable.ofSize(limit)).getContent();
    }

    @Override
    @Transactional(readOnly = true)
    public List<Order> findByCustomer(String customerId, int limit) {
        return repository.findByCustomerIdOrderByCreatedAtDesc(customerId, Pageable.ofSize(limit));
    }

    @Override
    @Transactional(readOnly = true)
    public List<Order> findUnpublished(Instant createdBefore, int limit) {
        return repository.findByStatusAndCreatedAtBeforeOrderByCreatedAtAsc(
                OrderStatus.PUBLISH_FAILED, createdBefore, Pageable.ofSize(limit));
    }

    @Override
    @Transactional(readOnly = true)
    public boolean isStillUnpublished(UUID id) {
        return repository.countStillUnpublished(id) > 0;
    }

    @Override
    @Transactional
    public void markPublished(UUID id, Instant publishedAt) {
        require(id).markPublished(publishedAt);
    }

    @Override
    @Transactional
    public void markPublishFailed(UUID id) {
        require(id).markPublishFailed();
    }
}
