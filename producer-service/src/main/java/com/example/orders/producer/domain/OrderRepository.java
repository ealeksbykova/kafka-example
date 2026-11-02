package com.example.orders.producer.domain;

import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

public interface OrderRepository extends JpaRepository<Order, UUID> {

    List<Order> findByCustomerIdOrderByCreatedAtDesc(String customerId, Pageable pageable);

    Optional<Order> findByEventId(UUID eventId);

    /**
     * Orders the broker never acknowledged, oldest first.
     *
     * <p>{@link OrderStatus#PUBLISH_FAILED} is keyed by {@code @Enumerated(EnumType.STRING)}, so JPA
     * converts the enum to its name without needing a converter.</p>
     */
    List<Order> findByStatusAndCreatedAtBeforeOrderByCreatedAtAsc(
            OrderStatus status, Instant createdBefore, Pageable pageable);

    /** Used by the relay to avoid re-publishing an order that another instance already handled. */
    @Query("select count(o) from Order o where o.id = :id and o.status = com.example.orders.producer.domain.OrderStatus.PUBLISH_FAILED")
    long countStillUnpublished(@Param("id") UUID id);
}
