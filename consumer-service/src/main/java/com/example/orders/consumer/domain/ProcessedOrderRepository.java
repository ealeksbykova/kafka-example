package com.example.orders.consumer.domain;

import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

public interface ProcessedOrderRepository extends JpaRepository<ProcessedOrder, UUID> {

    Optional<ProcessedOrder> findByEventId(UUID eventId);

    List<ProcessedOrder> findByCustomerIdOrderByOccurredAtDesc(String customerId);
}
