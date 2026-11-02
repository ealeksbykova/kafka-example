package com.example.orders.consumer.api;

import com.example.orders.consumer.domain.ProcessedOrder;
import com.example.orders.consumer.domain.ProcessedOrderRepository;
import com.example.orders.consumer.domain.ProcessingStats;
import org.springframework.data.domain.Pageable;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Read API of the consumer service.
 *
 * <p>Exposing what the consumer actually stored is what makes the pipeline observable: the e2e test
 * polls {@code /api/processed-orders/by-event/{eventId}} until the producer event shows up here.</p>
 */
@RestController
@RequestMapping("/api/processed-orders")
public class ProcessedOrderController {

    private final ProcessedOrderRepository repository;
    private final ProcessingStats stats;

    public ProcessedOrderController(ProcessedOrderRepository repository, ProcessingStats stats) {
        this.repository = repository;
        this.stats = stats;
    }

    /** Looks up the stored projection by the producer's event id. 404 until the event is consumed. */
    @GetMapping("/by-event/{eventId}")
    public ResponseEntity<ProcessedOrderResponse> byEventId(@PathVariable UUID eventId) {
        return repository.findByEventId(eventId)
                .map(ProcessedOrderResponse::from)
                .map(ResponseEntity::ok)
                .orElseGet(() -> ResponseEntity.notFound().build());
    }

    @GetMapping("/{orderId}")
    public ResponseEntity<ProcessedOrderResponse> byOrderId(@PathVariable UUID orderId) {
        return repository.findById(orderId)
                .map(ProcessedOrderResponse::from)
                .map(ResponseEntity::ok)
                .orElseGet(() -> ResponseEntity.notFound().build());
    }

    @GetMapping
    public List<ProcessedOrderResponse> list(@RequestParam(required = false) String customerId,
                                             @RequestParam(defaultValue = "50") int limit) {
        int safeLimit = Math.clamp(limit, 1, 500);
        List<ProcessedOrder> orders = (customerId == null || customerId.isBlank())
                ? repository.findAll(Pageable.ofSize(safeLimit)).getContent()
                : repository.findByCustomerIdOrderByOccurredAtDesc(customerId.trim());

        return orders.stream()
                .limit(safeLimit)
                .map(ProcessedOrderResponse::from)
                .toList();
    }

    /** Processing counters: CREATED, DUPLICATE_IGNORED and FAILED. */
    @GetMapping("/stats")
    public Map<String, Long> stats() {
        return stats.counts();
    }
}
