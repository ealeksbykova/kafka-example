package com.example.orders.producer.api;

import com.example.orders.producer.domain.Order;
import com.example.orders.producer.messaging.OrderApplicationService;
import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.support.ServletUriComponentsBuilder;

import java.net.URI;
import java.util.List;
import java.util.UUID;

/** REST API of the producer service. */
@RestController
@RequestMapping("/api/orders")
public class OrderController {

    private final OrderApplicationService service;

    public OrderController(OrderApplicationService service) {
        this.service = service;
    }

    /**
     * Creates an order and publishes its {@code OrderCreatedEvent}.
     *
     * <p>The response contains the {@code eventId}, which is what the consumer stores and what the
     * e2e test uses to confirm that the message actually travelled through Kafka.</p>
     */
    @PostMapping
    public ResponseEntity<OrderResponse> create(@Valid @RequestBody CreateOrderRequest request) {
        Order created = service.createOrder(request);
        URI location = ServletUriComponentsBuilder.fromCurrentRequestUri()
                .path("/{id}")
                .buildAndExpand(created.getId())
                .toUri();
        return ResponseEntity.created(location).body(OrderResponse.from(created));
    }

    @GetMapping("/{id}")
    public OrderResponse get(@PathVariable UUID id) {
        return OrderResponse.from(service.getOrder(id));
    }

    @GetMapping
    public List<OrderResponse> list(@RequestParam(required = false) String customerId,
                                    @RequestParam(defaultValue = "20") int limit) {
        int safeLimit = Math.clamp(limit, 1, 100);
        return service.findOrders(customerId, safeLimit).stream()
                .map(OrderResponse::from)
                .toList();
    }

    @GetMapping("/by-event/{eventId}")
    public ResponseEntity<OrderResponse> byEventId(@PathVariable UUID eventId) {
        return service.findByEventId(eventId)
                .map(OrderResponse::from)
                .map(ResponseEntity::ok)
                .orElseGet(() -> ResponseEntity.status(HttpStatus.NOT_FOUND).build());
    }
}
