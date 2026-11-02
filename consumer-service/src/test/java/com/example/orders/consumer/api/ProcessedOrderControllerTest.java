package com.example.orders.consumer.api;

import com.example.orders.consumer.domain.ProcessedOrder;
import com.example.orders.consumer.domain.ProcessedOrderRepository;
import com.example.orders.consumer.domain.ProcessingOutcome;
import com.example.orders.consumer.domain.ProcessingStats;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.Pageable;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.BDDMockito.given;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/** Web layer of the consumer read model. */
@WebMvcTest(ProcessedOrderController.class)
class ProcessedOrderControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @MockitoBean
    private ProcessedOrderRepository repository;

    @MockitoBean
    private ProcessingStats stats;

    private static ProcessedOrder projection() {
        return ProcessedOrder.of(
                UUID.fromString("22222222-2222-2222-2222-222222222222"),
                UUID.fromString("11111111-1111-1111-1111-111111111111"),
                "customer-42", "Mechanical keyboard", 2, new BigDecimal("259.98"), "EUR",
                Instant.parse("2024-05-01T10:15:30Z"), Instant.parse("2024-05-01T10:15:31Z"));
    }

    @Test
    @DisplayName("GET /api/processed-orders/by-event/{eventId} returns the projection")
    void findsByEventId() throws Exception {
        ProcessedOrder stored = projection();
        given(repository.findByEventId(stored.getEventId())).willReturn(Optional.of(stored));

        mockMvc.perform(get("/api/processed-orders/by-event/{eventId}", stored.getEventId()))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.orderId").value(stored.getOrderId().toString()))
                .andExpect(jsonPath("$.eventId").value(stored.getEventId().toString()))
                .andExpect(jsonPath("$.customerId").value("customer-42"))
                .andExpect(jsonPath("$.totalPrice").value(259.98))
                .andExpect(jsonPath("$.currency").value("EUR"));
    }

    @Test
    @DisplayName("GET /api/processed-orders/by-event/{eventId} returns 404 before the event is consumed")
    void returns404WhenNotConsumedYet() throws Exception {
        UUID eventId = UUID.randomUUID();
        given(repository.findByEventId(eventId)).willReturn(Optional.empty());

        mockMvc.perform(get("/api/processed-orders/by-event/{eventId}", eventId))
                .andExpect(status().isNotFound());
    }

    @Test
    @DisplayName("GET /api/processed-orders/{orderId} returns the projection")
    void findsByOrderId() throws Exception {
        ProcessedOrder stored = projection();
        given(repository.findById(stored.getOrderId())).willReturn(Optional.of(stored));

        mockMvc.perform(get("/api/processed-orders/{orderId}", stored.getOrderId()))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.orderId").value(stored.getOrderId().toString()));
    }

    @Test
    @DisplayName("GET /api/processed-orders returns a paged list and clamps the limit")
    void listsOrders() throws Exception {
        given(repository.findAll(any(Pageable.class))).willReturn(new PageImpl<>(List.of(projection())));

        mockMvc.perform(get("/api/processed-orders").param("limit", "9999"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$[0].orderId").exists())
                .andExpect(jsonPath("$.length()").value(1));

        org.mockito.Mockito.verify(repository).findAll(Pageable.ofSize(500));
    }

    @Test
    @DisplayName("GET /api/processed-orders filters by customer id")
    void filtersByCustomer() throws Exception {
        given(repository.findByCustomerIdOrderByOccurredAtDesc(eq("customer-42")))
                .willReturn(List.of(projection()));

        mockMvc.perform(get("/api/processed-orders").param("customerId", "customer-42"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$[0].customerId").value("customer-42"));
    }

    @Test
    @DisplayName("GET /api/processed-orders/stats exposes the outcome counters")
    void exposesStats() throws Exception {
        given(stats.counts()).willReturn(Map.of(
                ProcessingOutcome.CREATED.name(), 5L,
                ProcessingOutcome.DUPLICATE_IGNORED.name(), 2L));

        mockMvc.perform(get("/api/processed-orders/stats"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.CREATED").value(5))
                .andExpect(jsonPath("$.DUPLICATE_IGNORED").value(2));
    }
}
