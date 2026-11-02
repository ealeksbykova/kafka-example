package com.example.orders.producer.api;

import com.example.orders.producer.domain.Order;
import com.example.orders.producer.domain.OrderStatus;
import com.example.orders.producer.messaging.OrderApplicationService;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.http.MediaType;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.UUID;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * Web layer contract of the producer: status codes, validation errors and the JSON field names the
 * e2e test relies on. No database and no broker are involved.
 */
@WebMvcTest(OrderController.class)
class OrderControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private ObjectMapper objectMapper;

    @MockitoBean
    private OrderApplicationService service;

    private static Order order(OrderStatus status) {
        Order order = Order.create("customer-42", "Mechanical keyboard", 2, new BigDecimal("259.98"),
                "EUR", Instant.parse("2024-05-01T10:15:30Z"));
        if (status == OrderStatus.PUBLISHED) {
            order.markPublished(Instant.parse("2024-05-01T10:15:31Z"));
        }
        return order;
    }

    private String validBody() {
        return """
                {
                  "customerId": "customer-42",
                  "product": "Mechanical keyboard",
                  "quantity": 2,
                  "totalPrice": 259.98,
                  "currency": "EUR"
                }
                """;
    }

    @Test
    @DisplayName("POST /api/orders returns 201, a Location header and the generated eventId")
    void createsOrder() throws Exception {
        Order created = order(OrderStatus.PUBLISHED);
        given(service.createOrder(any(CreateOrderRequest.class))).willReturn(created);

        mockMvc.perform(post("/api/orders").contentType(MediaType.APPLICATION_JSON).content(validBody()))
                .andExpect(status().isCreated())
                .andExpect(header().exists("Location"))
                .andExpect(jsonPath("$.id").value(created.getId().toString()))
                .andExpect(jsonPath("$.eventId").value(created.getEventId().toString()))
                .andExpect(jsonPath("$.status").value("PUBLISHED"))
                .andExpect(jsonPath("$.currency").value("EUR"))
                .andExpect(jsonPath("$.totalPrice").value(259.98));
    }

    @Test
    @DisplayName("POST /api/orders rejects an invalid body with field level details")
    void rejectsInvalidBody() throws Exception {
        String invalid = """
                {"customerId": "", "product": "", "quantity": 0, "totalPrice": -1, "currency": "EURO"}
                """;

        mockMvc.perform(post("/api/orders").contentType(MediaType.APPLICATION_JSON).content(invalid))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.title").value("Invalid request"))
                .andExpect(jsonPath("$.errors.customerId").exists())
                .andExpect(jsonPath("$.errors.product").exists())
                .andExpect(jsonPath("$.errors.quantity").exists())
                .andExpect(jsonPath("$.errors.currency").exists());

        verifyNoInteractions(service);
    }

    @Test
    @DisplayName("POST /api/orders rejects a malformed JSON payload")
    void rejectsMalformedJson() throws Exception {
        mockMvc.perform(post("/api/orders").contentType(MediaType.APPLICATION_JSON).content("{notJson"))
                .andExpect(status().isBadRequest());
    }

    @Test
    @DisplayName("GET /api/orders/{id} returns the order")
    void getsOrder() throws Exception {
        Order stored = order(OrderStatus.PUBLISH_FAILED);
        given(service.getOrder(stored.getId())).willReturn(stored);

        mockMvc.perform(get("/api/orders/{id}", stored.getId()))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.id").value(stored.getId().toString()))
                .andExpect(jsonPath("$.status").value("PUBLISH_FAILED"))
                .andExpect(jsonPath("$.publishedAt").doesNotExist());
    }

    @Test
    @DisplayName("GET /api/orders/{id} maps a missing order to 404")
    void returns404ForUnknownOrder() throws Exception {
        UUID id = UUID.randomUUID();
        given(service.getOrder(id)).willThrow(new NoSuchElementException("Order " + id + " not found"));

        mockMvc.perform(get("/api/orders/{id}", id))
                .andExpect(status().isNotFound())
                .andExpect(jsonPath("$.title").value("Resource not found"));
    }

    @Test
    @DisplayName("GET /api/orders clamps the limit into a safe range")
    void clampsLimit() throws Exception {
        given(service.findOrders(eq(null), anyInt())).willReturn(List.of());

        mockMvc.perform(get("/api/orders").param("limit", "5000"))
                .andExpect(status().isOk());

        org.mockito.Mockito.verify(service).findOrders(null, 100);
    }

    @Test
    @DisplayName("GET /api/orders/by-event/{eventId} returns 404 when the event is unknown")
    void returns404ForUnknownEventId() throws Exception {
        UUID eventId = UUID.randomUUID();
        given(service.findByEventId(eventId)).willReturn(java.util.Optional.empty());

        mockMvc.perform(get("/api/orders/by-event/{eventId}", eventId))
                .andExpect(status().isNotFound());
    }
}
