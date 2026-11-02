package com.example.orders.producer.api;

import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Size;

import java.math.BigDecimal;

/**
 * Request body of {@code POST /api/orders}.
 *
 * <p>Currency is optional and defaults to EUR in the service layer.</p>
 */
public record CreateOrderRequest(

        @NotBlank(message = "customerId must not be blank")
        @Size(max = 64, message = "customerId must be at most 64 characters")
        String customerId,

        @NotBlank(message = "product must not be blank")
        @Size(max = 255, message = "product must be at most 255 characters")
        String product,

        @NotNull(message = "quantity is required")
        @Min(value = 1, message = "quantity must be >= 1")
        @Max(value = 100_000, message = "quantity must be <= 100000")
        Integer quantity,

        @NotNull(message = "totalPrice is required")
        @DecimalMin(value = "0.0", inclusive = true, message = "totalPrice must be >= 0")
        BigDecimal totalPrice,

        @Pattern(regexp = "[A-Z]{3}", message = "currency must be an ISO-4217 code like EUR")
        String currency
) {

    public String currencyOrDefault() {
        return currency == null ? "EUR" : currency;
    }
}
