package com.demo.streams.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.AllArgsConstructor;

import java.math.BigDecimal;

public class Models {

    @Data
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class DebeziumEvent<T> {
        private Payload<T> payload;

        @Data
        @JsonIgnoreProperties(ignoreUnknown = true)
        public static class Payload<T> {
            private T before;
            private T after;
            private String op; // c, u, d, r
        }
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Customer {
        @JsonProperty("ID")
        private Long id;
        @JsonProperty("NAME")
        private String name;
        @JsonProperty("EMAIL")
        private String email;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Order {
        @JsonProperty("ID")
        private Long id;
        @JsonProperty("CUSTOMER_ID")
        private Long customerId;
        @JsonProperty("AMOUNT")
        private BigDecimal amount;
        @JsonProperty("STATUS")
        private String status;
        @JsonProperty("CREATED_AT") // Debezium usually sends timestamps as long or string depending on config
        private Long createdAt;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Payment {
        @JsonProperty("ID")
        private Long id;
        @JsonProperty("ORDER_ID")
        private Long orderId;
        @JsonProperty("METHOD")
        private String method;
        @JsonProperty("PAID_AMOUNT")
        private BigDecimal paidAmount;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class EnrichedOrder {
        private Order order;
        private Customer customer;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class OrderPaidEvent {
        private Long orderId;
        private BigDecimal totalAmount;
        private String customerEmail;
        private String paymentMethod;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class DailyRevenue {
        private String date; // YYYY-MM-DD
        private BigDecimal revenue;
    }
}
