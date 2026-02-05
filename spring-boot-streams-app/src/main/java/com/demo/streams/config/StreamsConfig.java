package com.demo.streams.config;

import com.demo.streams.model.Models.*;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.support.serializer.JsonSerde;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

@Configuration
@RequiredArgsConstructor
@Slf4j
public class StreamsConfig {

    @Value("${app.topics.customer}")
    private String customerTopic;

    @Value("${app.topics.orders}")
    private String ordersTopic;

    @Value("${app.topics.payment}")
    private String paymentTopic;

    @Value("${app.topics.enriched-orders}")
    private String enrichedOrdersTopic;

    @Value("${app.topics.order-paid}")
    private String orderPaidTopic;

    @Value("${app.topics.daily-revenue}")
    private String dailyRevenueTopic;

    private final JdbcTemplate jdbcTemplate;

    @Bean
    public KStream<String, String> kStream(StreamsBuilder builder) {
        ObjectMapper mapper = new ObjectMapper();

        // Serdes
        JsonSerde<Customer> customerSerde = new JsonSerde<>(Customer.class);
        JsonSerde<Order> orderSerde = new JsonSerde<>(Order.class);
        JsonSerde<Payment> paymentSerde = new JsonSerde<>(Payment.class);
        JsonSerde<EnrichedOrder> enrichedOrderSerde = new JsonSerde<>(EnrichedOrder.class);
        JsonSerde<OrderPaidEvent> orderPaidSerde = new JsonSerde<>(OrderPaidEvent.class);
        JsonSerde<DailyRevenue> revenueSerde = new JsonSerde<>(DailyRevenue.class);

        // 1. Consumer Customer Table (GlobalKTable or KTable)
        // We use KTable here keyed by ID. Debezium payload key defines ID.
        // But Debezium key is complex JSON {"ID": ...}. We assume simple extraction or
        // re-keying is needed.
        // Actually, Kafka Connect JSON converter often enables schemas, but we disabled
        // them in config.
        // Key is struct/json: {"id": 1}.

        // Helper to extract data from Debezium JSON

        KTable<Long, Customer> customerTable = builder
                .stream(customerTopic, Consumed.with(Serdes.String(), Serdes.String()))
                .mapValues(val -> extract(val, Customer.class, mapper))
                .filter((k, v) -> v != null)
                .selectKey((k, v) -> v.getId()) // Rekey by ID just in case
                .toTable(Materialized.with(Serdes.Long(), customerSerde));

        // 2. Stream Order
        KStream<Long, Order> orderStream = builder.stream(ordersTopic, Consumed.with(Serdes.String(), Serdes.String()))
                .mapValues(val -> extract(val, Order.class, mapper))
                .filter((k, v) -> v != null)
                .selectKey((k, v) -> v.getId());

        // 3. Stream Payments
        KStream<Long, Payment> paymentStream = builder
                .stream(paymentTopic, Consumed.with(Serdes.String(), Serdes.String()))
                .mapValues(val -> extract(val, Payment.class, mapper))
                .filter((k, v) -> v != null)
                .selectKey((k, v) -> v.getOrderId()); // Key by OrderID to join

        // 4. Join Orders + Customer -> EnrichedOrder
        KStream<Long, EnrichedOrder> enrichedOrders = orderStream.join(customerTable,
                (order, customer) -> new EnrichedOrder(order, customer),
                Joined.with(Serdes.Long(), orderSerde, customerSerde));

        enrichedOrders.to(enrichedOrdersTopic, Produced.with(Serdes.Long(), enrichedOrderSerde));

        // 5. Join Payment + EnrichedOrder -> OrderPaidEvent
        // Require re-keying EnrichedOrder to ID? No, it's keyed by OrderID (v.getId()
        // from Step 2)
        // Payment is keyed by OrderID (Step 3)
        // Join
        KStream<Long, OrderPaidEvent> paidEvents = paymentStream.join(enrichedOrders,
                (payment, enriched) -> new OrderPaidEvent(
                        enriched.getOrder().getId(),
                        payment.getPaidAmount(),
                        enriched.getCustomer().getEmail(),
                        payment.getMethod()),
                JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofMinutes(5)),
                StreamJoined.with(Serdes.Long(), paymentSerde, enrichedOrderSerde));

        paidEvents.to(orderPaidTopic, Produced.with(Serdes.Long(), orderPaidSerde));

        // 6. Aggregate Daily Revenue
        // We need a date key. `createdAt` from Order? Or Processing time?
        // Let's use Processing time or extraction from Order if available.
        // Assuming we want Order date.

        KTable<Windowed<String>, BigDecimal> dailyStats = paidEvents
                .map((k, v) -> new KeyValue<>("revenue", v.getTotalAmount()))
                .groupByKey(Grouped.with(Serdes.String(), new JsonSerde<>(BigDecimal.class))) // Key is "revenue" string
                                                                                              // (global agg)
                .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofDays(1)))
                .reduce(BigDecimal::add, Materialized.with(Serdes.String(), new JsonSerde<>(BigDecimal.class)));

        // Convert to Stream and map to DailyRevenue
        KStream<String, DailyRevenue> revenueStream = dailyStats.toStream()
                .map((windowedKey, total) -> {
                    String date = Instant.ofEpochMilli(windowedKey.window().start())
                            .atZone(ZoneId.systemDefault())
                            .format(DateTimeFormatter.ISO_LOCAL_DATE);
                    return new KeyValue<>(date, new DailyRevenue(date, total));
                });

        revenueStream.to(dailyRevenueTopic, Produced.with(Serdes.String(), revenueSerde));

        // 7. Sink to Oracle (Side effect)
        revenueStream.foreach((k, v) -> {
            log.info("Persisting Revenue Report: {}", v);
            upsertRevenue(v);
        });

        return null; // Just building topology
    }

    private <T> T extract(String json, Class<T> clazz, ObjectMapper mapper) {
        try {
            // Robust generic parsing
            com.fasterxml.jackson.databind.JavaType type = mapper.getTypeFactory()
                    .constructParametricType(DebeziumEvent.class, clazz);
            DebeziumEvent<T> event = mapper.readValue(json, type);

            if (event.getPayload() != null && event.getPayload().getAfter() != null) {
                return event.getPayload().getAfter();
            }
        } catch (Exception e) {
            // log.warn("Skipping message: {}", e.getMessage());
        }
        return null;
    }

    private void upsertRevenue(DailyRevenue dr) {
        try {
            // Simple upsert or insert logic
            // Oracle 23c supports proper UPSERT? merge into...
            String sql = "MERGE INTO REVENUE_REPORT r USING (SELECT ? as dt, ? as rev FROM dual) s " +
                    "ON (r.report_date = s.dt) " +
                    "WHEN MATCHED THEN UPDATE SET r.total_revenue = s.rev, r.updated_at = CURRENT_TIMESTAMP " +
                    "WHEN NOT MATCHED THEN INSERT (report_date, total_revenue) VALUES (s.dt, s.rev)";
            jdbcTemplate.update(sql, dr.getDate(), dr.getRevenue());
        } catch (Exception e) {
            log.error("Failed to write to DB", e);
        }
    }
}
