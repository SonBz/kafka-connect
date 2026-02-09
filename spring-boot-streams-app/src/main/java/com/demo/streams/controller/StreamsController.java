package com.demo.streams.controller;

import com.demo.streams.model.Models.Customer;
import lombok.RequiredArgsConstructor;

import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/customers")
@RequiredArgsConstructor
public class StreamsController {

    private final StreamsBuilderFactoryBean factoryBean;

    @GetMapping("/{id}")
    public Customer getCustomer(@PathVariable Long id) {
        ReadOnlyKeyValueStore<Long, Customer> store = factoryBean
                .getKafkaStreams()
                .store(StoreQueryParameters.fromNameAndType("customer-store", QueryableStoreTypes.keyValueStore()));

        return store.get(id);
    }
}
