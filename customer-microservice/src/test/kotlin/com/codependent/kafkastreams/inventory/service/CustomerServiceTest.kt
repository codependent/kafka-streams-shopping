package com.codependent.kafkastreams.inventory.service

import com.codependent.kafkastreams.inventory.dto.Customer
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class CustomerServiceTest {

    private val customerService = CustomerService("test", "localhost:9092")

    @BeforeAll
    fun initializeStreams() {
        customerService.initializeStreams()
    }

    @AfterAll
    fun stopStreams() {
        customerService.stopStreams()
    }

    @Test
    fun shouldCreateCustomer() {
        customerService.createCustomer(Customer("55444333D", "Joey"))
        val customer = customerService.getCustomer("55444333D")
        assertNotNull(customer)
    }
}