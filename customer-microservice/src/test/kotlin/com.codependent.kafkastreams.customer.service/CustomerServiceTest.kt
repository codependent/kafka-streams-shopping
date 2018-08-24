package com.codependent.kafkastreams.customer.service

import com.codependent.kafkastreams.customer.dto.Customer
import org.junit.Assert
import org.junit.Test

class CustomerServiceTest {

    private val customerService = CustomerService("test", "localhost:9092")

    init {
        customerService.initializeStreams()
    }

    @Test
    fun shouldCreateCustomer() {
        customerService.createCustomer(Customer("55444333D", "Joey"))
        val customer = customerService.getCustomer("55444333D")
        Assert.assertNotNull(customer)
    }
}