package com.codependent.kafkastreams.inventory.service

import com.codependent.kafkastreams.inventory.dto.Customer
import com.codependent.kafkastreams.inventory.streams.StreamsConfiguration
import org.junit.jupiter.api.*
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertNull

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class CustomerServiceTest {

    private val streamsConfiguration = StreamsConfiguration("test", "localhost:9092")
    private val kafkaStreams = streamsConfiguration.kafkaStreams()
    private val customerProducer = streamsConfiguration.customerProducer()
    private val customerService = CustomerService(kafkaStreams, customerProducer)

    @BeforeAll
    fun initializeStreams() {
        streamsConfiguration.startStreams(kafkaStreams)
    }

    @AfterAll
    fun stopStreams() {
        streamsConfiguration.stopStreams(kafkaStreams, customerProducer)
    }

    @Test
    fun shouldCreateAndDeleteCustomer() {
        customerService.createCustomer(Customer("55444333D", "Joey"))
        Thread.sleep(5000)
        val customer = customerService.getCustomer("55444333D")
        assertNotNull(customer)
        customerService.deleteCustomer("55444333D")
        Thread.sleep(5000)
        assertNull(customerService.getCustomer("55444333D"))
    }

}