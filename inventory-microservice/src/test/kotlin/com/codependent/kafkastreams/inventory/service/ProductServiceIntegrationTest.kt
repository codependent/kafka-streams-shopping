package com.codependent.kafkastreams.inventory.service

import com.codependent.kafkastreams.inventory.dto.Product
import com.codependent.kafkastreams.inventory.dto.ProductType
import com.codependent.kafkastreams.inventory.streams.StreamsConfiguration
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ProductServiceIntegrationTest {

    private val streamsConfiguration = StreamsConfiguration("test", "localhost:9092")
    private val kafkaStreams = streamsConfiguration.kafkaStreams()
    private val inventoryProducer = streamsConfiguration.inventoryProducer()
    private val inventoryService = InventoryService(kafkaStreams, inventoryProducer)

    @BeforeAll
    fun initializeStreams() {
        streamsConfiguration.startStreams(kafkaStreams)
    }

    @AfterAll
    fun stopStreams() {
        streamsConfiguration.stopStreams(kafkaStreams, inventoryProducer)
    }

    @Test
    fun shouldAddAndDeleteProduct() {
        inventoryService.addProduct(Product("0", "Kindle", ProductType.ELECTRONICS, "A Kindle", 10))
        inventoryService.addProduct(Product("0", "Kindle", ProductType.ELECTRONICS, "A Kindle", 5))
        Thread.sleep(5000)
        var product = inventoryService.getProduct("0")
        assertNotNull(product)
        assertEquals(15, product?.units)
        inventoryService.deleteProduct("0")
        Thread.sleep(5000)
        product = inventoryService.getProduct("0")
        assertNull(product)
    }
}