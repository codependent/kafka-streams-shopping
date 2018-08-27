package com.codependent.kafkastreams.inventory.service

import com.codependent.kafkastreams.inventory.dto.Product
import com.codependent.kafkastreams.inventory.dto.ProductType
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ProductServiceTest {

    private val inventoryService = InventoryService("test", "localhost:9092")

    @BeforeAll
    fun initializeStreams() {
        inventoryService.initializeStreams()
    }

    @AfterAll
    fun stopStreams() {
        inventoryService.stopStreams()
    }

    @Test
    fun shouldAddAndDeleteProduct() {
        //inventoryService.deleteProduct("0")
        inventoryService.addProduct(Product("0", "Kindle", ProductType.ELECTRONICS, "A Kindle", 10))
        inventoryService.getProduct("0")
        inventoryService.addProduct(Product("0", "Kindle", ProductType.ELECTRONICS, "A Kindle", 5))
        Thread.sleep(2000)
        val product = inventoryService.getProduct("0")
        assertNotNull(product)
        assertEquals(15, product?.units)
        inventoryService.deleteProduct("0")
        val deletedProduct = inventoryService.getProduct("0")
        Thread.sleep(2000)
        assertNull(deletedProduct)
    }
}