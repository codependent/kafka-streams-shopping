package com.codependent.kafkastreams.inventory.web

import com.codependent.kafkastreams.inventory.dto.Product
import com.codependent.kafkastreams.inventory.service.InventoryService
import org.springframework.web.bind.annotation.*

@RestController
@RequestMapping("/inventory")
class InventoryRestController(private val inventoryService: InventoryService) {

    @GetMapping("/{productId}")
    fun getCustomer(@PathVariable productId: String): Product? {
        return inventoryService.getProduct(productId)
    }

    @PostMapping
    fun createCustomer(@RequestBody product: Product) {
        inventoryService.addProduct(product)
    }

}