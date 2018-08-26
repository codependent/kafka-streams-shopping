package com.codependent.kafkastreams.inventory.web

import com.codependent.kafkastreams.inventory.dto.Customer
import com.codependent.kafkastreams.inventory.service.CustomerService
import org.springframework.web.bind.annotation.*

@RestController
@RequestMapping("/customers")
class CustomerRestController(private val customerService: CustomerService) {

    @GetMapping("/{id}")
    fun getCustomer(@PathVariable id: String): Customer? {
        return customerService.getCustomer(id)
    }

    @PostMapping
    fun createCustomer(@RequestBody customer: Customer) {
        customerService.createCustomer(customer)
    }

}