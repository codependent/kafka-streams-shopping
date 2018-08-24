package com.codependent.kafkastreams.customer.web

import com.codependent.kafkastreams.customer.dto.Customer
import com.codependent.kafkastreams.customer.service.CustomerService
import org.springframework.web.bind.annotation.*

@RestController
@RequestMapping("/customers")
class CustomerRestController(private val customerService: CustomerService) {

    @GetMapping("/{id}")
    fun getCustomer(@PathVariable id: String): Customer {
        //return customerService.getCustomer(id)
        TODO()
    }

    @PostMapping("/{id}")
    fun createCustomer(@RequestBody customer: Customer) {
        customerService.createCustomer(customer)
    }

}