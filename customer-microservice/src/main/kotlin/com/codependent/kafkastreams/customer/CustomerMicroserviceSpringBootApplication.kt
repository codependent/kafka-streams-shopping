package com.codependent.kafkastreams.customer

import org.springframework.boot.SpringApplication
import org.springframework.boot.autoconfigure.SpringBootApplication

@SpringBootApplication
class CustomerMicroserviceSpringBootApplication

fun main(args: Array<String>) {
    SpringApplication.run(CustomerMicroserviceSpringBootApplication::class.java, *args)
}