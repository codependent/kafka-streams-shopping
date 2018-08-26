package com.codependent.kafkastreams.inventory.dto

data class Product(var id: String, var name: String, var type: ProductType, var description: String, var units: Int)
