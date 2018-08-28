package com.codependent.kafkastreams.inventory.service

import com.codependent.kafkastreams.inventory.dto.Product
import com.codependent.kafkastreams.inventory.dto.ProductType
import com.codependent.kafkastreams.inventory.streams.INVENTORY_STORE
import com.codependent.kafkastreams.inventory.streams.INVENTORY_TOPIC
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.errors.InvalidStateStoreException
import org.apache.kafka.streams.state.QueryableStoreTypes
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service

@Service
class InventoryService(private val streams: KafkaStreams,
                       private val inventoryProducer: Producer<String, Product>) {

    private val logger = LoggerFactory.getLogger(this.javaClass)

    fun getProduct(id: String): Product? {
        var keyValueStore: ReadOnlyKeyValueStore<String, Product>? = null
        while (keyValueStore == null) {
            try {
                keyValueStore = streams.store(INVENTORY_STORE, QueryableStoreTypes.keyValueStore<String, Product>())
            } catch (ex: InvalidStateStoreException) {
                ex.printStackTrace()
                Thread.sleep(500)
            }
        }
        return keyValueStore.get(id)
    }

    fun addProduct(product: Product) {
        val record = ProducerRecord<String, Product>(INVENTORY_TOPIC, product.id, product)
        val metadata = inventoryProducer.send(record).get()
        logger.info("{}", metadata)
        inventoryProducer.flush()
    }

    fun deleteProduct(id: String) {
        val record = ProducerRecord<String, Product>(INVENTORY_TOPIC, id, Product(id, "", ProductType.ELECTRONICS, "", -1))
        val metadata = inventoryProducer.send(record).get()
        logger.info("{}", metadata)
        inventoryProducer.flush()
    }

}
/*
fun main(args: Array<String>) {
    val inventoryService = InventoryService("main", "localhost:9092")
    inventoryService.startStreams()
    inventoryService.addProduct(Product("1", "Kindle", ProductType.ELECTRONICS, "A Kindle", 10))
    inventoryService.addProduct(Product("1", "Kindle", ProductType.ELECTRONICS, "A Kindle", 5))
    val product = inventoryService.getProduct("1")
    println(product)
    inventoryService.stopStreams()
}*/