package com.codependent.kafkastreams.inventory.service

import com.codependent.kafkastreams.inventory.dto.Customer
import com.codependent.kafkastreams.inventory.streams.CUSTOMERS_STORE
import com.codependent.kafkastreams.inventory.streams.CUSTOMERS_TOPIC
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.errors.InvalidStateStoreException
import org.apache.kafka.streams.state.QueryableStoreTypes
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service

@Service
class CustomerService(private val streams: KafkaStreams,
                      private val customerProducer: Producer<String, Customer>) {

    private val logger = LoggerFactory.getLogger(this.javaClass)

    fun getCustomer(id: String): Customer? {
        var keyValueStore: ReadOnlyKeyValueStore<String, Customer>? = null
        while (keyValueStore == null) {
            try {
                keyValueStore = streams.store(CUSTOMERS_STORE, QueryableStoreTypes.keyValueStore<String, Customer>())
            } catch (ex: InvalidStateStoreException) {
                ex.printStackTrace()
                Thread.sleep(500)
            }
        }
        return keyValueStore.get(id)
    }

    fun deleteCustomer(id: String) {
        val record = ProducerRecord<String, Customer>(CUSTOMERS_TOPIC, id, null)
        val metadata = customerProducer.send(record).get()
        logger.info("{}", metadata)
        customerProducer.flush()
    }

    fun createCustomer(customer: Customer) {
        val record = ProducerRecord<String, Customer>(CUSTOMERS_TOPIC, customer.id, customer)
        val metadata = customerProducer.send(record).get()
        logger.info("{}", metadata)
        customerProducer.flush()
    }

}
/*
fun main(args: Array<String>) {
    val customerService = CustomerService("main", "localhost:9092")
    customerService.initializeStreams()
    customerService.createCustomer(Customer("53", "Joey"))
    val customer = customerService.getCustomer("53")
    println(customer)
    customerService.stopStreams()
}*/