package com.codependent.kafkastreams.inventory.service

import com.codependent.kafkastreams.inventory.dto.Product
import com.codependent.kafkastreams.inventory.dto.ProductType
import com.codependent.kafkastreams.inventory.serdes.JsonPojoDeserializer
import com.codependent.kafkastreams.inventory.serdes.JsonPojoSerializer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.kafka.common.utils.Bytes
import org.apache.kafka.streams.*
import org.apache.kafka.streams.errors.InvalidStateStoreException
import org.apache.kafka.streams.kstream.Materialized
import org.apache.kafka.streams.kstream.Produced
import org.apache.kafka.streams.state.KeyValueStore
import org.apache.kafka.streams.state.QueryableStoreTypes
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Service
import java.util.*
import javax.annotation.PostConstruct
import javax.annotation.PreDestroy


const val INVENTORY_TOPIC = "inventory"
const val INVENTORY_STORE = "inventory-store"

@Service
class InventoryService(val topology: Topology,
                       @Value("\${spring.application.name}") private val applicationName: String,
                       @Value("\${kafka.boostrap-servers}") private val kafkaBootstrapServers: String) {

    private lateinit var streams: KafkaStreams
    private val inventoryProducer: Producer<String, Product>
    private val logger = LoggerFactory.getLogger(this.javaClass)

    init {
        inventoryProducer = createInventoryProducer()
    }

    @PostConstruct
    fun startStreams() {
        val props = Properties()
        props[StreamsConfig.APPLICATION_ID_CONFIG] = applicationName
        props[StreamsConfig.BOOTSTRAP_SERVERS_CONFIG] = kafkaBootstrapServers
        streams = KafkaStreams(topology, props)
        streams.start()
    }

    @PreDestroy
    fun stopStreams() {
        logger.info("*********** Closing streams ***********")
        inventoryProducer.close()
        streams.close()
    }

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


    private fun createInventoryProducer(): Producer<String, Product> {
        val props = Properties()
        props[ProducerConfig.BOOTSTRAP_SERVERS_CONFIG] = kafkaBootstrapServers
        props[ProducerConfig.CLIENT_ID_CONFIG] = "InventoryService"

        return KafkaProducer(props, StringSerializer(), JsonPojoSerializer<Product>())
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