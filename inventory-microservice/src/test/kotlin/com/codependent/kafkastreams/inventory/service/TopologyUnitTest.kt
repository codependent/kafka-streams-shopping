package com.codependent.kafkastreams.inventory.service

import com.codependent.kafkastreams.inventory.dto.Product
import com.codependent.kafkastreams.inventory.dto.ProductType
import com.codependent.kafkastreams.inventory.serdes.JsonPojoSerializer
import com.codependent.kafkastreams.inventory.streams.INVENTORY_STORE
import com.codependent.kafkastreams.inventory.streams.INVENTORY_TOPIC
import com.codependent.kafkastreams.inventory.streams.StreamsConfiguration
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.TopologyTestDriver
import org.apache.kafka.streams.state.KeyValueStore
import org.apache.kafka.streams.test.ConsumerRecordFactory
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import java.util.*


@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class TopologyUnitTest {

    private val config = Properties()
    private val streamsConfiguration = StreamsConfiguration("test", "dummy:1234")
    private val recordFactory: ConsumerRecordFactory<String, Product>
    private lateinit var testDriver: TopologyTestDriver
    private lateinit var store: KeyValueStore<String, Product>

    init {
        config[StreamsConfig.APPLICATION_ID_CONFIG] = "test"
        config[StreamsConfig.BOOTSTRAP_SERVERS_CONFIG] = "dummy:1234"
        recordFactory = ConsumerRecordFactory<String, Product>(INVENTORY_TOPIC, StringSerializer(), JsonPojoSerializer<Product>())

    }

    @BeforeEach
    fun initializeTestDriver() {
        testDriver = TopologyTestDriver(streamsConfiguration.topology(), config)
        store = testDriver.getKeyValueStore(INVENTORY_STORE)
    }

    @AfterEach
    fun tearDown() {
        testDriver.close()
    }

    @Test
    fun shouldAddProductsToInventoryStore() {
        val product = Product("0", "Kindle", ProductType.ELECTRONICS, "My Kindle", 10)
        testDriver.pipeInput(recordFactory.create(INVENTORY_TOPIC, "0", product))
        assertEquals(product, store.get("0"))
        assertEquals(10, store.get("0").units)
        testDriver.pipeInput(recordFactory.create(INVENTORY_TOPIC, "0", product))
        assertEquals(20, store.get("0").units)
    }

    @Test
    fun shouldAddAndDeleteProductFromInventoryStore() {
        val product = Product("0", "Kindle", ProductType.ELECTRONICS, "My Kindle", 10)
        testDriver.pipeInput(recordFactory.create(INVENTORY_TOPIC, "0", product))
        assertEquals(product, store.get("0"))
        assertEquals(10, store.get("0").units)
        testDriver.pipeInput(recordFactory.create(INVENTORY_TOPIC, "0",
                Product("0", "Kindle", ProductType.ELECTRONICS, "My Kindle", -1)))
        assertNull(store.get("0"))
    }
}