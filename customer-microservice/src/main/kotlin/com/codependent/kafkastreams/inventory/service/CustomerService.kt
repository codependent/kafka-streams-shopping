package com.codependent.kafkastreams.inventory.service

import com.codependent.kafkastreams.inventory.dto.Customer
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
import org.apache.kafka.streams.Consumed
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.StreamsConfig
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


const val CUSTOMERS_TOPIC = "customers"
const val CUSTOMERS_STORE = "customers-store"

@Service
class CustomerService(@Value("\${spring.application.name}") private val applicationName: String,
                      @Value("\${kafka.boostrap-servers}") private val kafkaBootstrapServers: String) {

    private lateinit var streams: KafkaStreams
    private lateinit var customerProducer: Producer<String, Customer>
    private val logger = LoggerFactory.getLogger(this.javaClass)

    @PostConstruct
    fun initializeStreams() {
        val customerSerde: Serde<Customer> = Serdes.serdeFrom(JsonPojoSerializer<Customer>(), JsonPojoDeserializer(Customer::class.java))
        val props = Properties()
        props[StreamsConfig.APPLICATION_ID_CONFIG] = applicationName
        props[StreamsConfig.BOOTSTRAP_SERVERS_CONFIG] = kafkaBootstrapServers

        customerProducer = createCustomerProducer()

        val builder = StreamsBuilder()
        builder.table(CUSTOMERS_TOPIC,
                Consumed.with(Serdes.String(), customerSerde),
                Materialized.`as`<String, Customer, KeyValueStore<Bytes, ByteArray>>(CUSTOMERS_STORE)
                        .withKeySerde(Serdes.String())
                        .withValueSerde(customerSerde))
                .toStream().to("customers-processed", Produced.with(Serdes.String(), customerSerde))

        streams = KafkaStreams(builder.build(), props)
        streams.start()
    }

    @PreDestroy
    fun stopStreams() {
        logger.info("*********** Closing streams ***********")
        customerProducer.close()
        streams.close()
    }

    fun getCustomer(id: String): Customer? {
        var keyValueStore: ReadOnlyKeyValueStore<String, Customer>? = null
        while (keyValueStore == null) {
            try {
                keyValueStore = streams.store(CUSTOMERS_STORE, QueryableStoreTypes.keyValueStore<String, Customer>())
            } catch (ex: InvalidStateStoreException) {
                ex.printStackTrace()
                Thread.sleep(100)
            }
        }
        return keyValueStore.get(id)
    }

    fun createCustomer(customer: Customer) {
        val record = ProducerRecord<String, Customer>(CUSTOMERS_TOPIC, customer.id, customer)

        val metadata = customerProducer.send(record).get()
        logger.info("{}", metadata)
        customerProducer.flush()
    }

    private fun createCustomerProducer(): Producer<String, Customer> {
        val props = Properties()
        props[ProducerConfig.BOOTSTRAP_SERVERS_CONFIG] = kafkaBootstrapServers
        props[ProducerConfig.CLIENT_ID_CONFIG] = "CustomerService"

        return KafkaProducer(props, StringSerializer(), JsonPojoSerializer<Customer>())
    }

}

fun main(args: Array<String>) {
    val customerService = CustomerService("main", "localhost:9092")
    customerService.initializeStreams()
    customerService.createCustomer(Customer("53", "Joey"))
    val customer = customerService.getCustomer("53")
    println(customer)
    customerService.stopStreams()
}