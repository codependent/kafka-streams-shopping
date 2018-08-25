package com.codependent.kafkastreams.customer.service

import com.codependent.kafkastreams.customer.dto.Customer
import com.codependent.kafkastreams.customer.serdes.JsonPojoDeserializer
import com.codependent.kafkastreams.customer.serdes.JsonPojoSerializer
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.KotlinModule
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
import org.apache.kafka.streams.processor.StateStore
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
    private val logger = LoggerFactory.getLogger(this.javaClass)
    private val objectMapper = ObjectMapper().registerModule(KotlinModule())

    @PostConstruct
    fun initializeStreams() {
        val customerSerde: Serde<Customer> = Serdes.serdeFrom(JsonPojoSerializer<Customer>(), JsonPojoDeserializer(Customer::class.java))
        val props = Properties()
        props[StreamsConfig.APPLICATION_ID_CONFIG] = applicationName
        props[StreamsConfig.BOOTSTRAP_SERVERS_CONFIG] = kafkaBootstrapServers

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
        streams.close()
    }

    fun getCustomer(id: String): Customer? {
        var keyValueStore: ReadOnlyKeyValueStore<String, Customer>? = null
        while(keyValueStore == null) {
            try {
                keyValueStore = streams.store(CUSTOMERS_STORE, QueryableStoreTypes.keyValueStore<String, Customer>())
            } catch (ex: InvalidStateStoreException) {
                ex.printStackTrace()
            }
        }
        val customer = keyValueStore.get(id)
        return customer
    }

    fun createCustomer(customer: Customer) {
        val producer = createProducer()
        val record = ProducerRecord<String, Customer>(CUSTOMERS_TOPIC, customer.id, customer)

        val metadata = producer.send(record).get()
        logger.info("{}", metadata)
        producer.flush()
        producer.close()
    }

    private fun createProducer(): Producer<String, Customer> {
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