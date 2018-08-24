package com.codependent.kafkastreams.customer.service

import com.codependent.kafkastreams.customer.dto.Customer
import com.codependent.kafkastreams.customer.serdes.JsonPojoDeserializer
import com.codependent.kafkastreams.customer.serdes.JsonPojoSerializer
import com.codependent.kafkastreams.util.MetadataService
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.JsonSerializer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.common.utils.Bytes
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.kstream.Materialized
import org.apache.kafka.streams.state.KeyValueStore
import org.apache.kafka.streams.state.QueryableStoreTypes
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Service
import java.util.*
import javax.annotation.PostConstruct
import javax.annotation.PreDestroy
import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.kafka.common.serialization.StringSerializer


const val CUSTOMERS_TOPIC = "customers"
const val CUSTOMERS_STORE = "customers-store"

@Service
class CustomerService(@Value("\${spring.application.name}") private val applicationName: String,
                      @Value("\${kafka.boostrap-servers}") private val kafkaBootstrapServers: String) {

    private lateinit var streams: KafkaStreams
    private lateinit var metadataService: MetadataService
    private val logger = LoggerFactory.getLogger(this.javaClass)
    private val objectMapper = ObjectMapper()

    @PostConstruct
    fun initializeStreams() {
        val props = Properties()
        props[StreamsConfig.APPLICATION_ID_CONFIG] = applicationName
        props[StreamsConfig.BOOTSTRAP_SERVERS_CONFIG] = kafkaBootstrapServers
        props[StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG] = Serdes.String().javaClass
        //props[StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG] = Serdes.serdeFrom(JsonPojoSerializer<Customer>(), JsonPojoDeserializer(Customer::class.java))
        props[StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG] = "org.apache.kafka.connect.json.JsonSerializer"
        val builder = StreamsBuilder()
        builder.table(CUSTOMERS_TOPIC, Materialized.`as`<String, JsonNode, KeyValueStore<Bytes, ByteArray>>(CUSTOMERS_STORE))

        streams = KafkaStreams(builder.build(), props)
        metadataService = MetadataService(streams)
        streams.start()
    }

    @PreDestroy
    fun stopStreams() {
        logger.info("*********** Closing streams ***********")
        streams.close()
    }

    fun getCustomer(id: String): Customer {
        val keyValueStore = streams.store(CUSTOMERS_STORE, QueryableStoreTypes.keyValueStore<String, JsonNode>())
        return objectMapper.treeToValue(keyValueStore.get(id), Customer::class.java)
    }

    fun createCustomer(customer: Customer) {
        val producer = createProducer()
        val record = ProducerRecord<String, Customer>(CUSTOMERS_TOPIC, customer.name, customer)

        val metadata = producer.send(record).get()
        println(metadata)
        producer.flush()
        producer.close()
    }

    private fun createProducer(): Producer<String, Customer> {
        val props = Properties()
        props[ProducerConfig.BOOTSTRAP_SERVERS_CONFIG] = kafkaBootstrapServers
        props[ProducerConfig.CLIENT_ID_CONFIG] = "CustomerService"
        //props[ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG] = "org.apache.kafka.common.serialization.StringSerializer"
        //props[ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG] = "org.apache.kafka.connect.json.JsonSerializer"
        return KafkaProducer(props, StringSerializer(), JsonPojoSerializer<Customer>())
    }

}