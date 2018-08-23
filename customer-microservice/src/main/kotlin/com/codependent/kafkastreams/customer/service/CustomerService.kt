package com.codependent.kafkastreams.customer.service

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

const val CUSTOMERS_TOPIC = "customers"
const val CUSTOMERS_STORE = "customers-store"

@Service
class CustomerService(@Value("\${spring.application.name}") private val applicationName: String,
                      @Value("\${kafka.boostrap-servers}") private val kafkaBootstrapServers: String) {

    private lateinit var streams: KafkaStreams
    private val logger = LoggerFactory.getLogger(this.javaClass)

    @PostConstruct
    fun initializeStreams() {
        val props = Properties()
        props[StreamsConfig.APPLICATION_ID_CONFIG] = applicationName
        props[StreamsConfig.BOOTSTRAP_SERVERS_CONFIG] = kafkaBootstrapServers
        props[StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG] = Serdes.String().javaClass
        props[StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG] = Serdes.String().javaClass

        val builder = StreamsBuilder()
        builder.table(CUSTOMERS_TOPIC, Materialized.`as`<String, Long, KeyValueStore<Bytes, ByteArray>>(CUSTOMERS_STORE))

        streams = KafkaStreams(builder.build(), props)
        streams.start()
    }

    @PreDestroy
    fun stopStreams() {
        logger.info("*********** Closing streams ***********")
        streams.close()
    }

    fun getCustomer(word: String): Long? {
        val keyValueStore = streams.store(COUNTS_STORE, QueryableStoreTypes.keyValueStore<String, Long>())
        return keyValueStore.get(word)
    }

}