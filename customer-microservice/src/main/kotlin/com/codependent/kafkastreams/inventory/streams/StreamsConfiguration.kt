package com.codependent.kafkastreams.inventory.streams

import com.codependent.kafkastreams.inventory.dto.Customer
import com.codependent.kafkastreams.inventory.serdes.JsonPojoDeserializer
import com.codependent.kafkastreams.inventory.serdes.JsonPojoSerializer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.kafka.common.utils.Bytes
import org.apache.kafka.streams.*
import org.apache.kafka.streams.kstream.Materialized
import org.apache.kafka.streams.kstream.Produced
import org.apache.kafka.streams.state.KeyValueStore
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import java.util.*
import javax.annotation.PostConstruct
import javax.annotation.PreDestroy

const val CUSTOMERS_TOPIC = "customers"
const val CUSTOMERS_STORE = "customers-store"

@Configuration
class StreamsConfiguration(@Value("\${spring.application.name}") private val applicationName: String,
                           @Value("\${kafka.boostrap-servers}") private val kafkaBootstrapServers: String) {

    private val logger = LoggerFactory.getLogger(javaClass)

    @PostConstruct
    fun startStreams(kafkaStreams: KafkaStreams) {
        kafkaStreams.start()
    }

    @PreDestroy
    fun stopStreams(kafkaStreams: KafkaStreams, customerProducer: Producer<String, Customer>) {
        logger.info("*********** Closing streams ***********")
        customerProducer.close()
        kafkaStreams.close()
    }

    @Bean
    fun topology(): Topology {
        val customerSerde: Serde<Customer> = Serdes.serdeFrom(JsonPojoSerializer<Customer>(), JsonPojoDeserializer(Customer::class.java))
        val builder = StreamsBuilder()
        builder.table(CUSTOMERS_TOPIC,
                Consumed.with(Serdes.String(), customerSerde),
                Materialized.`as`<String, Customer, KeyValueStore<Bytes, ByteArray>>(CUSTOMERS_STORE)
                        .withKeySerde(Serdes.String())
                        .withValueSerde(customerSerde))
                .toStream().to("customers-processed", Produced.with(Serdes.String(), customerSerde))

        return builder.build()
    }

    @Bean
    fun kafkaStreams(): KafkaStreams {
        val props = Properties()
        props[StreamsConfig.APPLICATION_ID_CONFIG] = applicationName
        props[StreamsConfig.BOOTSTRAP_SERVERS_CONFIG] = kafkaBootstrapServers
        return KafkaStreams(topology(), props)
    }

    @Bean
    fun customerProducer(): Producer<String, Customer> {
        val props = Properties()
        props[ProducerConfig.BOOTSTRAP_SERVERS_CONFIG] = kafkaBootstrapServers
        props[ProducerConfig.CLIENT_ID_CONFIG] = "CustomerService"
        return KafkaProducer(props, StringSerializer(), JsonPojoSerializer<Customer>())
    }

}