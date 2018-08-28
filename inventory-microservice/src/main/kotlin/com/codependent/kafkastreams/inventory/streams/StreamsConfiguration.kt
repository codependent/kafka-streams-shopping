package com.codependent.kafkastreams.inventory.streams

import com.codependent.kafkastreams.inventory.dto.Product
import com.codependent.kafkastreams.inventory.dto.ProductType
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

const val INVENTORY_TOPIC = "inventory"
const val INVENTORY_STORE = "inventory-store"

@Configuration
class StreamsConfiguration(@Value("\${spring.application.name}") private val applicationName: String,
                           @Value("\${kafka.boostrap-servers}") private val kafkaBootstrapServers: String) {

    private val logger = LoggerFactory.getLogger(javaClass)

    @PostConstruct
    fun startStreams(kafkaStreams: KafkaStreams) {
        kafkaStreams.start()
    }

    @PreDestroy
    fun stopStreams(kafkaStreams: KafkaStreams, inventoryProducer: Producer<String, Product>) {
        logger.info("*********** Closing streams ***********")
        inventoryProducer.close()
        kafkaStreams.close()
    }

    @Bean
    fun topology(): Topology {
        val productSerde: Serde<Product> = Serdes.serdeFrom(JsonPojoSerializer<Product>(), JsonPojoDeserializer(Product::class.java))
        val builder = StreamsBuilder()
        builder.stream(INVENTORY_TOPIC, Consumed.with(Serdes.String(), productSerde))
                .mapValues { _, value ->
                    value
                }
                .groupByKey().aggregate({ Product("0", "", ProductType.ELECTRONICS, "", 0) },
                        { _, value, aggregate ->
                            if (value.units == -1) {
                                null
                            } else {
                                value.units = aggregate.units + value.units
                                value
                            }

                        },
                        Materialized.`as`<String, Product, KeyValueStore<Bytes, ByteArray>>(INVENTORY_STORE)
                                .withKeySerde(Serdes.String())
                                .withValueSerde(productSerde))
                .toStream().to("inventory-processed", Produced.with(Serdes.String(), productSerde))


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
    fun inventoryProducer(): Producer<String, Product> {
        val props = Properties()
        props[ProducerConfig.BOOTSTRAP_SERVERS_CONFIG] = kafkaBootstrapServers
        props[ProducerConfig.CLIENT_ID_CONFIG] = "InventoryService"
        return KafkaProducer(props, StringSerializer(), JsonPojoSerializer<Product>())
    }

}