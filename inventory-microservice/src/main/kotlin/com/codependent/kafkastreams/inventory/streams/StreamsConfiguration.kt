package com.codependent.kafkastreams.inventory.streams

import com.codependent.kafkastreams.inventory.dto.Product
import com.codependent.kafkastreams.inventory.dto.ProductType
import com.codependent.kafkastreams.inventory.serdes.JsonPojoDeserializer
import com.codependent.kafkastreams.inventory.serdes.JsonPojoSerializer
import com.codependent.kafkastreams.inventory.service.INVENTORY_STORE
import com.codependent.kafkastreams.inventory.service.INVENTORY_TOPIC
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.common.utils.Bytes
import org.apache.kafka.streams.Consumed
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.Topology
import org.apache.kafka.streams.kstream.Materialized
import org.apache.kafka.streams.kstream.Produced
import org.apache.kafka.streams.state.KeyValueStore
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration

@Configuration
class StreamsConfiguration {

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

}