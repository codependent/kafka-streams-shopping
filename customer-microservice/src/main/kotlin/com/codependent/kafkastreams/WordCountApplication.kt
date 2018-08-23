package com.codependent.kafkastreams

import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.common.utils.Bytes
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.kstream.Materialized
import org.apache.kafka.streams.kstream.Produced
import org.apache.kafka.streams.state.KeyValueStore
import java.util.*


class WordCountApplication {

    fun start() {
        val props = Properties()
        props[StreamsConfig.APPLICATION_ID_CONFIG] = "wordcount-application"
        props[StreamsConfig.BOOTSTRAP_SERVERS_CONFIG] = "localhost:9092"
        props[StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG] = Serdes.String().javaClass
        props[StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG] = Serdes.String().javaClass

        val builder = StreamsBuilder()
        val textLines = builder.stream<String, String>("TextLinesTopic")
        val wordCounts = textLines
                .flatMapValues { textLine -> Arrays.asList(*textLine.toLowerCase().split("\\W+".toRegex()).dropLastWhile { it.isEmpty() }.toTypedArray()) }
                .groupBy { _, word -> word }
                .count(Materialized.`as`<String, Long, KeyValueStore<Bytes, ByteArray>>("counts-store"))
        wordCounts.toStream().to("WordsWithCountsTopic", Produced.with(Serdes.String(), Serdes.Long()))

        val streams = KafkaStreams(builder.build(), props)
        streams.start()
    }

}



