package com.codependent.kafkastreams.inventory.service

import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.common.utils.Bytes
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.kstream.Materialized
import org.apache.kafka.streams.kstream.Produced
import org.apache.kafka.streams.state.KeyValueStore
import org.apache.kafka.streams.state.QueryableStoreTypes
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Service
import java.util.*
import javax.annotation.PostConstruct
import javax.annotation.PreDestroy

const val COUNTS_STORE = "counts-store"

@Service
class WordService(@Value("\${spring.application.name}") private val applicationName: String,
                  @Value("\${kafka.boostrap-servers}") private val kafkaBootstrapServers: String) {

    private lateinit var streams: KafkaStreams

    @PostConstruct
    fun initializeStreams() {
        val props = Properties()
        props[StreamsConfig.APPLICATION_ID_CONFIG] = applicationName
        props[StreamsConfig.BOOTSTRAP_SERVERS_CONFIG] = kafkaBootstrapServers
        props[StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG] = Serdes.String().javaClass
        props[StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG] = Serdes.String().javaClass

        val builder = StreamsBuilder()
        val textLines = builder.stream<String, String>("TextLinesTopic")
        val wordCounts = textLines
                .flatMapValues { textLine -> Arrays.asList(*textLine.toLowerCase().split("\\W+".toRegex()).dropLastWhile { it.isEmpty() }.toTypedArray()) }
                .groupBy { _, word -> word }
                .count(Materialized.`as`<String, Long, KeyValueStore<Bytes, ByteArray>>(COUNTS_STORE))
        wordCounts.toStream().to("WordsWithCountsTopic", Produced.with(Serdes.String(), Serdes.Long()))

        streams = KafkaStreams(builder.build(), props)
        streams.start()
    }

    @PreDestroy
    fun stopStreams() {
        streams.close()
    }

    fun getWordCount(word: String): Long? {
        val keyValueStore = streams.store(COUNTS_STORE, QueryableStoreTypes.keyValueStore<String, Long>())
        return keyValueStore.get(word)
    }

}
/*
fun main(args : Array<String>) {
    val wordService = WordService("main", "localhost:9092")
    wordService.initializeStreams()
    val wordCount = wordService.getWordCount("hello")
    println(wordCount)
}*/