package com.codependent.kafkastreams.inventory.serdes

import org.apache.kafka.common.serialization.Serializer
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.KotlinModule
import org.apache.kafka.common.errors.SerializationException


class JsonPojoSerializer<T> : Serializer<T> {

    private val objectMapper = ObjectMapper().registerModule(KotlinModule())

    override fun configure(p0: MutableMap<String, *>?, p1: Boolean) {}

    override fun serialize(topic: String?, data: T): ByteArray {
        try {
            return objectMapper.writeValueAsBytes(data)
        } catch (e: Exception) {
            throw SerializationException("Error serializing JSON message", e)
        }
    }

    override fun close() {
    }
}