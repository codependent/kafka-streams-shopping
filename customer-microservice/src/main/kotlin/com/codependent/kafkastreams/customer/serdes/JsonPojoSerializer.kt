package com.codependent.kafkastreams.customer.serdes

import org.apache.kafka.common.serialization.Serializer
import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.kafka.common.errors.SerializationException


class JsonPojoSerializer<T> : Serializer<T> {

    private val objectMapper = ObjectMapper()

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