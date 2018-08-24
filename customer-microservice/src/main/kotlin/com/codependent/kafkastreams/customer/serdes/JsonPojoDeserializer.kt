package com.codependent.kafkastreams.customer.serdes

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.kafka.common.errors.SerializationException
import org.apache.kafka.common.serialization.Deserializer


class JsonPojoDeserializer<T>(private val clazz: Class<T>) : Deserializer<T> {

    private val objectMapper = ObjectMapper()

    override fun configure(props: MutableMap<String, *>, isKey: Boolean) {}

    override fun deserialize(topic: String, bytes: ByteArray): T? {
        val data: T
        try {
            data = objectMapper.readValue(bytes, clazz)
        } catch (e: Exception) {
            throw SerializationException(e)
        }
        return data
    }

    override fun close() {
    }

}