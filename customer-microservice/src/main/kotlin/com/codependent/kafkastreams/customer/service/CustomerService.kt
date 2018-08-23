package com.codependent.kafkastreams.customer.service

import org.apache.kafka.streams.KafkaStreams
import org.springframework.stereotype.Service
import javax.annotation.PostConstruct
import javax.annotation.PreDestroy

@Service
class CustomerService {

    private lateinit var streams: KafkaStreams

    @PostConstruct
    fun initializeStreams() {

    }

    @PreDestroy
    fun stopStreams() {

    }

}