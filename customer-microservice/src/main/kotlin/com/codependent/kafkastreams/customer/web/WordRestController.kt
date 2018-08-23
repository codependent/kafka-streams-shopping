package com.codependent.kafkastreams.customer.web

import com.codependent.kafkastreams.customer.service.WordService
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.PathVariable
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RestController

@RestController
@RequestMapping("/words")
class WordRestController(private val wordService: WordService) {

    @GetMapping("/{word}/count")
    fun getWordCount(@PathVariable word: String): Long? {
        return wordService.getWordCount(word)
    }
}