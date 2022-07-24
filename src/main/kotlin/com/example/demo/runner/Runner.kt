package com.example.demo.runner

import com.example.demo.model.Widget
import org.springframework.boot.CommandLineRunner
import org.springframework.data.mongodb.core.MongoTemplate
import org.springframework.data.mongodb.core.query.Query
import org.springframework.data.mongodb.core.stream
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.stereotype.Component

@Component
class Runner(val mongoTemplate: MongoTemplate, val kafkaTemplate: KafkaTemplate<String, Any>) : CommandLineRunner {
    override fun run(vararg args: String?) {
        val query = Query()// some query that pulls a lot of results

        for (widget in mongoTemplate.stream<Widget>(query)) {


            //transformation logic might go here


            kafkaTemplate.sendDefault(widget)
        }
    }
}