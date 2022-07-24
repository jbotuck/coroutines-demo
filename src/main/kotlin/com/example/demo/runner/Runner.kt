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
        val chunkSize = args.first()?.toInt() ?: 50
        val query = Query()// some query that pulls a lot of results

        mongoTemplate.stream<Widget>(query).use { records ->
            records
                .asSequence()
                .map {
                    //transformation logic might go here
                    it
                }
                .chunked(chunkSize)
                .forEach { records ->
                    val results = records.map {
                        kafkaTemplate.sendDefault(it)
                    }
                    kafkaTemplate.flush()
                    for (result in results) result.get()
                }
        }
    }
}