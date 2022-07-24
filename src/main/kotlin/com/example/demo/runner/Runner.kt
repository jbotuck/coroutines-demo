package com.example.demo.runner

import com.example.demo.model.Widget
import org.slf4j.LoggerFactory
import org.springframework.boot.CommandLineRunner
import org.springframework.data.mongodb.core.MongoTemplate
import org.springframework.data.mongodb.core.query.Query
import org.springframework.data.mongodb.core.stream
import org.springframework.kafka.core.KafkaProducerException
import org.springframework.kafka.core.KafkaSendCallback
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.support.SendResult
import org.springframework.stereotype.Component
import java.util.concurrent.atomic.AtomicReference

@Component
class Runner(val mongoTemplate: MongoTemplate, val kafkaTemplate: KafkaTemplate<String, Any>) : CommandLineRunner {
    override fun run(vararg args: String?) {
        val query = Query()// some query that pulls a lot of results
        val firstException = AtomicReference<KafkaProducerException>(null)

        mongoTemplate.stream<Widget>(query).use { records ->
            for (widget in records) {
                firstException.get()?.let { throw it }


                //transformation logic might go here


                val listenableFuture = kafkaTemplate.sendDefault(widget)
                listenableFuture.addCallback(object : KafkaSendCallback<String, Any> {
                    override fun onSuccess(result: SendResult<String, Any>?) {
                        //do nothing. When record is written to kafka our job is done
                        logger.debug("successfully wrote a record to kafka")
                    }

                    override fun onFailure(ex: KafkaProducerException) {
                        //set the exception so the parent knows something went wrong
                        firstException.compareAndSet(null, ex)
                        logger.error("error writing to kafka", ex)
                    }
                })
            }
        }
        kafkaTemplate.flush()
        firstException.get()?.let { throw it }

    }

    companion object {
        val logger = LoggerFactory.getLogger(Runner::class.java)
    }
}