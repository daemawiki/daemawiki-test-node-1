package com.daemawiki.testnode.node

import com.mongodb.client.model.changestream.OperationType
import org.bson.Document
import org.bson.types.ObjectId
import org.springframework.data.mongodb.core.ChangeStreamOptions
import org.springframework.data.mongodb.core.ReactiveMongoTemplate
import org.springframework.data.mongodb.core.aggregation.Aggregation
import org.springframework.data.mongodb.core.query.Criteria
import org.springframework.stereotype.Component
import reactor.core.publisher.Flux

@Component
class Subscriber(
    private val mongoTemplate: ReactiveMongoTemplate
) {

    fun execute(collectionType: String, _id: String): Flux<Any> {
        return mongoTemplate.changeStream(
            Collections.valueOf(collectionType).str,
            ChangeStreamOptions.builder()
                .filter(Aggregation.newAggregation(
                    Aggregation.match(
                        Criteria.where("operationType").`is`(OperationType.REPLACE)
                            .orOperator(
                                listOf(
                                    Criteria.where("fullDocument._id").`is`(_id),
                                    Criteria.where("documentKey._id").`is`(ObjectId(_id))
                                )
                            )
                    )
                ))
                .build(),
            Document::class.java
        ).mapNotNull { it.body }
    }

    internal enum class Collections(val str: String) {
        ARTICLE("articles")
    }
}
