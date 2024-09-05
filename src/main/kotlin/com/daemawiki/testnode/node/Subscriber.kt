package com.daemawiki.testnode.node

import com.daemawiki.testnode.mongo.ChangeStreamBuilder
import com.mongodb.client.model.changestream.OperationType
import org.bson.Document
import org.bson.types.ObjectId
import org.springframework.data.mongodb.core.ReactiveMongoTemplate
import org.springframework.data.mongodb.core.query.Criteria
import org.springframework.stereotype.Component
import reactor.core.publisher.Flux

@Component
class Subscriber(
    private val mongoTemplate: ReactiveMongoTemplate
) {

    fun execute(collectionType: String, _id: String): Flux<Document> {
        return mongoTemplate.changeStream {
            collection(collectionType)
            operationType(OperationType.REPLACE)
            filterBy(Criteria.where("fullDocument._id").`is`(_id))
            filterBy(Criteria.where("documentKey._id").`is`(ObjectId(_id)))
        }
    }
    
}

internal fun ReactiveMongoTemplate.changeStream(init: ChangeStreamBuilder.() -> Unit): Flux<Document> {
    val builder = ChangeStreamBuilder()
        .apply(init)

    return changeStream(builder.build(), Document::class.java)
        .mapNotNull { it.body }
}