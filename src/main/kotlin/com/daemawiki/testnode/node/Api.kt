package com.daemawiki.testnode.node

import com.daemawiki.testnode.annotation.Api
import org.bson.Document
import org.springframework.http.MediaType
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.PathVariable
import reactor.core.publisher.Flux

@Api
class Api(
    private val subscriber: Subscriber
) {
    data class Response(
        val document: Document?
    )

    @GetMapping(
        value = ["/{collectionType}/{_id}"],
        produces = [MediaType.TEXT_EVENT_STREAM_VALUE]
    )
    fun subscribe(
        @PathVariable collectionType: String,
        @PathVariable _id: String
    ): Flux<Response> = subscriber.execute(collectionType, _id)
        .map { Response(it) }
}