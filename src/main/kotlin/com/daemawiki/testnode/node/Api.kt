package com.daemawiki.testnode.node

import com.daemawiki.testnode.annotation.Api
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.PathVariable

@Api
class Api(
    private val subscriber: Subscriber
) {
    data class Response(
        val document: Any?
    )

    @GetMapping("/{collectionType}/{_id}")
    fun subscribe(
        @PathVariable collectionType: String,
        @PathVariable _id: String
    ) = subscriber.execute(collectionType, _id)
            .map { Response(it) }
}