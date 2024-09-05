package com.daemawiki.testnode.mongo

import com.mongodb.client.model.changestream.OperationType
import org.springframework.data.mongodb.core.ChangeStreamOptions
import org.springframework.data.mongodb.core.aggregation.Aggregation
import org.springframework.data.mongodb.core.query.Criteria

class ChangeStreamBuilder {
    private var collectionName: String = ""
    private val filters = mutableListOf<Criteria>()
    private var operationType: OperationType? = null

    fun collection(name: String) = apply { collectionName = Collections.valueOf(name).str }
    fun filterBy(criteria: Criteria) = apply { filters.add(criteria) }
    fun operationType(type: OperationType) = apply { operationType = type }

    fun build(): ChangeStreamOptions {
        val matchCriteria = Criteria().apply {
            operationType?.let { and(Keys.operationType.name).`is`(it) }
            if (filters.isNotEmpty()) {
                orOperator(*filters.toTypedArray())
            }
        }

        return ChangeStreamOptions.builder()
            .filter(Aggregation.newAggregation(Aggregation.match(matchCriteria)))
            .build()
    }

    internal enum class Keys {
        operationType
    }

    internal enum class Collections(val str: String) {
        ARTICLE("articles")
    }
}