package com.github.clasicrando.common

import com.github.clasicrando.common.result.QueryResult

interface Executor {
    suspend fun sendQuery(query: String): QueryResult
    suspend fun sendPreparedStatement(
        query: String,
        parameters: List<Any?>,
        release: Boolean = false,
    ): QueryResult
}