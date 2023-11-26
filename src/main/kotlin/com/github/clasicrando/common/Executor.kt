package com.github.clasicrando.common

import com.github.clasicrando.common.result.QueryResult

interface Executor {
    suspend fun sendQuery(query: String): QueryResult
    suspend fun sendPreparedStatement(
        query: String,
        parameters: List<Any?>,
        release: Boolean = false,
    ): QueryResult
    suspend fun sendQueryCatching(query: String): Result<QueryResult> {
        return runCatching {
            sendQuery(query)
        }
    }
    suspend fun sendPreparedStatementCatching(
        query: String,
        parameters: List<Any?>,
        release: Boolean = false,
    ): Result<QueryResult> {
        return runCatching {
            sendPreparedStatement(query, parameters, release)
        }
    }
}