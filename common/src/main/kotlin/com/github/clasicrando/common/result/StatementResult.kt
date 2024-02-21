package com.github.clasicrando.common.result

import com.github.clasicrando.common.AutoRelease

class StatementResult(
    private var queryResults: List<QueryResult>?,
) : Iterable<QueryResult>, AutoRelease {
    override fun iterator(): Iterator<QueryResult> = queryResults?.iterator()
        ?: error("Attempted to iterate over a closed/released StatementResult")

    override fun release() {
        queryResults = null
    }

    class Builder {
        private var list = mutableListOf<QueryResult>()
        fun addQueryResult(queryResult: QueryResult): Builder {
            list.add(queryResult)
            return this
        }

        fun build(): StatementResult {
            val result = StatementResult(list)
            list = mutableListOf()
            return result
        }
    }
}
