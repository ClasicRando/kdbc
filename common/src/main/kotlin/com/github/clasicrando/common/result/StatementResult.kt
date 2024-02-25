package com.github.clasicrando.common.result

import com.github.clasicrando.common.AutoRelease

class StatementResult(
    private var queryResults: List<QueryResult>?,
) : Iterable<QueryResult>, AutoRelease {
    val size: Int = queryResults?.size
        ?: error("Attempted to get size of a closed/released StatementResult")

    operator fun get(index: Int): QueryResult {
        require(index in 0..<size) {
            "Attempted to access QueryResult of invalid index. Must be 0..<$size but got $index"
        }
        return queryResults?.get(index)
            ?: error("Attempted to get QueryResult of a closed/released StatementResult")
    }

    override fun iterator(): Iterator<QueryResult> = queryResults?.iterator()
        ?: error("Attempted to iterate over a closed/released StatementResult")

    override fun release() {
        queryResults?.let {
            for (queryResult in it) {
                queryResult.release()
            }
        }
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
