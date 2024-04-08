package io.github.clasicrando.kdbc.core.result

import io.github.clasicrando.kdbc.core.AutoRelease

/**
 * Type representing the zero or more [QueryResult] instances that can be returned from a single
 * database statement. For instance, a single simple query to a postgresql database can return
 * multiple [QueryResult]s that may be required by the querying user. To make these accessible,
 * they are packed into this type and the user can decide how to unwrap this into the results it
 * needs.
 */
class StatementResult(
    private var queryResults: List<QueryResult>?,
) : Iterable<QueryResult>, AutoRelease {
    /**
     * Return the number of [QueryResult]s that the database returned from the previously executed
     * statement.
     *
     * @throws IllegalStateException if this method is called after [release] is called
     */
    val size: Int get() = queryResults?.size
        ?: error("Attempted to get size of a closed/released StatementResult")

    /**
     * Return the [QueryResult] backed by this [index].
     *
     * @throws IllegalArgumentException if the index does not point to an existing entry
     * @throws IllegalStateException if this method is called after [release]
     */
    operator fun get(index: Int): QueryResult {
        require(index in 0..<size) {
            "Attempted to access QueryResult of invalid index. Must be 0..<$size but got $index"
        }
        return queryResults?.get(index)
            ?: error("Attempted to get QueryResult of a closed/released StatementResult")
    }

    /**
     * Return an [Iterator] over the [QueryResult]s returned.
     *
     * @throws IllegalStateException if this method is called after [release]
     */
    override fun iterator(): Iterator<QueryResult> = queryResults?.iterator()
        ?: error("Attempted to iterate over a closed/released StatementResult")

    /**
     * Remove all [QueryResult]s of the backing [MutableList] of this result. If the list is still
     * populated, each [QueryResult] will be released as well.
     */
    override fun release() {
        queryResults?.let {
            for (queryResult in it) {
                queryResult.release()
            }
        }
        queryResults = null
    }

    /**
     * Builder for a [StatementResult]. Stored a [MutableList] of [QueryResult]s, allowing
     * additions to that list using the [addQueryResult] method. After all [QueryResult]s have been
     * collected, [build] is called to clear this object and return a [StatementResult].
     */
    class Builder {
        private var list = mutableListOf<QueryResult>()

        /** Add another [QueryResult] to this [StatementResult] builder */
        fun addQueryResult(queryResult: QueryResult): Builder {
            list.add(queryResult)
            return this
        }

        /**
         * Finish collecting [QueryResult]s and move all collected [QueryResult]s into a new
         * [StatementResult]
         */
        fun build(): StatementResult {
            val result = StatementResult(list)
            list = mutableListOf()
            return result
        }
    }
}
