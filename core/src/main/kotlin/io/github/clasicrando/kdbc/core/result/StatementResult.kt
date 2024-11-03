package io.github.clasicrando.kdbc.core.result

/**
 * Type representing the zero or more [QueryResult] instances that can be returned from a single
 * database statement. For instance, a single simple query to a postgresql database can return
 * multiple [QueryResult]s that may be required by the querying user. To make these accessible, they
 * are packed into this type and the user can decide how to unwrap this into the results it needs.
 */
public class StatementResult(private var queryResults: List<QueryResult>) : Iterable<QueryResult> {
    /**
     * Return the number of [QueryResult]s that the database returned from the previously executed
     * statement.
     */
    public val size: Int
        get() = queryResults.size

    /**
     * Return the [QueryResult] backed by this [index].
     *
     * @throws IllegalArgumentException if the index does not point to an existing entry
     */
    public operator fun get(index: Int): QueryResult {
        require(index in 0..<size) {
            "Attempted to access QueryResult of invalid index. Must be 0..<$size but got $index"
        }
        return queryResults[index]
    }

    /** Return an [Iterator] over the [QueryResult]s returned. */
    override fun iterator(): Iterator<QueryResult> = queryResults.iterator()

    /**
     * Builder for a [StatementResult]. Stored a [MutableList] of [QueryResult]s, allowing additions
     * to that list using the [addQueryResult] method. After all [QueryResult]s have been collected,
     * [build] is called to clear this object and return a [StatementResult].
     */
    public class Builder {
        private var list = mutableListOf<QueryResult>()

        /** Add another [QueryResult] to this [StatementResult] builder */
        public fun addQueryResult(queryResult: QueryResult): Builder {
            list.add(queryResult)
            return this
        }

        /**
         * Finish collecting [QueryResult]s and move all collected [QueryResult]s into a new
         * [StatementResult]
         */
        public fun build(): StatementResult {
            val result = StatementResult(list)
            list = mutableListOf()
            return result
        }
    }
}
