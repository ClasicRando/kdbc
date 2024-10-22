package io.github.clasicrando.kdbc.core.query

/**
 * API to perform a single query against a database
 */
sealed interface Query {
    val sql: String

    data class Simple(
        override val sql: String,
    ) : Query

    class Prepared(
        override val sql: String,
    ) : Query {
        private val parametersInner: MutableList<QueryParameter> = mutableListOf()

        val parameters: List<QueryParameter> get() = parametersInner

        /**
         * Bind a next [parameter] to the [Query.Prepared]. This adds the parameter to the internal
         * list of parameters in the order the parameter exists in the query regardless of the
         * vendor specific method of linking parameter values to query parameters.
         *
         * Returns a reference to the same object to allow for method chaining.
         */
        fun bind(parameter: QueryParameter): Prepared {
            parametersInner.add(parameter)
            return this
        }

        /** Clears all parameters previously bound */
        fun clearParameters() = parametersInner.clear()

        override fun equals(other: Any?): Boolean {
            if (this === other) return true
            if (other !is Prepared) return false

            if (sql != other.sql) return false
            if (parametersInner != other.parametersInner) return false

            return true
        }

        override fun hashCode(): Int {
            var result = sql.hashCode()
            result = 31 * result + parametersInner.hashCode()
            return result
        }

        override fun toString(): String = "Query.Prepared(sql='$sql', parameters=$parametersInner)"
    }
}

fun query(sql: String): Query.Simple = Query.Simple(sql)

fun preparedQuery(sql: String): Query.Prepared = Query.Prepared(sql)

/**
 * Extension method to call [Query.Prepared.bind] and construct the [QueryParameter] using the
 * utility methods that construct the required type data implicitly.
 */
inline fun <reified T : Any> Query.Prepared.bind(parameter: T?): Query.Prepared =
    bind(QueryParameter(parameter))

/**
 * Extension method to call [Query.Prepared.bind] and construct the [QueryParameter] using the
 * utility methods that construct the required type data implicitly. Special case for a [List] of
 * nullable elements.
 */
@JvmName("QueryParameterNonNullItem")
inline fun <reified T : Any> Query.Prepared.bind(parameter: List<T?>): Query.Prepared =
    bind(QueryParameter(parameter))

/**
 * Extension method to call [Query.Prepared.bind] and construct the [QueryParameter] using the
 * utility methods that construct the required type data implicitly. Special case for a [List] of
 * non-null elements.
 */
inline fun <reified T : Any> Query.Prepared.bind(parameter: List<T>): Query.Prepared =
    bind(QueryParameter(parameter))
