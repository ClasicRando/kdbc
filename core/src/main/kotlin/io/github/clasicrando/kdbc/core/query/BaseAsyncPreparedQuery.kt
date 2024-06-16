package io.github.clasicrando.kdbc.core.query

import io.github.clasicrando.kdbc.core.connection.AsyncConnection

/**
 * Base implementation of a [AsyncPreparedQuery]. Extends [BaseAsyncQuery] so each vendor
 * will still override [vendorExecuteQuery] to send and execute a prepared statement.
 */
abstract class BaseAsyncPreparedQuery<C : AsyncConnection>(
    connection: C?,
    sql: String,
) : BaseAsyncQuery<C>(connection = connection, sql = sql), AsyncPreparedQuery {
    /** Internal list of parameters that are bound to this [AsyncPreparedQuery] */
    protected val innerParameters: MutableList<QueryParameter> = mutableListOf()

    override val parameters: List<QueryParameter> get() = innerParameters

    /**
     * Bind a next [parameter] to the [AsyncPreparedQuery]. This adds the parameter to the
     * internal list of parameters in the order the parameter exists in the query regardless of the
     * vendor specific method of linking parameter values to query parameters.
     *
     * Returns a reference to the [AsyncPreparedQuery] to allow for method chaining.
     */
    final override fun bind(parameter: QueryParameter): AsyncPreparedQuery {
        innerParameters += parameter
        return this
    }

    /** Clears all parameters previously bound to this [AsyncPreparedQuery]  */
    final override fun clearParameters() {
        innerParameters.clear()
    }
}