package io.github.clasicrando.kdbc.core.query

import io.github.clasicrando.kdbc.core.connection.BlockingConnection
import kotlin.reflect.KType

/**
 * Base implementation of a [BlockingPreparedQuery]. Extends [BaseBlockingQuery] so each vendor
 * will still override [vendorExecuteQuery] to send and execute a prepared statement.
 */
abstract class BaseBlockingPreparedQuery<C : BlockingConnection>(
    connection: C?,
    sql: String,
) : BaseBlockingQuery<C>(connection = connection, sql = sql), BlockingPreparedQuery {
    /** Internal list of parameters that are bound to this [BlockingPreparedQuery] */
    protected val innerParameters: MutableList<QueryParameter> = mutableListOf()

    override val parameters: List<QueryParameter> get() = innerParameters

    /**
     * Bind a next [parameter] to the [BlockingPreparedQuery]. This adds the parameter to the
     * internal list of parameters in the order the parameter exists in the query regardless of the
     * vendor specific method of linking parameter values to query parameters.
     *
     * Returns a reference to the [BlockingPreparedQuery] to allow for method chaining.
     */
    final override fun bind(parameter: QueryParameter): BlockingPreparedQuery {
        innerParameters += parameter
        return this
    }

    /** Clears all parameters previously bound to this [BlockingPreparedQuery]  */
    final override fun clearParameters() {
        innerParameters.clear()
    }
}