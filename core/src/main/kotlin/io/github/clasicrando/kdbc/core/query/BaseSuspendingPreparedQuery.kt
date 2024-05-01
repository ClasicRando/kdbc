package io.github.clasicrando.kdbc.core.query

import io.github.clasicrando.kdbc.core.connection.SuspendingConnection

/**
 * Base implementation of a [SuspendingPreparedQuery]. Extends [BaseSuspendingQuery] so each vendor
 * will still override [vendorExecuteQuery] to send and execute a prepared statement.
 */
abstract class BaseSuspendingPreparedQuery<C : SuspendingConnection>(
    connection: C?,
    sql: String,
) : BaseSuspendingQuery<C>(connection = connection, sql = sql), SuspendingPreparedQuery {
    /** Internal list of parameters that are bound to this [SuspendingPreparedQuery] */
    protected val innerParameters: MutableList<Any?> = mutableListOf()

    override val parameters: List<Any?> get() = innerParameters

    /**
     * Bind a next [parameter] to the [SuspendingPreparedQuery]. This adds the parameter to the
     * internal list of parameters in the order the parameter exists in the query regardless of the
     * vendor specific method of linking parameter values to query parameters.
     *
     * Returns a reference to the [SuspendingPreparedQuery] to allow for method chaining.
     */
    final override fun bind(parameter: Any?): SuspendingPreparedQuery {
        innerParameters += parameter
        return this
    }

    /**
     * Bind the [parameters] to the [SuspendingPreparedQuery]. This adds each parameter to the
     * internal list of parameters in the order the parameter exists in the query regardless of the
     * vendor specific method of linking parameter values to query parameters.
     *
     * Returns a reference to the [SuspendingPreparedQuery] to allow for method chaining.
     */
    final override fun bindMany(parameters: Collection<Any?>): SuspendingPreparedQuery {
        this.innerParameters.addAll(parameters)
        return this
    }

    /**
     * Bind the [parameters] to the [SuspendingPreparedQuery]. This adds each parameter to the
     * internal list of parameters in the order the parameter exists in the query regardless of the
     * vendor specific method of linking parameter values to query parameters.
     *
     * Returns a reference to the [SuspendingPreparedQuery] to allow for method chaining.
     */
    final override fun bindMany(vararg parameters: Any?): SuspendingPreparedQuery {
        this.innerParameters.addAll(parameters)
        return this
    }

    /** Clears all parameters previously bound to this [SuspendingPreparedQuery]  */
    final override fun clearParameters() {
        innerParameters.clear()
    }
}