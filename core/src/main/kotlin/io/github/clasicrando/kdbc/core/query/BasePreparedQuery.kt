package io.github.clasicrando.kdbc.core.query

import io.github.clasicrando.kdbc.core.connection.Connection

/**
 * Base implementation of a [PreparedQuery]. Extends [BaseQuery] so each vendor will still override
 * [vendorExecuteQuery] to send and execute a prepared statement.
 */
abstract class BasePreparedQuery<C : Connection>(
    connection: C?,
    sql: String,
) : BaseQuery<C>(connection = connection, sql = sql), PreparedQuery {
    /** Internal list of parameters that are bound to this [PreparedQuery] */
    protected val innerParameters: MutableList<Any?> = mutableListOf()

    override val parameters: List<Any?> get() = innerParameters

    /**
     * Bind a next [parameter] to the [PreparedQuery]. This adds the parameter to the internal list
     * of parameters in the order the parameter exists in the query regardless of the vendor
     * specific method of linking parameter values to query parameters.
     *
     * Returns a reference to the [PreparedQuery] to allow for method chaining.
     */
    final override fun bind(parameter: Any?): PreparedQuery {
        innerParameters += parameter
        return this
    }

    /**
     * Bind the [parameters] to the [PreparedQuery]. This adds each parameter to the internal list
     * of parameters in the order the parameter exists in the query regardless of the vendor
     * specific method of linking parameter values to query parameters.
     *
     * Returns a reference to the [PreparedQuery] to allow for method chaining.
     */
    final override fun bindMany(parameters: Collection<Any?>): PreparedQuery {
        this.innerParameters.addAll(parameters)
        return this
    }

    /**
     * Bind the [parameters] to the [PreparedQuery]. This adds each parameter to the internal list
     * of parameters in the order the parameter exists in the query regardless of the vendor
     * specific method of linking parameter values to query parameters.
     *
     * Returns a reference to the [PreparedQuery] to allow for method chaining.
     */
    final override fun bindMany(vararg parameters: Any?): PreparedQuery {
        this.innerParameters.addAll(parameters)
        return this
    }

    /** Clears all parameters previously bound to this [PreparedQuery]  */
    final override fun clearParameters() {
        innerParameters.clear()
    }
}