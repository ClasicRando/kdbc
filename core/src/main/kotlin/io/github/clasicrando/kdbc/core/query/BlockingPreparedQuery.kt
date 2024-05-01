package io.github.clasicrando.kdbc.core.query

/**
 * API extending [BlockingQuery] to allow for executing a SQL query with zero or more parameters.
 *
 * This is the preferred option when executing queries since it's flexible to allow for parameters
 * within statements and also always caches the query plan for future execution of this exact same
 * SQL query.
 */
interface BlockingPreparedQuery : BlockingQuery {
    /**
     * Read-only [List] of the parameters already bound to this statement. The order in the [List]
     * is the order that the parameters have been bound.
     */
    val parameters: List<Any?>
    /**
     * Bind a next [parameter] to the [BlockingQuery]. This adds the parameter to the internal list
     * of parameters in the order the parameter exists in the query regardless of the vendor
     * specific method of linking parameter values to query parameters.
     *
     * Returns a reference to the [BlockingQuery] to allow for method chaining.
     */
    fun bind(parameter: Any?): BlockingPreparedQuery

    /**
     * Bind the [parameters] to the [BlockingQuery]. This adds each parameter to the internal list
     * of parameters in the order the parameter exists in the query regardless of the vendor
     * specific method of linking parameter values to query parameters.
     *
     * Returns a reference to the [BlockingQuery] to allow for method chaining.
     */
    fun bindMany(parameters: Collection<Any?>): BlockingPreparedQuery

    /**
     * Bind the [parameters] to the [BlockingQuery]. This adds each parameter to the internal list
     * of parameters in the order the parameter exists in the query regardless of the vendor
     * specific method of linking parameter values to query parameters.
     *
     * Returns a reference to the [BlockingQuery] to allow for method chaining.
     */
    fun bindMany(vararg parameters: Any?): BlockingPreparedQuery

    /** Clears all parameters previously bound to this [BlockingQuery] */
    fun clearParameters()
}