package io.github.clasicrando.kdbc.core.query

/**
 * Extension interface for queries that include parameters (usually called prepared statements by
 * database vendors). This does not explicitly dictate query execution behaviour but is always
 * implemented for classes/interfaces that already have query execution behaviour.
 */
interface PreparedQuery : Query {
    /**
     * Read-only [List] of the parameters already bound to this statement. The order in the [List]
     * is the order that the parameters have been bound.
     */
    val parameters: List<QueryParameter>
    /**
     * Bind a next [parameter] to the [PreparedQuery]. This adds the parameter to the internal
     * list of parameters in the order the parameter exists in the query regardless of the vendor
     * specific method of linking parameter values to query parameters.
     *
     * Returns a reference to the same object to allow for method chaining.
     */
    fun bind(parameter: QueryParameter): PreparedQuery

    /** Clears all parameters previously bound to this [PreparedQuery] */
    fun clearParameters()
}
