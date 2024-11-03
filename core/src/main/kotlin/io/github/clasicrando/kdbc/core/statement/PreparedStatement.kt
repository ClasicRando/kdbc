package io.github.clasicrando.kdbc.core.statement

import kotlinx.datetime.Instant

/**
 * Required properties of a statement prepared by the data for repeated execution and supplied
 * parameters
 */
public interface PreparedStatement {
    /** Query backing this prepared statement */
    public val query: String

    /** Unique identifier for each statement to keep track of a prepared statement cache */
    public val statementId: Int

    /** Number of parameters required for the prepared statement */
    public val paramCount: Int

    /** Flag indicating if the statement has been prepared by the server */
    public var prepared: Boolean

    /**
     * Last time the prepared statement was executed. Used to find the best prepared statement to
     * remove from the cache when it reaches its capacity.
     */
    public var lastExecuted: Instant?
}
