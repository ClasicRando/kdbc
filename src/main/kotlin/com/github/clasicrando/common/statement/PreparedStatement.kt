package com.github.clasicrando.common.statement

import kotlinx.uuid.UUID

/**
 * Required properties of a statement prepared by the data for repeated execution and supplied
 * parameters
 */
interface PreparedStatement {
    /** Query backing this prepared statement */
    val query: String
    /** Unique identifier for each statement to keep track of a prepared statement cache */
    val statementId: UUID
    /** Number of parameters required for the prepared statement */
    val paramCount: Int
    /** Flag indicating if the statement has been prepared by the server */
    var prepared: Boolean
}
