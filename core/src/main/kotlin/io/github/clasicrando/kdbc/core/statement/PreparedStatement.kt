package io.github.clasicrando.kdbc.core.statement

import io.github.clasicrando.kdbc.core.column.ColumnMetadata

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
    /** Result column metadata */
    public val resultMetadata: List<ColumnMetadata>
}
