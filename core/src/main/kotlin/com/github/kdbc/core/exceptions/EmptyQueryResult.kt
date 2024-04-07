package com.github.kdbc.core.exceptions

/**
 * [KdbcException] thrown when a query result was expected to contain 1 or more rows, but zero were
 * found
 */
class EmptyQueryResult(query: String)
    : KdbcException("No results found in query result for:\n$query")