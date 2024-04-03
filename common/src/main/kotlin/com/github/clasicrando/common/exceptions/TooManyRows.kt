package com.github.clasicrando.common.exceptions

/**
 * [KdbcException] thrown when a query result is expected to be exactly 1 row but multiple rows are
 * found
 */
class TooManyRows(query: String)
    : KdbcException("Query returns more than exactly 1 row. Query:\n$query")
