package com.github.clasicrando.common.exceptions

/** [KdbcException] thrown when a query is executed but no query result instances were collected */
class NoResultFound(query: String)
    : KdbcException("Could not find any results executing:\n$query")