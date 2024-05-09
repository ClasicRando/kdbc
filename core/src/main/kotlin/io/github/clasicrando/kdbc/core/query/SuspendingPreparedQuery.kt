package io.github.clasicrando.kdbc.core.query

/**
 * API extending [SuspendingQuery] to allow for executing a SQL query with zero or more parameters.
 *
 * This is the preferred option when executing queries since it's flexible to allow for parameters
 * within statements and also always caches the query plan for future execution of this exact same
 * SQL query.
 */
interface SuspendingPreparedQuery : PreparedQuery<SuspendingPreparedQuery>, SuspendingQuery
