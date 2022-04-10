@file:Suppress("UNUSED")
package com.clasicrando.kdbc

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.job
import javax.sql.DataSource
import org.apache.commons.dbcp2.BasicDataSource
import java.sql.Connection
import java.sql.PreparedStatement
import java.sql.ResultSet
import java.sql.SQLException
import kotlin.coroutines.CoroutineContext

/**
 * Utility class for working with a JDBC [DataSource] and coroutines. Stores the dataSource within a containing
 * [CoroutineScope], [scope], whose job is child of the parent context's job and points to the [IO][Dispatchers.IO]
 * dispatcher for launching tasks/coroutines. The parent scope is used for cancellation purposes so if the parent
 * context's job is cancelled then anything linked to this scope is also cancelled.
 */
class DbConnection(parentContext: CoroutineContext, dataSource: DataSource) {

    constructor(
        parentContext: CoroutineContext,
        driverClassName: String,
        url: String,
        username: String,
        password: String,
        autoCommit: Boolean = false,
    ): this(parentContext, BasicDataSource().apply {
        this.driverClassName = driverClassName
        this.url = url
        this.username = username
        this.password = password
        this.defaultAutoCommit = autoCommit
    })

    val scope = CoroutineScope(Job(parentContext.job) + Dispatchers.IO + CoroutineDataSource(dataSource))

    /**
     * Suspends to make a DB call from the [IO][Dispatchers.IO] dispatcher and returns a [List] of [QueryResult] data
     * class instances after performing the provided [sql] query. All [parameters] are bound as [PreparedStatement]
     * parameters after flattening any [Iterable]/[Array] params.
     *
     * The type [T] is used to look up a registered [ResultSetParser]. If the parser is found, then the query
     * is executed (with the provided params) and each row of the query is parsed into the [QueryResult] data
     * class as defined by the parser.
     *
     * @throws SQLException the query failed and/or the connection threw an error
     * @throws IllegalArgumentException [T] does not have a registered [ResultSetParser]
     */
    suspend inline fun <reified T> submitQuery(
        sql: String,
        vararg parameters: Any?,
    ): List<T> = withConnection(scope.coroutineContext) {
        coroutineContext.connection.submitQuery(sql, *parameters)
    }

    /**
     * Suspends to make a DB call from the [IO][Dispatchers.IO] dispatcher and returns the first result of the provided
     * [sql] query, transforming the row into the desired type
     *
     * Performs a similar operation to [submitQuery] but returns a single nullable object of type [T]. The result will be
     * non-null if the query returns at least 1 result or null if the query returns nothing.
     *
     * @throws SQLException when the query connection throws an exception
     */
    suspend inline fun <reified T> queryFirstOrNull(
        sql: String,
        vararg parameters: Any?,
    ): T? = withConnection(scope.coroutineContext) {
        coroutineContext.connection.queryFirstOrNull(sql, *parameters)
    }

    /**
     * Suspends to make a DB call from the [IO][Dispatchers.IO] dispatcher and returns a Boolean value denoting if the
     * [sql] query provided returns any rows in the [ResultSet]
     *
     * @throws SQLException if the statement preparation or execution throw an exception
     */
    suspend inline fun <reified T> queryHasResult(
        sql: String,
        vararg parameters: Any?,
    ): Boolean = withConnection(scope.coroutineContext) {
        coroutineContext.connection.queryHasResult(sql, *parameters)
    }

    /**
     * Shorthand for running a DML [sql] command using the provided [parameters]. Suspends to make a DB call from the
     * [IO][Dispatchers.IO] dispatcher and returns the affected rows count
     *
     * @throws SQLException if the statement preparation or execution throw an exception
     */
    suspend inline fun <reified T> runDml(
        sql: String,
        vararg parameters: Any?,
    ): Int = withConnection(scope.coroutineContext) {
        coroutineContext.connection.runDml(sql, *parameters)
    }

    /**
     * Shorthand for running DDL statements which do not return any counts. Suspends to make a DB call from the
     * [IO][Dispatchers.IO] dispatcher and uses the [sql] command and [parameters] to execute the statement
     *
     * @throws SQLException if the statement preparation or execution throw an exception
     */
    suspend inline fun <reified T> executeNoReturn(
        sql: String,
        vararg parameters: Any?,
    ): Unit = withConnection(scope.coroutineContext) {
        coroutineContext.connection.executeNoReturn(sql, *parameters)
    }

    /**
     * Suspends to make a DB call from the [IO][Dispatchers.IO] dispatcher and runs a batch DML using the [sql] DML
     * statement provided, treating each element of the [parameters][parameters] iterable as a batch used for the
     * statement. If the item type of [parameters] is not an [Iterable] then each item gets treated as a single item in
     * the batch.
     */
    suspend inline fun <reified T> runBatchDML(
        sql: String,
        parameters: Iterable<Any?>,
    ): Unit = withConnection(scope.coroutineContext) {
        coroutineContext.connection.runBatchDML(sql, parameters)
    }

    /**
     * Suspends to make a DB call from the [IO][Dispatchers.IO] dispatcher and runs the specified [block] within a db
     * transaction.
     * The [block] is not a suspending lambda and the receiver [Connection] is in manual commit mode so the code should
     * be treated as regular blocking JDBC code with no commits. At the end of the block, an implicit commit happens
     * and if an error is thrown during the block execution, the transaction is rolled back.
     */
    suspend inline fun <reified T> runAsTransaction(
        crossinline block: Connection.() -> T,
    ): T = withTransaction(scope.coroutineContext) {
        coroutineContext.connection.block()
    }

}
