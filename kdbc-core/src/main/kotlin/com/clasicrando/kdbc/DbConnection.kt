@file:Suppress("UNUSED")
package com.clasicrando.kdbc

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import javax.sql.DataSource
import org.apache.commons.dbcp2.BasicDataSource
import java.sql.PreparedStatement
import java.sql.ResultSet
import java.sql.SQLException

/** */
class DbConnection(private val dataSource: DataSource) {

    constructor(
        driverClassName: String,
        url: String,
        username: String,
        password: String,
        autoCommit: Boolean = false,
    ): this(BasicDataSource().apply {
        this.driverClassName = driverClassName
        this.url = url
        this.username = username
        this.password = password
        this.defaultAutoCommit = autoCommit
    })

    val scope = CoroutineScope(Dispatchers.IO + CoroutineDataSource(dataSource))

    /**
     * Returns a [List] of [QueryResult] data class instances after performing the provided [sql] query. All
     * [parameters] are bound as [PreparedStatement] parameters after flattening any [Iterable]/[Array] params.
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
    ): List<T> = withContext(scope.coroutineContext) {
        withConnection {
            coroutineContext.connection.submitQuery(sql, *parameters)
        }
    }

    /**
     * Returns the first result of the provided [sql] query, transforming the row into the desired type
     *
     * Performs a similar operation to [submitQuery] but returns a single nullable object of type [T]. The result will be
     * non-null if the query returns at least 1 result or null if the query returns nothing.
     *
     * @throws SQLException when the query connection throws an exception
     */
    suspend inline fun <reified T> queryFirstOrNull(
        sql: String,
        vararg parameters: Any?,
    ): T? = withContext(scope.coroutineContext) {
        withConnection {
            coroutineContext.connection.queryFirstOrNull(sql, *parameters)
        }
    }

    /**
     * Returns a Boolean value denoting if the [sql] query provided returns any rows in the [ResultSet]
     *
     * @throws SQLException if the statement preparation or execution throw an exception
     */
    suspend inline fun <reified T> queryHasResult(
        sql: String,
        vararg parameters: Any?,
    ): Boolean = withContext(scope.coroutineContext) {
        withConnection {
            coroutineContext.connection.queryHasResult(sql, *parameters)
        }
    }

    /**
     * Shorthand for running a DML [sql] command using the provided [parameters]. Returns the affected rows count
     *
     * @throws SQLException if the statement preparation or execution throw an exception
     */
    suspend inline fun <reified T> runDml(
        sql: String,
        vararg parameters: Any?,
    ): Int = withContext(scope.coroutineContext) {
        withConnection {
            coroutineContext.connection.runDml(sql, *parameters)
        }
    }

    /**
     * Shorthand for running DDL statements which do not return any counts. Uses the [sql] command and [parameters]
     * to execute the statement
     *
     * @throws SQLException if the statement preparation or execution throw an exception
     */
    suspend inline fun <reified T> executeNoReturn(
        sql: String,
        vararg parameters: Any?,
    ): Unit = withContext(scope.coroutineContext) {
        withConnection {
            coroutineContext.connection.executeNoReturn(sql, *parameters)
        }
    }

    /**
     * Runs a batch DML using the [sql] DML statement provided, treating each element of the [parameters][parameters]
     * iterable as a batch used for the statement. If the item type of [parameters] is not an [Iterable] then each item gets
     * treated as a single item in the batch.
     */
    suspend inline fun <reified T> runBatchDML(
        sql: String,
        parameters: Iterable<Any?>,
    ): Unit = withContext(scope.coroutineContext) {
        withConnection {
            coroutineContext.connection.runBatchDML(sql, parameters)
        }
    }

    /**
     * Returns a [List] of [QueryResult] data class instances after performing the provided [sql] query. All
     * [parameters] are bound as [PreparedStatement] parameters after flattening any [Iterable]/[Array] params.
     * This version makes the query call from within an open transaction or creates a new transaction
     *
     * The type [T] is used to look up a registered [ResultSetParser]. If the parser is found, then the query
     * is executed (with the provided params) and each row of the query is parsed into the [QueryResult] data
     * class as defined by the parser.
     *
     * @throws SQLException the query failed and/or the connection threw an error
     * @throws IllegalArgumentException [T] does not have a registered [ResultSetParser]
     */
    suspend inline fun <reified T> submitQueryTransaction(
        sql: String,
        vararg parameters: Any?,
    ): List<T> = withContext(scope.coroutineContext) {
        withTransaction {
            coroutineContext.connection.submitQuery(sql, *parameters)
        }
    }

    /**
     * Returns the first result of the provided [sql] query, transforming the row into the desired type. This version
     * makes the query call from within an open transaction or creates a new transaction
     *
     * Performs a similar operation to [submitQueryTransaction] but returns a single nullable object of type [T]. The
     * result will be non-null if the query returns at least 1 result or null if the query returns nothing.
     *
     * @throws SQLException when the query connection throws an exception
     */
    suspend inline fun <reified T> queryFirstOrNullTransaction(
        sql: String,
        vararg parameters: Any?,
    ): T? = withContext(scope.coroutineContext) {
        withTransaction {
            coroutineContext.connection.queryFirstOrNull(sql, *parameters)
        }
    }

    /**
     * Returns a Boolean value denoting if the [sql] query provided returns any rows in the [ResultSet]. This version
     * makes the query call from within an open transaction or creates a new transaction
     *
     * @throws SQLException if the statement preparation or execution throw an exception
     */
    suspend inline fun <reified T> queryHasResultTransaction(
        sql: String,
        vararg parameters: Any?,
    ): Boolean = withContext(scope.coroutineContext) {
        withTransaction {
            coroutineContext.connection.queryHasResult(sql, *parameters)
        }
    }

    /**
     * Shorthand for running a DML [sql] command using the provided [parameters]. Returns the affected rows count. This
     * version makes the query call from within an open transaction or creates a new transaction
     *
     * @throws SQLException if the statement preparation or execution throw an exception
     */
    suspend inline fun <reified T> runDmlTransaction(
        sql: String,
        vararg parameters: Any?,
    ): Int = withContext(scope.coroutineContext) {
        withTransaction {
            coroutineContext.connection.runDml(sql, *parameters)
        }
    }

    /**
     * Shorthand for running DDL statements which do not return any counts. Uses the [sql] command and [parameters]
     * to execute the statement. This version makes the query call from within an open transaction or creates a new
     * transaction
     *
     * @throws SQLException if the statement preparation or execution throw an exception
     */
    suspend inline fun <reified T> executeNoReturnTransaction(
        sql: String,
        vararg parameters: Any?,
    ): Unit = withContext(scope.coroutineContext) {
        withTransaction {
            coroutineContext.connection.executeNoReturn(sql, *parameters)
        }
    }

    /**
     * Runs a batch DML using the [sql] DML statement provided, treating each element of the [parameters][parameters]
     * iterable as a batch used for the statement. If the item type of [parameters] is not an [Iterable] then each item
     * gets treated as a single item in the batch. This version makes the query call from within an open transaction or
     * creates a new transaction
     *
     * @throws SQLException if the statement preparation or execution throw an exception
     */
    suspend inline fun <reified T> runBatchDMLTransaction(
        sql: String,
        parameters: Iterable<Any?>,
    ): Unit = withContext(scope.coroutineContext) {
        withTransaction {
            coroutineContext.connection.runBatchDML(sql, parameters)
        }
    }

}
