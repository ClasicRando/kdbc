package com.clasicrando.kdbc

import com.google.devtools.ksp.processing.CodeGenerator
import com.google.devtools.ksp.processing.Dependencies
import com.google.devtools.ksp.processing.KSPLogger
import com.google.devtools.ksp.processing.Resolver
import com.google.devtools.ksp.processing.SymbolProcessor
import com.google.devtools.ksp.processing.SymbolProcessorEnvironment
import com.google.devtools.ksp.processing.SymbolProcessorProvider
import com.google.devtools.ksp.symbol.KSAnnotated
import com.google.devtools.ksp.symbol.KSClassDeclaration
import com.google.devtools.ksp.validate

class ConnectionExtensionsProcessor (
    private val codeGenerator: CodeGenerator,
    private val logger: KSPLogger,
): SymbolProcessor {
    override fun process(resolver: Resolver): List<KSAnnotated> {
        val classNames = resolver
            .getSymbolsWithAnnotation(QueryResult::class.qualifiedName!!)
            .filter { it is KSClassDeclaration && it.validate() }
            .map {
                it as KSClassDeclaration
                it.simpleName.asString() to it.packageName.asString()
            }
        createExtensionsFile(classNames)
        return emptyList()
    }

    private fun createExtensionsFile(classNames: Sequence<Pair<String, String>>) {
        val file = try {
            codeGenerator.createNewFile(
                Dependencies(true),
                packageName = "com.clasicrando.kdbc",
                fileName = "ConnectionExtensions",
            )
        } catch (ex: FileAlreadyExistsException) {
            logger.info("ConnectionExtensions file already exists. Skipping creation")
            return
        }
        val registeredParsers = classNames.joinToString(
            separator = ",\n                ",
            prefix = "\n                ",
            postfix = "\n            "
        ) { (className, packageName) ->
            "${packageName}.$className::class to ${className}ResultSetParser"
        }.ifBlank { "" }
        file.appendText("""
            @file:Suppress("UNUSED")
            package com.clasicrando.kdbc
            
            import java.sql.Connection
            import kotlin.reflect.KClass
            import kotlin.reflect.typeOf
            import java.sql.ResultSet
            import java.sql.PreparedStatement
            import java.sql.SQLException
            
            val registeredResultParsers = mapOf<KClass<*>, ResultSetParser<*>>($registeredParsers)
            
            /**
             * Returns all items of the SQL Array as [T] if the type cast is possible
             *
             * @throws IllegalStateException - the type cast is not possible
             */
            inline fun <reified T> java.sql.Array.getList(): List<T> {
                val arrayTyped = array as Array<*>
                return arrayTyped.map {
                    when (it) {
                        is T -> it
                        else -> throw IllegalStateException("SQL array item must be of type ${'$'}{typeOf<T>()}")
                    }
                }
            }
            
            /** Returns all items of an SQL Array as [T]. If the underlining Array is null then null is returned */
            @JvmName("getListNullable")
            inline fun <reified T> java.sql.Array?.getList(): List<T>? {
                if (this == null) {
                    return null
                }
                return getList()
            }
            
            /** */
            @Suppress("UNCHECKED_CAST")
            inline fun <reified T> getParser(): ResultSetParser<T> {
                val parser = registeredResultParsers[T::class]
                    ?: error("The type (${'$'}{T::class.simpleName}) does not have a registered parser")
                return parser as ResultSetParser<T>
            }

            /**
             * Returns a [List] of [QueryResult] data class instances after performing the provided [sql] query. All
             * [params] are bound as [PreparedStatement] parameters after flattening any [Iterable]/[Array] params.
             *
             * The type [T] is used to look up a registered [ResultSetParser]. If the parser is found, then the query
             * is executed (with the provided params) and each row of the query is parsed into the [QueryResult] data
             * class as defined by the parser.
             *
             * @throws SQLException the query failed and/or the connection threw an error
             * @throws IllegalArgumentException [T] does not have a registered [ResultSetParser]
             */
            inline fun <reified T> Connection.submitQuery(sql: String, vararg params: Any?): List<T> {
                val parser = getParser<T>()
                return prepareStatement(sql).use { statement ->
                    for (parameter in flattenParameters(*params).withIndex()) {
                        statement.setObject(parameter.index + 1, parameter.value)
                    }
                    statement.executeQuery().use { rs ->
                        rs.collectRows(parser)
                    }
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
            inline fun <reified T> Connection.queryFirstOrNull(
                sql: String,
                vararg parameters: Any?,
            ): T? {
                val parser = getParser<T>()
                return prepareStatement(sql).use { statement ->
                    for (parameter in parameters.withIndex()) {
                        statement.setObject(parameter.index + 1, parameter.value)
                    }
                    statement.executeQuery().use { rs ->
                        if (rs.next()) {
                            parser.parse(rs)
                        } else {
                            null
                        }
                    }
                }
            }
            
            /**
             * Returns a Boolean value denoting if the [sql] query provided returns any rows in the [ResultSet]
             *
             * @throws SQLException if the statement preparation or execution throw an exception
             */
            fun Connection.queryHasResult(
                sql: String,
                vararg parameters: Any?,
            ): Boolean {
                return prepareStatement(sql).use { statement ->
                    for (parameter in parameters.withIndex()) {
                        statement.setObject(parameter.index + 1, parameter.value)
                    }
                    statement.executeQuery().use {
                        it.next()
                    }
                }
            }
            
            /**
             * Shorthand for running an update [sql] command using the provided [parameters]. Returns the affected count
             *
             * @throws SQLException if the statement preparation or execution throw an exception
             */
            fun Connection.runDml(
                sql: String,
                vararg parameters: Any?,
            ): Int {
                return prepareStatement(sql).use { statement ->
                    for (parameter in flattenParameters(*parameters).withIndex()) {
                        statement.setObject(parameter.index + 1, parameter.value)
                    }
                    statement.executeUpdate()
                }
            }
            
            /**
             * Shorthand for running DDL statements which do not return any counts. Uses the [sql] command and [parameters]
             * to execute the statement
             *
             * @throws SQLException if the statement preparation or execution throw an exception
             */
            fun Connection.executeNoReturn(
                sql: String,
                vararg parameters: Any?,
            ) {
                prepareStatement(sql).use { statement ->
                    for (parameter in parameters.withIndex()) {
                        statement.setObject(parameter.index + 1, parameter.value)
                    }
                    statement.execute()
                }
            }
            
            /**
             * Utility function to flatten a value into an [Iterable] of [IndexedValue]. If the value is not an [Iterable] then
             * the value is converted to an [IndexedValue] and returned as an [Iterable] with a single item
             */
            private fun getParams(param: Any?): Iterable<IndexedValue<Any?>> {
                return if (param is Iterable<*>) {
                    param.withIndex()
                }
                else {
                    listOf(IndexedValue(0, param))
                }
            }
            
            /**
             * Runs a batch DML using the [sql] DML statement provided, treating each element of the [parameters][parameters]
             * iterable as a batch used for the statement. If the item type of [parameters] is not an [Iterable] then each item
             * gets treated as a single item in the batch.
             *
             * @throws SQLException if the statement preparation or execution throw an exception
             */
            fun Connection.runBatchDML(
                sql: String,
                parameters: Iterable<Any?>,
            ): Int {
                return prepareStatement(sql).use { statement ->
                    for (params in parameters) {
                        for ((i, param) in getParams(params)) {
                            statement.setObject(i + 1, param)
                        }
                        statement.addBatch()
                    }
                    statement.executeBatch()
                }.sum()
            }
            
            /**
             * Returns all rows of a [ResultSet] as a transformed type [T] using the provided [ResultSetParser] as the
             * transformation mechanism
             */
            inline fun <reified T> ResultSet.collectRows(parser: ResultSetParser<T>): List<T> {
                require(!isClosed) { "ResultSet is closed" }
                require(row == 0) { "ResultSet has already been initialized" }
                return buildList {
                    while (next()) {
                        add(parser.parse(this@collectRows))
                    }
                }
            }
            
            /** Flattens an iterable by passing each item to the sequence builder */
            suspend fun SequenceScope<Any?>.extractParams(items: Iterable<*>) {
                for (item in items) {
                    yield(item)
                }
            }
            
            /** Flattens an iterable by passing each item to the sequence builder */
            suspend fun SequenceScope<Any?>.extractParams(items: Array<*>) {
                for (item in items) {
                    yield(item)
                }
            }
            
            /** Returns a lazy sequence of params with iterable items flattened to single items */
            fun flattenParameters(vararg params: Any?): Sequence<Any?> = sequence {
                for (param in params) {
                    when (param) {
                        is Iterable<*> -> extractParams(param)
                        is Array<*> -> extractParams(param)
                        else -> yield(param)
                    }
                }
            }
        """.trimIndent())
    }
}

class ConnectionExtensionsProcessorProvider : SymbolProcessorProvider {
    override fun create(
        environment: SymbolProcessorEnvironment
    ): SymbolProcessor {
        return ConnectionExtensionsProcessor(environment.codeGenerator, environment.logger)
    }
}
