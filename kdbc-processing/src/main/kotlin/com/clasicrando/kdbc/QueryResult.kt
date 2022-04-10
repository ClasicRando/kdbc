package com.clasicrando.kdbc

/**
 * Symbol used to mark a data class as representing a SQL query result. This means that the KSP plugin will create a
 * parser object that is registered to the annotating class, allowing for easy [ResultSet][java.sql.ResultSet] parsing
 * with declared types (avoiding [getObject][java.sql.ResultSet.getObject]). Currently this just supports data classes
 * that contain simple types (standard data types and enums) so if a data class' constructor contains more complex
 * types, then the KSP build task will fail.
 *
 * It should also be noted that this does not verify that a [ResultSet][java.sql.ResultSet] matches the data class
 * definition. This means errors can be thrown at runtime if a specified field does not match the typing prescribed by
 * the data class. However, the object created does check to ensure a query's [ResultSet][java.sql.ResultSet] has the
 * same number of columns as the data class constructor. The user should employ unit tests to verify each [QueryResult]
 * annotated class can handle the queries to be run during regular application operation.
 */
@Target(AnnotationTarget.CLASS)
annotation class QueryResult
