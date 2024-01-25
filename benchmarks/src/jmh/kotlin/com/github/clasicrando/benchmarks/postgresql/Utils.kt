package com.github.clasicrando.benchmarks.postgresql

import com.github.clasicrando.common.LogSettings
import com.github.clasicrando.common.connection.Connection
import com.github.clasicrando.postgresql.connection.PgConnectOptions
import com.github.clasicrando.postgresql.connection.PgConnection
import io.github.oshai.kotlinlogging.Level
import java.sql.Connection as JdbcConnection
import java.sql.DriverManager

val jdbcQuerySingle = """
    SELECT
        id, "text", creation_date, last_change_date, counter1, counter2, counter3, counter4,
        counter5, counter6, counter7, counter8, counter9
    FROM public.posts
    WHERE id = ?
""".trimIndent()

val kdbcQuerySingle = """
    SELECT
        id, "text", creation_date, last_change_date, counter1, counter2, counter3, counter4,
        counter5, counter6, counter7, counter8, counter9
    FROM public.posts
    WHERE id = $1
""".trimIndent()

val jdbcQuery = """
    SELECT
        id, "text", creation_date, last_change_date, counter1, counter2, counter3, counter4,
        counter5, counter6, counter7, counter8, counter9
    FROM public.posts
    WHERE id BETWEEN ? AND ?
""".trimIndent()

val kdbcQuery = """
    SELECT
        id, "text", creation_date, last_change_date, counter1, counter2, counter3, counter4,
        counter5, counter6, counter7, counter8, counter9
    FROM public.posts
    WHERE id BETWEEN $1 AND $2
""".trimIndent()

val setupQuery = """
    DROP TABLE IF EXISTS public.posts;
    CREATE TABLE public.posts
    (
        id int generated always as identity primary key,
        "text" text not null,
        creation_date timestamp not null,
        last_change_date timestamp not null,
        counter1 int,
        counter2 int,
        counter3 int,
        counter4 int,
        counter5 int,
        counter6 int,
        counter7 int,
        counter8 int,
        counter9 int
    );
    INSERT INTO public.posts("text", creation_date, last_change_date)
    SELECT LPAD('', 2000, 'x'), current_timestamp, current_timestamp
    FROM generate_series(1, 5000) t;
""".trimIndent()

private const val missingEnvironmentVariableMessage = "To run benchmarks the environment " +
        "variable JDBC_PG_CONNECTION_STRING must be available"

private val connectionString = System.getenv("JDBC_PG_CONNECTION_STRING")
    ?: throw IllegalStateException(missingEnvironmentVariableMessage)

fun getJdbcConnection(): JdbcConnection = DriverManager.getConnection(connectionString)

private val defaultConnectOptions = PgConnectOptions(
    host = "localhost",
    port = 5432U,
    username = "postgres",
    password = System.getenv("PG_BENCHMARK_PASSWORD")
        ?: error("To run benchmarks the environment variable PG_BENCHMARK_PASSWORD must be available"),
    applicationName = "KdbcTests",
    logSettings = LogSettings.DEFAULT.copy(statementLevel = Level.TRACE),
)

suspend fun getKdbcConnection(): Connection {
    return PgConnection.connect(connectOptions = defaultConnectOptions)
}
