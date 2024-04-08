package com.github.kdbc.benchmarks.postgresql

import com.github.jasync.sql.db.pool.ConnectionPool
import com.github.jasync.sql.db.postgresql.PostgreSQLConnection
import com.github.jasync.sql.db.postgresql.PostgreSQLConnectionBuilder
import io.github.clasicrando.kdbc.core.LogSettings
import io.github.clasicrando.kdbc.core.connection.BlockingConnection
import io.github.clasicrando.kdbc.core.connection.Connection
import io.github.clasicrando.kdbc.core.pool.PoolOptions
import io.github.clasicrando.kdbc.postgresql.Postgres
import io.github.clasicrando.kdbc.postgresql.connection.PgConnectOptions
import io.github.oshai.kotlinlogging.Level
import kotlinx.uuid.UUID
import kotlinx.uuid.generateUUID
import org.apache.commons.dbcp2.DriverManagerConnectionFactory
import org.apache.commons.dbcp2.PoolableConnection
import org.apache.commons.dbcp2.PoolableConnectionFactory
import org.apache.commons.dbcp2.PoolingDataSource
import org.apache.commons.pool2.impl.GenericObjectPool
import java.sql.DriverManager
import java.sql.Connection as JdbcConnection

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

private const val missingLocalEnvironmentVariableMessage = "To run benchmarks the " +
        "environment variable JDBC_PG_CONNECTION_STRING must be available"

private val localConnectionString = System.getenv("JDBC_LOCAL_PG_CONNECTION_STRING")
    ?: throw IllegalStateException(missingEnvironmentVariableMessage)

fun getJdbcConnection(): JdbcConnection = DriverManager.getConnection(connectionString)

fun getJdbcDataSource(): PoolingDataSource<PoolableConnection> {
    val connectionFactory = DriverManagerConnectionFactory(connectionString, null)
    val poolableConnectionFactory = PoolableConnectionFactory(connectionFactory, null)
    val connectionPool = GenericObjectPool(poolableConnectionFactory)
    poolableConnectionFactory.pool = connectionPool
    return PoolingDataSource(connectionPool)
}

private val defaultLocalConnectOptions = PgConnectOptions(
    host = "127.0.0.1",
    port = 5432U,
    username = "postgres",
    password = System.getenv("PG_LOCAL_BENCHMARK_PASSWORD")
        ?: error("To run benchmarks the environment variable PG_LOCAL_BENCHMARK_PASSWORD must be available"),
    database = "test",
    applicationName = "KdbcTests",
    logSettings = LogSettings.DEFAULT.copy(statementLevel = Level.TRACE),
)

private val defaultConnectOptions = PgConnectOptions(
    host = "192.168.2.15",
    port = 5430U,
    username = "em_admin",
    password = System.getenv("PG_BENCHMARK_PASSWORD")
        ?: error("To run benchmarks the environment variable PG_BENCHMARK_PASSWORD must be available"),
    database = "enviro_manager",
    applicationName = "KdbcTests",
    logSettings = LogSettings.DEFAULT.copy(statementLevel = Level.TRACE),
)

suspend fun getKdbcConnection(): Connection {
    return Postgres.connection(connectOptions = defaultConnectOptions)
}

suspend fun initializeConcurrentConnections(): PgConnectOptions {
    val options = PgConnectOptions(
        host = "192.168.2.15",
        port = 5430U,
        username = "em_admin",
        password = System.getenv("PG_BENCHMARK_PASSWORD")
            ?: error("To run benchmarks the environment variable PG_BENCHMARK_PASSWORD must be available"),
        database = "enviro_manager",
        applicationName = "KdbcTests${UUID.generateUUID()}",
        logSettings = LogSettings.DEFAULT.copy(statementLevel = Level.TRACE),
        poolOptions = PoolOptions(
            maxConnections = 10,
            minConnections = 8,
        ),
    )
    Postgres.connection(connectOptions = options).close()
    return options
}

fun getKdbcBlockingConnection(): BlockingConnection {
    return Postgres.blockingConnection(connectOptions = defaultConnectOptions)
}

fun initializeThreadPoolBlockingConnections(): PgConnectOptions {
    val options = PgConnectOptions(
        host = "192.168.2.15",
        port = 5430U,
        username = "em_admin",
        password = System.getenv("PG_BENCHMARK_PASSWORD")
            ?: error("To run benchmarks the environment variable PG_BENCHMARK_PASSWORD must be available"),
        database = "enviro_manager",
        applicationName = "KdbcTests${UUID.generateUUID()}",
        logSettings = LogSettings.DEFAULT.copy(statementLevel = Level.TRACE),
        poolOptions = PoolOptions(
            maxConnections = 10,
            minConnections = 8,
        ),
    )
    Postgres.blockingConnection(connectOptions = options).close()
    return options
}

fun getJasyncPool(): ConnectionPool<PostgreSQLConnection> {
    return PostgreSQLConnectionBuilder.createConnectionPool {
        host = "192.168.0.12"
        port = 5430
        database = "enviro_manager"
        username = "em_admin"
        password = System.getenv("PG_BENCHMARK_PASSWORD")
            ?: error("To run benchmarks the environment variable PG_BENCHMARK_PASSWORD must be available")
    }
}

const val concurrencyLimit: Int = 100
