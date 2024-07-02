package io.github.clasicrando.kdbc.benchmarks.postgresql

import com.github.doyaaaaaken.kotlincsv.dsl.csvWriter
import io.github.clasicrando.kdbc.benchmarks.IOUtils
import io.github.clasicrando.kdbc.core.LogSettings
import io.github.clasicrando.kdbc.core.pool.PoolOptions
import io.github.clasicrando.kdbc.postgresql.Postgres
import io.github.clasicrando.kdbc.postgresql.connection.PgAsyncConnection
import io.github.clasicrando.kdbc.postgresql.connection.PgBlockingConnection
import io.github.clasicrando.kdbc.postgresql.connection.PgConnectOptions
import io.github.clasicrando.kdbc.postgresql.copy.CopyStatement
import io.github.oshai.kotlinlogging.Level
import kotlinx.datetime.Clock
import kotlinx.datetime.TimeZone
import kotlinx.datetime.toKotlinInstant
import kotlinx.datetime.toLocalDateTime
import kotlinx.io.asOutputStream
import kotlinx.io.buffered
import kotlinx.io.files.Path
import kotlinx.uuid.UUID
import kotlinx.uuid.generateUUID
import org.apache.commons.dbcp2.DriverManagerConnectionFactory
import org.apache.commons.dbcp2.PoolableConnection
import org.apache.commons.dbcp2.PoolableConnectionFactory
import org.apache.commons.dbcp2.PoolingDataSource
import org.apache.commons.pool2.impl.GenericObjectPool
import org.postgresql.jdbc.PgConnection
import java.sql.DriverManager
import java.sql.ResultSet

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

val copyOutSetupQuery = """
    DROP TABLE IF EXISTS public.copy_out_posts;
    CREATE TABLE public.copy_out_posts
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
    INSERT INTO public.copy_out_posts("text", creation_date, last_change_date)
    SELECT LPAD('', 2000, 'x'), current_timestamp, current_timestamp
    FROM generate_series(1, 5000) t;
""".trimIndent()

val copyInSetupQuery = """
    DROP TABLE IF EXISTS public.copy_in_posts;
    CREATE TABLE public.copy_in_posts
    (
        id int primary key,
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
""".trimIndent()

fun createBenchmarkCsv(outputPath: Path) {
    IOUtils.createFileIfNotExists(outputPath)
    IOUtils.sink(outputPath, append = false).buffered().use { sink ->
        csvWriter().open(sink.asOutputStream()) {
            for (i in 1..50000) {
                val currentTimestamp = Clock.System.now().toLocalDateTime(TimeZone.UTC)
                writeRow(i, "$i Value", currentTimestamp, currentTimestamp, null, null, null, null, null, null, null, null, null)
            }
        }
    }
}

fun createBenchmarkCsv(outputPath: java.nio.file.Path) {
    createBenchmarkCsv(Path(outputPath.toString()))
}

val kdbcCopyOut = CopyStatement.TableToCsv(schemaName = "public", tableName = "copy_out_posts")
const val jdbcCopyOut = "COPY public.copy_out_posts TO STDOUT WITH (FORMAT csv)"

val kdbcCopyIn = CopyStatement.TableFromCsv(schemaName = "public", tableName = "copy_in_posts")
const val jdbcCopyIn = "COPY public.copy_in_posts FROM STDIN WITH (FORMAT csv)"

private const val missingEnvironmentVariableMessage = "To run benchmarks the environment " +
        "variable JDBC_PG_CONNECTION_STRING must be available"

private val connectionString = System.getenv("JDBC_PG_CONNECTION_STRING")
    ?: throw IllegalStateException(missingEnvironmentVariableMessage)

fun getJdbcConnection(): PgConnection = DriverManager.getConnection(connectionString).unwrap(PgConnection::class.java)

fun getJdbcDataSource(): PoolingDataSource<PoolableConnection> {
    val connectionFactory = DriverManagerConnectionFactory(connectionString, null)
    val poolableConnectionFactory = PoolableConnectionFactory(connectionFactory, null)
    val connectionPool = GenericObjectPool(poolableConnectionFactory)
    poolableConnectionFactory.pool = connectionPool
    return PoolingDataSource(connectionPool)
}

val kdbcConnectOptions = PgConnectOptions(
    host = "127.0.0.1",
    port = 5432,
    username = "postgres",
    password = System.getenv("PG_BENCHMARK_PASSWORD")
        ?: error("To run benchmarks the environment variable PG_BENCHMARK_PASSWORD must be available"),
    database = "postgres",
    applicationName = "KdbcTests${UUID.generateUUID()}",
    logSettings = LogSettings.DEFAULT.copy(statementLevel = Level.TRACE),
)

val poolOptions = PoolOptions(
    maxConnections = 10,
    minConnections = 8,
)

suspend fun getKdbcAsyncConnection(): PgAsyncConnection {
    return Postgres.asyncConnection(connectOptions = kdbcConnectOptions)
}

fun getKdbcBlockingConnection(): PgBlockingConnection {
    return Postgres.blockingConnection(connectOptions = kdbcConnectOptions)
}

const val concurrencyLimit = 100

fun extractPostDataClassListFromResultSet(resultSet: ResultSet): List<PostDataClass> {
    val items = ArrayList<PostDataClass>()
    while (resultSet.next()) {
        val item = PostDataClass(
            resultSet.getInt(1),
            resultSet.getString(2),
            resultSet.getTimestamp(3).toInstant().toKotlinInstant(),
            resultSet.getTimestamp(4).toInstant().toKotlinInstant(),
            resultSet.getInt(5),
            resultSet.getInt(6),
            resultSet.getInt(7),
            resultSet.getInt(8),
            resultSet.getInt(9),
            resultSet.getInt(10),
            resultSet.getInt(11),
            resultSet.getInt(12),
            resultSet.getInt(13),
        )
        items.add(item)
    }
    return items
}
