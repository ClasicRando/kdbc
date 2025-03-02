package io.github.clasicrando.kdbc.benchmarks.mysql

import com.github.doyaaaaaken.kotlincsv.dsl.csvWriter
import com.mysql.cj.jdbc.JdbcConnection
import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import io.github.clasicrando.kdbc.benchmarks.IOUtils
import io.github.clasicrando.kdbc.benchmarks.PostDataClass
import io.github.clasicrando.kdbc.core.SslMode
import io.github.clasicrando.kdbc.core.pool.PoolOptions
import io.github.clasicrando.kdbc.core.stream.SocketOptions
import io.github.clasicrando.kdbc.mysql.MySql
import io.github.clasicrando.kdbc.mysql.connection.MySqlConnection
import io.github.clasicrando.kdbc.mysql.connection.MySqlConnectionOptions
import io.github.clasicrando.kdbc.postgresql.copy.CopyStatement
import io.github.oshai.kotlinlogging.Level
import java.sql.DriverManager
import java.sql.ResultSet
import java.time.LocalDateTime
import kotlin.time.DurationUnit
import kotlin.time.toDuration
import kotlin.uuid.Uuid
import kotlinx.io.asOutputStream
import kotlinx.io.buffered
import kotlinx.io.files.Path

val querySingle =
    """
    SELECT
        id, `text`, creation_date, last_change_date, counter1, counter2, counter3, counter4,
        counter5, counter6, counter7, counter8, counter9
    FROM posts
    WHERE id = ?
    """
        .trimIndent()

val query =
    """
    SELECT
        id, `text`, creation_date, last_change_date, counter1, counter2, counter3, counter4,
        counter5, counter6, counter7, counter8, counter9
    FROM posts
    WHERE id BETWEEN ? AND ?
    """
        .trimIndent()

val setupQueries =
    listOf<String>(
        "SET @@cte_max_recursion_depth = 6000;",
        "DROP TABLE IF EXISTS posts;",
        """
    CREATE TABLE posts
    (
        id int auto_increment,
        `text` text not null,
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
        counter9 int,
        primary key(id)
    );
    """
            .trimIndent(),
        """
    INSERT INTO posts(`text`, creation_date, last_change_date)
    WITH RECURSIVE seq(value) AS (
        SELECT 1 AS value
        UNION ALL
        SELECT value + 1
        FROM seq
        WHERE value < 5000
    )
    SELECT LPAD('', 2000, 'x'), current_timestamp, current_timestamp
    FROM seq t;
    """
            .trimIndent(),
    )

val setupQuery = setupQueries.joinToString(separator = "\n")

val copyOutSetupQuery =
    """
    DROP TABLE IF EXISTS copy_out_posts;
    CREATE TABLE copy_out_posts
    (
        id int generated always as identity primary key,
        `text` text not null,
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
    INSERT INTO copy_out_posts(`text`, creation_date, last_change_date)
    SELECT LPAD('', 2000, 'x'), current_timestamp, current_timestamp
    FROM generate_series(1, 5000) t;
    """
        .trimIndent()

fun createBenchmarkCsv(outputPath: Path) {
    IOUtils.createFileIfNotExists(outputPath)
    IOUtils.sink(outputPath, append = false).buffered().use { sink ->
        csvWriter().open(sink.asOutputStream()) {
            for (i in 1..50000) {
                val currentTimestamp = LocalDateTime.now()
                writeRow(
                    i,
                    "$i Value",
                    currentTimestamp,
                    currentTimestamp,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                )
            }
        }
    }
}

fun createBenchmarkCsv(outputPath: java.nio.file.Path) {
    createBenchmarkCsv(Path(outputPath.toString()))
}

val kdbcCopyOut = CopyStatement.TableToCsv(schemaName = "public", tableName = "copy_out_posts")
const val JDBC_COPY_OUT = "COPY public.copy_out_posts TO STDOUT WITH (FORMAT csv)"

val kdbcCopyIn = CopyStatement.TableFromCsv(schemaName = "public", tableName = "copy_in_posts")
const val JDBC_COPY_IN = "COPY public.copy_in_posts FROM STDIN WITH (FORMAT csv)"

private val connectionString =
    System.getenv("JDBC_MYSQL_CONNECTION_STRING")
        ?: error(
            "To run benchmarks the environment variable JDBC_MYSQL_CONNECTION_STRING must be available"
        )

fun getJdbcConnection(): JdbcConnection =
    DriverManager.getConnection(connectionString).unwrap(JdbcConnection::class.java)

fun getJdbcDataSource(): HikariDataSource {
    val config = HikariConfig().apply { jdbcUrl = connectionString }
    return HikariDataSource(config)
}

val kdbcConnectOptions =
    MySqlConnectionOptions(
        host = "127.0.0.1",
        port = 3306,
        username = "root",
        password =
            System.getenv("MYSQL_BENCHMARK_PASSWORD")
                ?: error(
                    "To run benchmarks the environment variable MYSQL_BENCHMARK_PASSWORD must be available"
                ),
        database = "test",
        applicationName = "KdbcTests${Uuid.random()}",
        statementLogLevel = Level.TRACE,
        socketOptions = SocketOptions(socketTimeout = 10.toDuration(DurationUnit.SECONDS)),
        sslMode = SslMode.Disable,
    )

val poolOptions =
    PoolOptions(maxConnections = 10, acquireTimeout = 10.toDuration(DurationUnit.SECONDS))

suspend fun getKdbcAsyncConnection(): MySqlConnection =
    MySql.connection(connectOptions = kdbcConnectOptions)

const val CONCURRENCY_LIMIT = 100

fun extractPostDataClassListFromResultSet(resultSet: ResultSet): List<PostDataClass> {
    val items = ArrayList<PostDataClass>()
    while (resultSet.next()) {
        val item =
            PostDataClass(
                resultSet.getInt(1),
                resultSet.getString(2),
                resultSet.getObject(3, LocalDateTime::class.java),
                resultSet.getObject(4, LocalDateTime::class.java),
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
