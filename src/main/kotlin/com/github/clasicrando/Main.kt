package com.github.clasicrando

import com.github.clasicrando.common.SslMode
import com.github.clasicrando.common.pool.PoolOptions
import com.github.clasicrando.postgresql.PgConnectOptions
import com.github.clasicrando.postgresql.PgConnection
import com.github.clasicrando.postgresql.PostgresqlConnectionBuilder
import io.klogging.Level
import io.klogging.config.loggingConfiguration
import io.klogging.logger
import kotlinx.coroutines.delay

val logger = logger("MainKt")

suspend fun main() {
    loggingConfiguration(append = true) {}
    val configuration = PgConnectOptions(
        host = "10.0.0.218",
        port = 5430U,
        username = "em_admin",
        password = "password",
        database = "enviro_manager",
        connectionTimeout = 5000U,
        applicationName = "Test",
        options = null,
        sslMode = SslMode.Prefer,
    ).logStatements(Level.INFO) as PgConnectOptions
    val poolOptions = PoolOptions(maxConnections = 2)
    try {
        val pool = PostgresqlConnectionBuilder.createConnectionPool(
            poolOptions,
            configuration,
        )
        pool.useConnection {
            it.sendQuery("drop table if exists test_table")
            it.sendQuery("create table if not exists test_table(id int primary key, text_field text not null)")
            it as PgConnection
            val result = it.copyIn(
                "COPY test_table FROM STDIN WITH (FORMAT csv)",
            ) {
                for (i in 1..100) {
                    emit("$i,\"test $i\"\n".toByteArray())
                }
            }
            logger.info("{result}", result)
            it.copyOutAsFlow(
                "COPY test_table TO STDOUT WITH (FORMAT csv)",
            ).collect { bytes ->
                logger.info(bytes.decodeToString().trim())
            }
        }
    } catch (ex: Throwable) {
        logger.error(ex)
    } finally {
        delay(2000)
    }
}
