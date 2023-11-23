package com.github.clasicrando

import com.github.clasicrando.common.pool.PoolOptions
import com.github.clasicrando.postgresql.PgConnectOptions
import com.github.clasicrando.postgresql.PostgresqlConnectionBuilder
import io.klogging.config.loggingConfiguration
import io.klogging.logger
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.supervisorScope
import kotlinx.datetime.DateTimePeriod
import kotlinx.datetime.TimeZone
import kotlinx.datetime.toLocalDateTime
import kotlin.coroutines.coroutineContext

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
    )
    val poolOptions = PoolOptions(maxConnections = 2)
    try {
        val pool = PostgresqlConnectionBuilder.createConnectionPool(
            poolOptions,
            configuration,
        )
        pool.useConnection {
            val result = it.sendPreparedStatement("select $1 int_field, $2 text_field", listOf(1, "Test"))
//            val result = it.sendQuery("select 1 int_field, 'Test' text_field")
            logger.info(result)
            for (row in result.rows) {
                logger.info(row)
            }
        }
    } catch (ex: Throwable) {
        println(ex)
    } finally {
        delay(2000)
    }
}
