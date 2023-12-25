package com.github.clasicrando.postgresql.authentication

import com.github.clasicrando.postgresql.PgConnectOptions
import com.github.clasicrando.postgresql.PgConnectionImpl
import com.github.clasicrando.postgresql.pool.PgConnectionFactory
import kotlinx.coroutines.runBlocking
import kotlin.test.Test

class TestSaslAuthFlow {
    private val defaultConnectOptions = PgConnectOptions(
        host = "localhost",
        port = 5432U,
        username = "postgres",
        password = System.getenv("PG_TEST_PASSWORD"),
        applicationName = "TestSaslAuthFlow",
    )
    private fun factory(connectOptions: PgConnectOptions) = PgConnectionFactory(connectOptions)

    @Test
    fun `saslAuthFlow should succeed when valid login`(): Unit = runBlocking {
        var connection: PgConnectionImpl? = null
        try {
            connection = factory(defaultConnectOptions).create(this) as PgConnectionImpl
            connection.sendQuery("SELECT 1")
        } catch (ex: Throwable) {
            throw ex
        } finally {
            connection?.close()
        }
    }
}
