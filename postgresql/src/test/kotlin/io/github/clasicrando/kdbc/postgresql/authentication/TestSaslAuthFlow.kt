package io.github.clasicrando.kdbc.postgresql.authentication

import io.github.clasicrando.kdbc.core.pool.PoolOptions
import io.github.clasicrando.kdbc.core.pool.useConnection
import io.github.clasicrando.kdbc.core.use
import io.github.clasicrando.kdbc.postgresql.PgConnectionHelper
import io.github.clasicrando.kdbc.postgresql.pool.PgConnectionPool
import kotlin.test.Test
import kotlinx.coroutines.runBlocking

class TestSaslAuthFlow {
    @Test
    fun `saslAuthFlow should succeed when valid login`(): Unit = runBlocking {
        val pool = PgConnectionPool(PgConnectionHelper.defaultConnectOptions, PoolOptions())
        pool.useConnection { it.sendSimpleQuery("SELECT 1") }
        pool.close()
    }

    @Test
    fun `saslAuthFlow should succeed when valid login with ssl`(): Unit = runBlocking {
        PgConnectionHelper.defaultConnectionSsl().use { it.sendSimpleQuery("SELECT 1") }
    }
}
