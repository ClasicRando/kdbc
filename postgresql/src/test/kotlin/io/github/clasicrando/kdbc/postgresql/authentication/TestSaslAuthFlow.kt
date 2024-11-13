package io.github.clasicrando.kdbc.postgresql.authentication

import io.github.clasicrando.kdbc.core.query.execute
import io.github.clasicrando.kdbc.core.query.query
import io.github.clasicrando.kdbc.core.use
import io.github.clasicrando.kdbc.postgresql.PgConnectionHelper
import kotlin.test.Test
import kotlinx.coroutines.runBlocking

class TestSaslAuthFlow {
    @Test
    fun `saslAuthFlow should succeed when valid login`(): Unit = runBlocking {
        PgConnectionHelper.defaultConnection().use { query("SELECT 1").execute(it) }
    }

    @Test
    fun `saslAuthFlow should succeed when valid login with ssl`(): Unit = runBlocking {
        PgConnectionHelper.defaultConnectionSsl().use { query("SELECT 1").execute(it) }
    }
}
