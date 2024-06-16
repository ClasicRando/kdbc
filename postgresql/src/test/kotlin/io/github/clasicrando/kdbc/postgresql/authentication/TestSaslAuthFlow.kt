package io.github.clasicrando.kdbc.postgresql.authentication

import io.github.clasicrando.kdbc.core.use
import io.github.clasicrando.kdbc.postgresql.PgConnectionHelper
import kotlinx.coroutines.runBlocking
import kotlin.test.Test

class TestSaslAuthFlow {
    @Test
    fun `saslAuthFlow should succeed when valid login`(): Unit = runBlocking {
        PgConnectionHelper.defaultSuspendingConnection().use {
            it.sendSimpleQuery("SELECT 1")
        }
    }
}
