package io.github.clasicrando.kdbc.postgresql.authentication

import io.github.clasicrando.kdbc.postgresql.PgConnectionHelper
import kotlin.test.Test

class TestBlockingSaslAuthFlow {
    @Test
    fun `saslAuthFlow should succeed when valid login`() {
        PgConnectionHelper.defaultBlockingConnection().use {
            it.sendSimpleQuery("SELECT 1")
        }
    }
}
