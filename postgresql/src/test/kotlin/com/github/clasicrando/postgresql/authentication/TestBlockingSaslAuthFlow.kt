package com.github.clasicrando.postgresql.authentication

import com.github.clasicrando.common.connection.use
import com.github.clasicrando.postgresql.PgConnectionHelper
import kotlin.test.Test

class TestBlockingSaslAuthFlow {
    @Test
    fun `saslAuthFlow should succeed when valid login`() {
        PgConnectionHelper.defaultBlockingConnection().use {
            it.sendQuery("SELECT 1")
        }
    }
}
