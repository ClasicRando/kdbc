package com.github.kdbc.postgresql.authentication

import com.github.kdbc.core.connection.use
import com.github.kdbc.postgresql.PgConnectionHelper
import kotlin.test.Test

class TestBlockingSaslAuthFlow {
    @Test
    fun `saslAuthFlow should succeed when valid login`() {
        PgConnectionHelper.defaultBlockingConnection().use {
            it.sendQuery("SELECT 1")
        }
    }
}
