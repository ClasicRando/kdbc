package com.github.kdbc.postgresql.authentication

import com.github.kdbc.core.connection.use
import com.github.kdbc.postgresql.PgConnectionHelper
import kotlinx.coroutines.runBlocking
import kotlin.test.Test

class TestSaslAuthFlow {
    @Test
    fun `saslAuthFlow should succeed when valid login`(): Unit = runBlocking {
        PgConnectionHelper.defaultConnection().use {
            it.sendQuery("SELECT 1")
        }
    }
}
