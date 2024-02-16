package com.github.clasicrando.postgresql.authentication

import com.github.clasicrando.common.connection.use
import com.github.clasicrando.postgresql.PgConnectionHelper
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
