package com.github.clasicrando.postgresql.authentication

import com.github.clasicrando.common.connection.use
import com.github.clasicrando.postgresql.PgConnectionHelper
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable
import kotlin.test.Test

class TestSaslAuthFlow {
    @Test
    @EnabledIfEnvironmentVariable(named = "PG_TEST_PASSWORD", matches = ".+")
    fun `saslAuthFlow should succeed when valid login`(): Unit = runBlocking {
        PgConnectionHelper.defaultConnection(this).use {
            it.sendQuery("SELECT 1")
        }
    }
}
