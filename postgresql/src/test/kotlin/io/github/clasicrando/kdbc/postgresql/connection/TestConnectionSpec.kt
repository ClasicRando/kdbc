package io.github.clasicrando.kdbc.postgresql.connection

import io.github.clasicrando.kdbc.core.use
import io.github.clasicrando.kdbc.postgresql.PgConnectionHelper
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assertions.assertTrue
import kotlin.test.Test

class TestConnectionSpec {
    @Test
    fun `isValid should return true for connection`(): Unit = runBlocking {
        PgConnectionHelper.defaultConnection().use {
            assertTrue(it.isValid())
        }
    }
}
