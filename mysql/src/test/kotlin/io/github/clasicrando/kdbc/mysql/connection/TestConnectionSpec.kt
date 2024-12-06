package io.github.clasicrando.kdbc.mysql.connection

import io.github.clasicrando.kdbc.core.use
import io.github.clasicrando.kdbc.mysql.MySqlConnectionHelper
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

class TestConnectionSpec {
    @Test
    fun `isValid should return true for connection`(): Unit = runBlocking {
        MySqlConnectionHelper.defaultConnection().use {
            assertTrue(it.isValid())
        }
    }
}
