package io.github.clasicrando.kdbc.mysql.authentication

import io.github.clasicrando.kdbc.core.query.execute
import io.github.clasicrando.kdbc.core.query.query
import io.github.clasicrando.kdbc.core.use
import io.github.clasicrando.kdbc.mysql.MySqlConnectionHelper
import kotlin.test.Test
import kotlinx.coroutines.runBlocking

class TestAuthFlow {
    @Test
    fun `authFlow should succeed when valid login`(): Unit = runBlocking {
        MySqlConnectionHelper.defaultConnection().use { query("SHOW CHARACTER SET").execute(it) }
    }

    @Test
    fun `authFlow should succeed when valid login and ssl`(): Unit = runBlocking {
        MySqlConnectionHelper.defaultConnectionWithSsl().use {
            query("SHOW CHARACTER SET").execute(it)
        }
    }
}
