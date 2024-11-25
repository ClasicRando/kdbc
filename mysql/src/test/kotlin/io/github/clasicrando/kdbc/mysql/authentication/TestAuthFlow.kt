package io.github.clasicrando.kdbc.mysql.authentication

import io.github.clasicrando.kdbc.core.query.execute
import io.github.clasicrando.kdbc.core.query.query
import io.github.clasicrando.kdbc.core.use
import io.github.clasicrando.kdbc.mysql.MySqlConnectionHelper
import kotlinx.coroutines.runBlocking
import kotlin.test.Test

class TestAuthFlow {
    @Test
    fun `authFlow should succeed when valid login`(): Unit = runBlocking {
        MySqlConnectionHelper.defaultConnection().use {
            query("SHOW CHARACTER SET").execute(it)
        }
    }
}
