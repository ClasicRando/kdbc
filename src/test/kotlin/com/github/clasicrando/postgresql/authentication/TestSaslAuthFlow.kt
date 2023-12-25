package com.github.clasicrando.postgresql.authentication

import com.github.clasicrando.postgresql.PgConnection
import com.github.clasicrando.postgresql.PgConnectionHelper
import kotlinx.coroutines.job
import kotlinx.coroutines.runBlocking
import kotlin.test.Test

class TestSaslAuthFlow {
    @Test
    fun `saslAuthFlow should succeed when valid login`(): Unit = runBlocking {
        var connection: PgConnection? = null
        try {
            connection = PgConnectionHelper.defaultConnection(this)
            connection.sendQuery("SELECT 1")
        } catch (ex: Throwable) {
            throw ex
        } finally {
            connection?.close()
        }
        for (child in this.coroutineContext.job.children) {
            println(child)
        }
    }
}
