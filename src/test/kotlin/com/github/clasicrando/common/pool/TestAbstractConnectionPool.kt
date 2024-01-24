package com.github.clasicrando.common.pool

import com.github.clasicrando.common.connection.Connection
import io.mockk.coEvery
import io.mockk.every
import io.mockk.mockk
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import kotlinx.uuid.UUID
import kotlinx.uuid.generateUUID
import org.junit.jupiter.api.assertDoesNotThrow
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue
import kotlin.time.DurationUnit
import kotlin.time.toDuration

class TestAbstractConnectionPool {
    @Test
    fun `isExhausted should be true when maxConnections = 0`(): Unit = runBlocking {
        val factory = mockk<ConnectionProvider<Connection>>()
        coEvery { factory.validate(any()) } returns true
        coEvery { factory.create(any()) } answers {
            val connectionId = UUID.generateUUID()
            val connection = mockk<Connection>(relaxed = true)
            every { connection.connectionId } returns connectionId
            connection
        }
        val options = PoolOptions(maxConnections = 0, minConnections = 0)
        TestConnectionPoolImpl(
            poolOptions = options,
            provider = factory,
        ).use {
            assertTrue(it.isExhausted)
        }
    }

    @Test
    fun `isExhausted should be true when all connections acquired`(): Unit = runBlocking {
        val factory = mockk<ConnectionProvider<Connection>>()
        coEvery { factory.validate(any()) } returns true
        coEvery { factory.create(any()) } answers {
            val connectionId = UUID.generateUUID()
            val connection = mockk<Connection>(relaxed = true)
            every { connection.connectionId } returns connectionId
            connection
        }
        val options = PoolOptions(maxConnections = 1, minConnections = 0)
        TestConnectionPoolImpl(
            poolOptions = options,
            provider = factory,
        ).use {
            it.acquire()
            assertTrue(it.isExhausted)
        }
    }

    @ParameterizedTest
    @ValueSource(ints = [0, 1])
    fun `acquire should return connection`(minConnections: Int): Unit = runBlocking {
        val factory = mockk<ConnectionProvider<Connection>>()
        coEvery { factory.validate(any()) } returns true
        coEvery { factory.create(any()) } answers {
            val connectionId = UUID.generateUUID()
            val connection = mockk<Connection>(relaxed = true)
            every { connection.connectionId } returns connectionId
            connection
        }
        val options = PoolOptions(
            maxConnections = 1,
            minConnections = minConnections,
            acquireTimeout = 1.toDuration(DurationUnit.SECONDS),
        )
        TestConnectionPoolImpl(
            poolOptions = options,
            provider = factory,
        ).use {
            assertDoesNotThrow { it.acquire() }
        }
    }

    @Test
    fun `acquire should return connection after suspending when pool exhausted`(): Unit = runBlocking {
        val factory = mockk<ConnectionProvider<Connection>>()
        coEvery { factory.validate(any()) } returns true
        coEvery { factory.create(any()) } answers {
            val connectionId = UUID.generateUUID()
            val connection = mockk<Connection>(relaxed = true)
            every { connection.connectionId } returns connectionId
            connection
        }
        val options = PoolOptions(maxConnections = 1, minConnections = 0)
        TestConnectionPoolImpl(
            poolOptions = options,
            provider = factory,
        ).use {
            val heldConnection = it.acquire()
            val expectedId = heldConnection.connectionId
            launch {
                delay(2_000)
                it.giveBack(heldConnection)
            }
            val result = withTimeout(10_000) {
                it.acquire()
            }

            assertEquals(expectedId, result.connectionId)
        }
    }

    @Test
    fun `acquire should throw cancellation exception when acquire duration exceeded`(): Unit = runBlocking {
        val factory = mockk<ConnectionProvider<Connection>>()
        coEvery { factory.validate(any()) } returns true
        coEvery { factory.create(any()) } answers {
            val connectionId = UUID.generateUUID()
            val connection = mockk<Connection>(relaxed = true)
            every { connection.connectionId } returns connectionId
            connection
        }
        val options = PoolOptions(
            maxConnections = 0,
            minConnections = 0,
            acquireTimeout = 1.toDuration(DurationUnit.SECONDS),
        )
        TestConnectionPoolImpl(
            poolOptions = options,
            provider = factory,
        ).use {
            assertThrows<CancellationException> { it.acquire() }
        }
    }

    @Test
    fun `giveBack should return connection to pool when returned connection is valid`(): Unit = runBlocking {
        val factory = mockk<ConnectionProvider<Connection>>()
        coEvery { factory.validate(any()) } returns true
        coEvery { factory.create(any()) } answers {
            val connectionId = UUID.generateUUID()
            val connection = mockk<Connection>(relaxed = true)
            every { connection.connectionId } returns connectionId
            connection
        }
        val options = PoolOptions(
            maxConnections = 1,
            minConnections = 0,
            acquireTimeout = 5.toDuration(DurationUnit.SECONDS),
        )
        TestConnectionPoolImpl(
            poolOptions = options,
            provider = factory,
        ).use {
            val acquiredConnection = it.acquire()
            val result = it.giveBack(acquiredConnection)

            assertTrue(result)
            assertTrue((it as AbstractConnectionPool).hasConnection(acquiredConnection))

            assertDoesNotThrow { it.acquire() }
        }
    }

    @Test
    fun `giveBack should invalidate connection and request another connection when returned connection is invalid`(): Unit = runBlocking {
        val factory = mockk<ConnectionProvider<Connection>>()
        coEvery { factory.validate(any()) } returnsMany (listOf(true, true, false, true))
        coEvery { factory.create(any()) } answers {
            val connectionId = UUID.generateUUID()
            val connection = mockk<Connection>(relaxed = true)
            every { connection.connectionId } returns connectionId
            connection
        }
        val options = PoolOptions(
            maxConnections = 1,
            minConnections = 0,
            acquireTimeout = 5.toDuration(DurationUnit.SECONDS),
        )
        TestConnectionPoolImpl(
            poolOptions = options,
            provider = factory,
        ).use {
            val acquiredConnection = it.acquire()
            val result = it.giveBack(acquiredConnection)
            // Give time to invalidate connection
            delay(2_000)

            assertTrue(result)
            assertFalse((it as AbstractConnectionPool).hasConnection(acquiredConnection))

            assertDoesNotThrow { it.acquire() }
        }
    }

    @Test
    fun `waitForValidation should return false when first connection is invalid`(): Unit = runBlocking {
        val factory = mockk<ConnectionProvider<Connection>>()
        coEvery { factory.validate(any()) } returns false
        coEvery { factory.create(any()) } answers {
            val connectionId = UUID.generateUUID()
            val connection = mockk<Connection>(relaxed = true)
            every { connection.connectionId } returns connectionId
            connection
        }
        val options = PoolOptions(
            maxConnections = 1,
            minConnections = 0,
            acquireTimeout = 5.toDuration(DurationUnit.SECONDS),
        )
        TestConnectionPoolImpl(
            poolOptions = options,
            provider = factory,
        ).use {
            val isValid = it.waitForValidation()
            assertFalse(isValid)
        }
    }

    @Test
    fun `waitForValidation should return true when first connection is valid`(): Unit = runBlocking {
        val factory = mockk<ConnectionProvider<Connection>>()
        coEvery { factory.validate(any()) } returns true
        coEvery { factory.create(any()) } answers {
            val connectionId = UUID.generateUUID()
            val connection = mockk<Connection>(relaxed = true)
            every { connection.connectionId } returns connectionId
            connection
        }
        val options = PoolOptions(
            maxConnections = 1,
            minConnections = 0,
            acquireTimeout = 5.toDuration(DurationUnit.SECONDS),
        )
        TestConnectionPoolImpl(
            poolOptions = options,
            provider = factory,
        ).use {
            val isValid = it.waitForValidation()
            assertTrue(isValid)
        }
    }

    @Test
    fun `waitForValidation throws exception when create throws`(): Unit = runBlocking {
        val factory = mockk<ConnectionProvider<Connection>>()
        coEvery { factory.validate(any()) } returns true
        val throwableMessage = "Special Throwable"
        coEvery { factory.create(any()) } throws Throwable(throwableMessage)
        val options = PoolOptions(
            maxConnections = 1,
            minConnections = 0,
            acquireTimeout = 5.toDuration(DurationUnit.SECONDS),
        )
        TestConnectionPoolImpl(
            poolOptions = options,
            provider = factory,
        ).use {
            val ex = assertThrows<Throwable> { it.waitForValidation() }
            assertEquals(throwableMessage, ex.message)
        }
    }
}

private suspend inline fun <R, C : Connection> ConnectionPool<C>.use(
    crossinline block: suspend (ConnectionPool<C>) -> R
): R {
    var cause: Throwable? = null
    return try {
        block(this)
    } catch (ex: Throwable) {
        cause = ex
        throw ex
    } finally {
        try {
            close()
        } catch (ex: Throwable) {
            cause?.addSuppressed(ex)
        }
    }
}

internal class TestConnectionPoolImpl(
    poolOptions: PoolOptions,
    provider: ConnectionProvider<Connection>,
) : AbstractConnectionPool<Connection>(poolOptions, provider) {
    override suspend fun disposeConnection(connection: Connection) = Unit
}
