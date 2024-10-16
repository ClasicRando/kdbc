package io.github.clasicrando.kdbc.core.pool

import io.github.clasicrando.kdbc.core.connection.AsyncConnection
import io.mockk.coEvery
import io.mockk.every
import io.mockk.mockk
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import kotlin.uuid.Uuid
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

class TestAbstractDefaultAsyncConnectionPool {
    @ParameterizedTest
    @ValueSource(ints = [0, 1])
    fun `acquire should return connection`(minConnections: Int): Unit = runBlocking {
        val factory = mockk<AsyncConnectionProvider<AsyncConnection>>()
        coEvery { factory.validate(any()) } returns true
        coEvery { factory.create(any()) } answers {
            val connectionId = Uuid.random()
            val connection = mockk<AsyncConnection>(relaxed = true)
            every { connection.resourceId } returns connectionId
            connection
        }
        val options = PoolOptions(
            maxConnections = 1,
            minConnections = minConnections,
            acquireTimeout = 1.toDuration(DurationUnit.SECONDS),
        )
        TestAsyncConnectionPoolImpl(
            poolOptions = options,
            provider = factory,
        ).use {
            assertDoesNotThrow { it.acquire() }
        }
    }

    @Test
    fun `acquire should return connection after suspending when pool exhausted`(): Unit = runBlocking {
        val factory = mockk<AsyncConnectionProvider<AsyncConnection>>()
        coEvery { factory.validate(any()) } returns true
        coEvery { factory.create(any()) } answers {
            val connectionId = Uuid.random()
            val connection = mockk<AsyncConnection>(relaxed = true)
            every { connection.resourceId } returns connectionId
            connection
        }
        val options = PoolOptions(maxConnections = 1, minConnections = 0)
        TestAsyncConnectionPoolImpl(
            poolOptions = options,
            provider = factory,
        ).use {
            val heldConnection = it.acquire()
            val expectedId = heldConnection.resourceId
            launch {
                delay(2_000)
                it.giveBack(heldConnection)
            }
            val result = withTimeout(10_000) {
                it.acquire()
            }

            assertEquals(expectedId, result.resourceId)
        }
    }

    @Test
    fun `acquire should throw cancellation exception when acquire duration exceeded`(): Unit = runBlocking {
        val factory = mockk<AsyncConnectionProvider<AsyncConnection>>()
        coEvery { factory.validate(any()) } returns true
        coEvery { factory.create(any()) } answers {
            val connectionId = Uuid.random()
            val connection = mockk<AsyncConnection>(relaxed = true)
            every { connection.resourceId } returns connectionId
            connection
        }
        val options = PoolOptions(
            maxConnections = 0,
            minConnections = 0,
            acquireTimeout = 1.toDuration(DurationUnit.NANOSECONDS),
        )
        TestAsyncConnectionPoolImpl(
            poolOptions = options,
            provider = factory,
        ).use {
            assertThrows<AcquireTimeout> { it.acquire() }
        }
    }

    @Test
    fun `giveBack should return connection to pool when returned connection is valid`(): Unit = runBlocking {
        val factory = mockk<AsyncConnectionProvider<AsyncConnection>>()
        coEvery { factory.validate(any()) } returns true
        coEvery { factory.create(any()) } answers {
            val connectionId = Uuid.random()
            val connection = mockk<AsyncConnection>(relaxed = true)
            every { connection.resourceId } returns connectionId
            connection
        }
        val options = PoolOptions(
            maxConnections = 1,
            minConnections = 0,
            acquireTimeout = 5.toDuration(DurationUnit.SECONDS),
        )
        TestAsyncConnectionPoolImpl(
            poolOptions = options,
            provider = factory,
        ).use {
            val acquiredConnection = it.acquire()
            val result = it.giveBack(acquiredConnection)

            assertTrue(result)
            assertTrue((it as AbstractDefaultAsyncConnectionPool).hasConnection(acquiredConnection))

            assertDoesNotThrow { it.acquire() }
        }
    }

    @Test
    fun `initialize should return false when first connection is invalid`(): Unit = runBlocking {
        val factory = mockk<AsyncConnectionProvider<AsyncConnection>>()
        coEvery { factory.validate(any()) } returns false
        coEvery { factory.create(any()) } answers {
            val connectionId = Uuid.random()
            val connection = mockk<AsyncConnection>(relaxed = true)
            every { connection.resourceId } returns connectionId
            connection
        }
        val options = PoolOptions(
            maxConnections = 1,
            minConnections = 0,
            acquireTimeout = 5.toDuration(DurationUnit.SECONDS),
        )
        TestAsyncConnectionPoolImpl(
            poolOptions = options,
            provider = factory,
        ).use {
            val isValid = it.initialize()
            assertFalse(isValid)
        }
    }

    @Test
    fun `initialize should return true when first connection is valid`(): Unit = runBlocking {
        val factory = mockk<AsyncConnectionProvider<AsyncConnection>>()
        coEvery { factory.validate(any()) } returns true
        coEvery { factory.create(any()) } answers {
            val connectionId = Uuid.random()
            val connection = mockk<AsyncConnection>(relaxed = true)
            every { connection.resourceId } returns connectionId
            connection
        }
        val options = PoolOptions(
            maxConnections = 1,
            minConnections = 0,
            acquireTimeout = 5.toDuration(DurationUnit.SECONDS),
        )
        TestAsyncConnectionPoolImpl(
            poolOptions = options,
            provider = factory,
        ).use {
            val isValid = it.initialize()
            assertTrue(isValid)
        }
    }

    @Test
    fun `initialize return false when create throws`(): Unit = runBlocking {
        val factory = mockk<AsyncConnectionProvider<AsyncConnection>>()
        coEvery { factory.validate(any()) } returns true
        val throwableMessage = "Special Throwable"
        coEvery { factory.create(any()) } throws Throwable(throwableMessage)
        val options = PoolOptions(
            maxConnections = 1,
            minConnections = 0,
            acquireTimeout = 5.toDuration(DurationUnit.SECONDS),
        )
        TestAsyncConnectionPoolImpl(
            poolOptions = options,
            provider = factory,
        ).use {
            val result = assertDoesNotThrow { it.initialize() }
            assertFalse(result)
        }
    }
}

private suspend inline fun <R, C : AsyncConnection> AsyncConnectionPool<C>.use(
    crossinline block: suspend (AsyncConnectionPool<C>) -> R
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

internal class TestAsyncConnectionPoolImpl(
    poolOptions: PoolOptions,
    provider: AsyncConnectionProvider<AsyncConnection>,
) : AbstractDefaultAsyncConnectionPool<AsyncConnection>(poolOptions, provider) {
    override suspend fun disposeConnection(connection: AsyncConnection) = Unit
}
