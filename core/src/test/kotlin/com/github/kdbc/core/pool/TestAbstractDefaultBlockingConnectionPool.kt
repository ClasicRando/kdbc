package com.github.kdbc.core.pool

import com.github.kdbc.core.connection.BlockingConnection
import io.mockk.coEvery
import io.mockk.every
import io.mockk.mockk
import kotlinx.uuid.UUID
import kotlinx.uuid.generateUUID
import org.junit.jupiter.api.assertDoesNotThrow
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import kotlin.concurrent.thread
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue
import kotlin.time.DurationUnit
import kotlin.time.toDuration

class TestAbstractDefaultBlockingConnectionPool {
    @ParameterizedTest
    @ValueSource(ints = [0, 1])
    fun `acquire should return connection`(minConnections: Int) {
        val factory = mockk<BlockingConnectionProvider<BlockingConnection>>()
        coEvery { factory.validate(any()) } returns true
        coEvery { factory.create(any()) } answers {
            val connectionId = UUID.generateUUID()
            val connection = mockk<BlockingConnection>(relaxed = true)
            every { connection.resourceId } returns connectionId
            connection
        }
        val options = PoolOptions(
            maxConnections = 1,
            minConnections = minConnections,
            acquireTimeout = 1.toDuration(DurationUnit.SECONDS),
        )
        TestBlockingConnectionPoolImpl(
            poolOptions = options,
            provider = factory,
        ).use {
            assertDoesNotThrow { it.acquire() }
        }
    }

    @Test
    fun `acquire should return connection after suspending when pool exhausted`() {
        val factory = mockk<BlockingConnectionProvider<BlockingConnection>>()
        coEvery { factory.validate(any()) } returns true
        coEvery { factory.create(any()) } answers {
            val connectionId = UUID.generateUUID()
            val connection = mockk<BlockingConnection>(relaxed = true)
            every { connection.resourceId } returns connectionId
            connection
        }
        val options = PoolOptions(maxConnections = 1, minConnections = 0)
        TestBlockingConnectionPoolImpl(
            poolOptions = options,
            provider = factory,
        ).use {
            val heldConnection = it.acquire()
            val expectedId = heldConnection.resourceId
            thread {
                Thread.sleep(2_000)
                it.giveBack(heldConnection)
            }
            val result = it.acquire()

            assertEquals(expectedId, result.resourceId)
        }
    }

    @Test
    fun `acquire should throw cancellation exception when acquire duration exceeded`() {
        val factory = mockk<BlockingConnectionProvider<BlockingConnection>>()
        coEvery { factory.validate(any()) } returns true
        coEvery { factory.create(any()) } answers {
            val connectionId = UUID.generateUUID()
            val connection = mockk<BlockingConnection>(relaxed = true)
            every { connection.resourceId } returns connectionId
            connection
        }
        val options = PoolOptions(
            maxConnections = 0,
            minConnections = 0,
            acquireTimeout = 1.toDuration(DurationUnit.NANOSECONDS),
        )
        TestBlockingConnectionPoolImpl(
            poolOptions = options,
            provider = factory,
        ).use {
            assertThrows<AcquireTimeout> { it.acquire() }
        }
    }

    @Test
    fun `giveBack should return connection to pool when returned connection is valid`() {
        val factory = mockk<BlockingConnectionProvider<BlockingConnection>>()
        coEvery { factory.validate(any()) } returns true
        coEvery { factory.create(any()) } answers {
            val connectionId = UUID.generateUUID()
            val connection = mockk<BlockingConnection>(relaxed = true)
            every { connection.resourceId } returns connectionId
            connection
        }
        val options = PoolOptions(
            maxConnections = 1,
            minConnections = 0,
            acquireTimeout = 5.toDuration(DurationUnit.SECONDS),
        )
        TestBlockingConnectionPoolImpl(
            poolOptions = options,
            provider = factory,
        ).use {
            val acquiredConnection = it.acquire()
            val result = it.giveBack(acquiredConnection)

            assertTrue(result)
            assertTrue((it as AbstractDefaultBlockingConnectionPool).hasConnection(acquiredConnection))

            assertDoesNotThrow { it.acquire() }
        }
    }

    @Test
    fun `initialize should return false when first connection is invalid`() {
        val factory = mockk<BlockingConnectionProvider<BlockingConnection>>()
        coEvery { factory.validate(any()) } returns false
        coEvery { factory.create(any()) } answers {
            val connectionId = UUID.generateUUID()
            val connection = mockk<BlockingConnection>(relaxed = true)
            every { connection.resourceId } returns connectionId
            connection
        }
        val options = PoolOptions(
            maxConnections = 1,
            minConnections = 0,
            acquireTimeout = 5.toDuration(DurationUnit.SECONDS),
        )
        TestBlockingConnectionPoolImpl(
            poolOptions = options,
            provider = factory,
        ).use {
            val isValid = it.initialize()
            assertFalse(isValid)
        }
    }

    @Test
    fun `initialize should return true when first connection is valid`() {
        val factory = mockk<BlockingConnectionProvider<BlockingConnection>>()
        coEvery { factory.validate(any()) } returns true
        coEvery { factory.create(any()) } answers {
            val connectionId = UUID.generateUUID()
            val connection = mockk<BlockingConnection>(relaxed = true)
            every { connection.resourceId } returns connectionId
            connection
        }
        val options = PoolOptions(
            maxConnections = 1,
            minConnections = 0,
            acquireTimeout = 5.toDuration(DurationUnit.SECONDS),
        )
        TestBlockingConnectionPoolImpl(
            poolOptions = options,
            provider = factory,
        ).use {
            val isValid = it.initialize()
            assertTrue(isValid)
        }
    }

    @Test
    fun `initialize return false when create throws`() {
        val factory = mockk<BlockingConnectionProvider<BlockingConnection>>()
        coEvery { factory.validate(any()) } returns true
        val throwableMessage = "Special Throwable"
        coEvery { factory.create(any()) } throws Throwable(throwableMessage)
        val options = PoolOptions(
            maxConnections = 1,
            minConnections = 0,
            acquireTimeout = 5.toDuration(DurationUnit.SECONDS),
        )
        TestBlockingConnectionPoolImpl(
            poolOptions = options,
            provider = factory,
        ).use {
            val result = assertDoesNotThrow { it.initialize() }
            assertFalse(result)
        }
    }
}

private inline fun <R, C : BlockingConnection> BlockingConnectionPool<C>.use(
    crossinline block: (BlockingConnectionPool<C>) -> R
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

internal class TestBlockingConnectionPoolImpl(
    poolOptions: PoolOptions,
    provider: BlockingConnectionProvider<BlockingConnection>,
) : AbstractDefaultBlockingConnectionPool<BlockingConnection>(poolOptions, provider) {
    override fun disposeConnection(connection: BlockingConnection) = Unit
}
