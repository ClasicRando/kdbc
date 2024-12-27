package io.github.clasicrando.kdbc.core.pool

import io.github.clasicrando.kdbc.core.connection.Connection
import io.mockk.every
import io.mockk.mockk
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.junit.jupiter.api.Timeout
import org.junit.jupiter.api.assertDoesNotThrow
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.params.provider.ValueSource
import kotlin.test.assertEquals
import kotlin.test.assertTrue
import kotlin.time.DurationUnit
import kotlin.time.toDuration
import kotlin.uuid.Uuid

const val TEST_TIMEOUT = 60L

class TestAbstractDefaultConnectionPool {
    //    @ParameterizedTest
    @Timeout(value = TEST_TIMEOUT)
    @ValueSource(ints = [0, 1])
    fun `acquire should return connection`(minConnections: Int): Unit = runBlocking {
        val options =
            PoolOptions(
                maxConnections = 1,
                minIdleConnections = minConnections,
                acquireTimeout = 1.toDuration(DurationUnit.SECONDS),
            )
        mockConnectionPool(
                poolOptions = options,
                create = {
                    val connectionId = Uuid.random()
                    val connection = mockk<Connection>(relaxed = true)
                    every { connection.resourceId } returns connectionId
                    every { connection.isConnected } returns true
                    connection
                },
                validate = { true },
            )
            .use { assertDoesNotThrow { it.acquire() } }
    }

    //    @Test
    @Timeout(value = TEST_TIMEOUT)
    fun `acquire should return connection after suspending when pool exhausted`(): Unit =
        runBlocking {
            val options = PoolOptions(maxConnections = 1, minIdleConnections = 0)
            mockConnectionPool(
                    poolOptions = options,
                    create = {
                        val connectionId = Uuid.random()
                        val connection = mockk<Connection>(relaxed = true)
                        every { connection.resourceId } returns connectionId
                        every { connection.isConnected } returns true
                        connection
                    },
                    validate = { true },
                )
                .use {
                    val heldConnection = it.acquire()
                    val expectedId = heldConnection.resourceId
                    launch {
                        delay(2_000)
                        it.giveBack(heldConnection)
                    }
                    val result = withTimeout(10_000) { it.acquire() }

                    assertEquals(expectedId, result.resourceId)
                }
        }

    //    @Test
    @Timeout(value = TEST_TIMEOUT)
    fun `acquire should throw cancellation exception when acquire duration exceeded`(): Unit =
        runBlocking {
            val options =
                PoolOptions(
                    maxConnections = 1,
                    minIdleConnections = 0,
                    acquireTimeout = 1.toDuration(DurationUnit.NANOSECONDS),
                )
            mockConnectionPool(
                    poolOptions = options,
                    create = {
                        val connectionId = Uuid.random()
                        val connection = mockk<Connection>(relaxed = true)
                        every { connection.resourceId } returns connectionId
                        every { connection.isConnected } returns true
                        connection
                    },
                    validate = { true },
                )
                .use {
                    assertDoesNotThrow { it.acquire() }
                    assertThrows<AcquireTimeout> { it.acquire() }
                }
        }

    //    @Test
    @Timeout(value = TEST_TIMEOUT)
    fun `giveBack should return connection to pool when returned connection is valid`(): Unit =
        runBlocking {
            val options =
                PoolOptions(
                    maxConnections = 1,
                    minIdleConnections = 0,
                    acquireTimeout = 5.toDuration(DurationUnit.SECONDS),
                )
            mockConnectionPool(
                    poolOptions = options,
                    create = {
                        val connectionId = Uuid.random()
                        val connection = mockk<Connection>(relaxed = true)
                        every { connection.resourceId } returns connectionId
                        every { connection.isConnected } returns true
                        connection
                    },
                    validate = { true },
                )
                .use {
                    val acquiredConnection = it.acquire()
                    val result = it.giveBack(acquiredConnection)

                    assertTrue(result)
                    assertTrue(
                        (it as AbstractDefaultConnectionPool).hasConnection(acquiredConnection)
                    )

                    assertDoesNotThrow { it.acquire() }
                }
        }

    //    @Test
    @Timeout(value = TEST_TIMEOUT)
    fun `initialize should return false when first connection is invalid`(): Unit = runBlocking {
        val options =
            PoolOptions(
                maxConnections = 1,
                minIdleConnections = 0,
                acquireTimeout = 5.toDuration(DurationUnit.SECONDS),
            )
        //        mockConnectionPool(
        //                poolOptions = options,
        //                create = {
        //                    val connectionId = Uuid.random()
        //                    val connection = mockk<Connection>(relaxed = true)
        //                    every { connection.resourceId } returns connectionId
        //                    every { connection.isConnected } returns true
        //                    connection
        //                },
        //                validate = { false },
        //            )
        //            .use {
        //                val isValid = it.initialize()
        //                assertFalse(isValid)
        //            }
    }

    //    @Test
    @Timeout(value = TEST_TIMEOUT)
    fun `initialize should return true when first connection is valid`(): Unit = runBlocking {
        val options =
            PoolOptions(
                maxConnections = 1,
                minIdleConnections = 0,
                acquireTimeout = 5.toDuration(DurationUnit.SECONDS),
            )
        //        mockConnectionPool(
        //                poolOptions = options,
        //                create = {
        //                    val connectionId = Uuid.random()
        //                    val connection = mockk<Connection>(relaxed = true)
        //                    every { connection.resourceId } returns connectionId
        //                    every { connection.isConnected } returns true
        //                    connection
        //                },
        //                validate = { true },
        //            )
        //            .use {
        //                val isValid = it.initialize()
        //                assertTrue(isValid)
        //            }
    }

    //    @Test
    @Timeout(value = TEST_TIMEOUT)
    fun `initialize return false when create throws`(): Unit = runBlocking {
        val exceptionMessage = "Special Throwable"
        val options =
            PoolOptions(
                maxConnections = 1,
                minIdleConnections = 0,
                acquireTimeout = 5.toDuration(DurationUnit.SECONDS),
            )
        //        mockConnectionPool(
        //                poolOptions = options,
        //                create = { throw Exception(exceptionMessage) },
        //                validate = { true },
        //            )
        //            .use {
        //                val result = assertDoesNotThrow { it.initialize() }
        //                assertFalse(result)
        //            }
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

private fun mockConnectionPool(
    poolOptions: PoolOptions,
    create: suspend () -> Connection,
    validate: suspend (Connection) -> Boolean,
): ConnectionPool<Connection> {
    return object : AbstractDefaultConnectionPool<Connection>(poolOptions) {
        override suspend fun create(): Connection = create()

        override suspend fun validate(connection: Connection): Boolean = validate(connection)

        override suspend fun disposeConnection(connection: Connection) = Unit
    }
}
