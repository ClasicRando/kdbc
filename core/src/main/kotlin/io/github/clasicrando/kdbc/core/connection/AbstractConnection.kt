package io.github.clasicrando.kdbc.core.connection

import io.github.clasicrando.kdbc.core.exceptions.UnexpectedTransactionState
import io.github.clasicrando.kdbc.core.logWithResource
import io.github.clasicrando.kdbc.core.query.execute
import io.github.clasicrando.kdbc.core.query.query
import io.github.oshai.kotlinlogging.KotlinLogging
import io.github.oshai.kotlinlogging.Level
import kotlin.uuid.Uuid
import kotlinx.atomicfu.AtomicBoolean
import kotlinx.atomicfu.atomic

private val logger = KotlinLogging.logger {}

/**
 * Base [Connection] implementation for supplying transaction commands using default implementations
 */
public abstract class AbstractConnection : Connection {
    final override val resourceId: Uuid = Uuid.random()

    final override val resourceIdAsString: String = resourceId.toString()

    private val _inTransaction: AtomicBoolean = atomic(false)
    final override val inTransaction: Boolean
        get() = _inTransaction.value

    protected open val beginQuery: String = "BEGIN;"

    final override suspend fun begin() {
        try {
            query(beginQuery).execute(this)
            if (!_inTransaction.compareAndSet(expect = false, update = true)) {
                throw UnexpectedTransactionState(inTransaction = true)
            }
        } catch (ex: OutOfMemoryError) {
            throw ex
        } catch (ex: UnexpectedTransactionState) {
            throw ex
        } catch (ex: Throwable) {
            if (!_inTransaction.compareAndSet(expect = true, update = false)) {
                try {
                    rollback()
                } catch (ex2: Throwable) {
                    logWithResource(logger, Level.WARN) {
                        message =
                            "Error while trying to rollback. " + "BEGIN called while in transaction"
                        cause = ex2
                    }
                    ex.addSuppressed(ex2)
                }
            }
            throw ex
        }
    }

    protected open val commitQuery: String = "COMMIT;"

    final override suspend fun commit() {
        try {
            query(commitQuery).execute(this)
        } finally {
            if (!_inTransaction.compareAndSet(expect = true, update = false)) {
                logWithResource(logger, Level.WARN) {
                    this.message = "Attempted to COMMIT a connection not within a transaction"
                }
            }
        }
    }

    protected open val rollbackQuery: String = "ROLLBACK;"

    final override suspend fun rollback() {
        try {
            query(rollbackQuery).execute(this)
        } finally {
            if (!_inTransaction.compareAndSet(expect = true, update = false)) {
                logWithResource(logger, Level.WARN) {
                    this.message = "Attempted to ROLLBACK a connection not within a transaction"
                }
            }
        }
    }
}
