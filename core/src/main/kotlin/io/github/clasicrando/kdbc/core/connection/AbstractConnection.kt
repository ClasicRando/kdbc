package io.github.clasicrando.kdbc.core.connection

import io.github.clasicrando.kdbc.core.exceptions.UnexpectedTransactionState
import io.github.clasicrando.kdbc.core.logWithResource
import io.github.clasicrando.kdbc.core.query.execute
import io.github.clasicrando.kdbc.core.query.query
import io.github.oshai.kotlinlogging.KotlinLogging
import io.github.oshai.kotlinlogging.Level
import kotlinx.atomicfu.AtomicBoolean
import kotlinx.atomicfu.atomic
import kotlin.uuid.Uuid

private val logger = KotlinLogging.logger {}

public abstract class AbstractConnection : Connection {
    final override val resourceId: Uuid = Uuid.random()

    final override val resourceIdAsString: String = resourceId.toString()

    private val _inTransaction: AtomicBoolean = atomic(false)
    override val inTransaction: Boolean
        get() = _inTransaction.value

    override suspend fun begin() {
        try {
            query("BEGIN;").execute(this)
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

    override suspend fun commit() {
        try {
            query("COMMIT;").execute(this)
        } finally {
            if (!_inTransaction.compareAndSet(expect = true, update = false)) {
                logWithResource(logger, Level.WARN) {
                    this.message = "Attempted to COMMIT a connection not within a transaction"
                }
            }
        }
    }

    override suspend fun rollback() {
        try {
            query("ROLLBACK;").execute(this)
        } finally {
            if (!_inTransaction.compareAndSet(expect = true, update = false)) {
                logWithResource(logger, Level.WARN) {
                    this.message = "Attempted to ROLLBACK a connection not within a transaction"
                }
            }
        }
    }
}
