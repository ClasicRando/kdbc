package com.clasicrando.kdbc

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.withContext
import java.sql.Connection
import kotlin.contracts.ExperimentalContracts
import kotlin.contracts.InvocationKind
import kotlin.contracts.contract
import kotlin.coroutines.AbstractCoroutineContextElement
import kotlin.coroutines.CoroutineContext
import kotlin.coroutines.coroutineContext

/**
 * Executes a [block] of code within a db transaction, using the [context] provided, suspending until the block of code
 * has finished. If the current context does not have an open transaction, a [Connection] is obtained and set to
 * manual commit mode for the duration of the block.
 *
 * @throws IllegalArgumentException the [context]'s transaction has already completed and cannot be restarted
 */
@PublishedApi
@OptIn(ExperimentalContracts::class)
internal suspend inline fun <T> withTransaction(
    context: CoroutineContext,
    crossinline block: suspend CoroutineScope.() -> T,
): T {
    contract {
        callsInPlace(block, InvocationKind.AT_MOST_ONCE)
    }
    return withContext(context) {
        coroutineContext[CoroutineTransaction]?.let { transaction ->
            require(!transaction.incomplete) {
                "Transaction in current context is complete. Cannot start a new transaction within this context"
            }
            block()
        } ?: withConnection(context) {
            runAsTransaction(block)
        }
    }
}

/**
 * Executes the [block] of code within a transaction, suspending until the block is complete, committing after a
 * successful run or rolling back when any [Throwable] is thrown. All exceptions are rethrown so anyone using this
 * function should handle the exceptions accordingly.
 *
 * @throws java.sql.SQLException connection throws an exception
 */
@PublishedApi
@OptIn(ExperimentalContracts::class)
internal suspend inline fun <T> runAsTransaction(crossinline block: suspend CoroutineScope.() -> T): T {
    contract {
        callsInPlace(block, InvocationKind.EXACTLY_ONCE)
    }
    coroutineContext.connection.runWithManualCommit {
        val transaction = CoroutineTransaction()
        try {
            val result = withContext(transaction) {
                block()
            }
            commit()
            return result
        } catch (ex: Throwable) {
            rollback()
            throw ex
        } finally {
            transaction.complete()
        }
    }
}

/**
 * Executes a [block] of code against a [Connection], setting the [autoCommit][Connection.getAutoCommit] property of the
 * connection to false for the duration of the block. Reverts the property to it's initial state after execution
 *
 * @throws java.sql.SQLException connection throws an exception
 */
@PublishedApi
@OptIn(ExperimentalContracts::class)
internal inline fun <T> Connection.runWithManualCommit(block: Connection.() -> T): T {
    contract {
        callsInPlace(block, InvocationKind.EXACTLY_ONCE)
    }
    val before = autoCommit
    return try {
        autoCommit = false
        this.run(block)
    } finally {
        autoCommit = before
    }
}

/**
 * [CoroutineContext] element holding the status of a database transaction. Used to pass along the same transaction
 * status to a coroutine even when switching contexts, resuming execution or switching
 * [Dispatchers][kotlinx.coroutines.Dispatchers]. This element should always be used in conjunction with a
 * [CoroutineConnection] since this element is just the commit state of a [Connection]
 */
@PublishedApi
internal class CoroutineTransaction(
    private var completed: Boolean = false
) : AbstractCoroutineContextElement(CoroutineTransaction) {

    companion object Key : CoroutineContext.Key<CoroutineTransaction>

    val incomplete: Boolean
        get() = !completed

    fun complete() {
        completed = true
    }

    override fun toString(): String = "CoroutineTransaction(completed=$completed)"
}
