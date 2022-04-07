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

@OptIn(ExperimentalContracts::class)
suspend inline fun <T> withTransaction(crossinline block: suspend CoroutineScope.() -> T): T {
    contract {
        callsInPlace(block, InvocationKind.AT_MOST_ONCE)
    }
    return coroutineContext[CoroutineTransaction]?.let { transaction ->
        require(!transaction.incomplete) {
            "Transaction in current context is complete. Cannot start a new transaction within this context"
        }
        withContext(coroutineContext) {
            block()
        }
    } ?: withConnection {
        runAsTransaction(block)
    }
}

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
