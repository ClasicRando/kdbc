package io.github.clasicrando.kdbc.core.pool

import io.github.clasicrando.kdbc.core.exceptions.KdbcException
import java.time.Instant
import kotlinx.atomicfu.AtomicRef
import kotlinx.atomicfu.atomic
import kotlinx.coroutines.Job

/**
 * Generic entry within a [ConcurrentBag]. Tracks the creation [Instant] of the item and the
 * [Instant] of the last time the item was accessed. [lastAccessed] is only updated once the item is
 * returned to the pool.
 */
internal class PoolEntry<T : Any> private constructor(var internalItem: T?) : ConcurrentBagEntry {
    var lastAccessed: Instant = Instant.now()
    private val status: AtomicRef<EntryStatus> = atomic(EntryStatus.Idle)
    var isEvicted: Boolean = false
    private var keepAliveJob: Job? = null
    private var maxLifetimeJob: Job? = null

    val item
        get() = internalItem ?: throw KdbcException("Tried to access pool entry after close")

    fun setKeepAliveJob(job: Job) {
        keepAliveJob = job
    }

    fun setMaxLifetimeJob(job: Job) {
        maxLifetimeJob = job
    }

    override fun compareAndSetStatus(expectedStatus: EntryStatus, newStatus: EntryStatus): Boolean {
        return status.compareAndSet(expectedStatus, newStatus)
    }

    override fun setStatus(status: EntryStatus) {
        this.status.value = status
    }

    override fun getStatus(): EntryStatus {
        return status.value
    }

    fun close(): T {
        val output = internalItem!!
        isEvicted = true
        internalItem = null
        keepAliveJob?.cancel()
        maxLifetimeJob?.cancel()
        return output
    }

    override fun toString(): String {
        return "PoolEntry(item=$internalItem,lastAccessed=$lastAccessed,status${status.value})"
    }

    companion object {
        fun <T : Any> of(item: T): PoolEntry<T> {
            return PoolEntry(item)
        }
    }
}
