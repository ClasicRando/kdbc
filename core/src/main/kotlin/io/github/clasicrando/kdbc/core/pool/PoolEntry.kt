package io.github.clasicrando.kdbc.core.pool

import java.time.Instant
import kotlinx.atomicfu.AtomicRef
import kotlinx.atomicfu.atomic
import kotlinx.coroutines.Job

/**
 * Generic entry within a [ConcurrentBag]. Tracks the creation [Instant] of the item and the
 * [Instant] of the last time the item was accessed. [lastAccessed] is only updated once the item is
 * returned to the pool.
 */
internal class PoolEntry<T : Any>(
    val item: T,
) : ConcurrentBagEntry {
    var lastAccessed: Instant = Instant.now()
    private val status: AtomicRef<EntryStatus> = atomic(EntryStatus.Idle)
    var isEvicted: Boolean = false
    private var keepAliveJob: Job? = null
    private var maxLifetimeJob: Job? = null

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

    override fun toString(): String {
        return "PoolEntry(item=$item,lastAccessed=$lastAccessed,status${status.value})"
    }
}
