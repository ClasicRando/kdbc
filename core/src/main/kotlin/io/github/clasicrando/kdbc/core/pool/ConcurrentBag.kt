package io.github.clasicrando.kdbc.core.pool

import io.github.oshai.kotlinlogging.KotlinLogging
import kotlinx.atomicfu.AtomicInt
import kotlinx.atomicfu.atomic
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.withTimeoutOrNull
import kotlinx.coroutines.yield
import java.util.concurrent.CopyOnWriteArrayList
import kotlin.coroutines.CoroutineContext
import kotlin.time.Duration
import kotlin.time.DurationUnit
import kotlin.time.toDuration

private val logger = KotlinLogging.logger {}

internal class ConcurrentBag<T : ConcurrentBagEntry>(
    override val coroutineContext: CoroutineContext,
    private val entryCreator: EntryCreator,
) : CoroutineScope {
    private var isClosed = false
    private val entries: MutableList<T> = CopyOnWriteArrayList()
    private val waitingCounter: AtomicInt = atomic(0)
    private val rendezvousChannel = Channel<T>(capacity = Channel.RENDEZVOUS)

    val waitingCount: Int get() = waitingCounter.value

    val values: List<T>
        get() = entries

    val size: Int
        get() = entries.size

    val idleEntriesCount: Int
        get() = entries.count { it.getStatus() == EntryStatus.Idle }

    suspend fun borrow(timeout: Duration): T? {
        try {
            waitingCounter.incrementAndGet()
            for (entry in entries) {
                if (entry.compareAndSetStatus(EntryStatus.Idle, EntryStatus.InUse)) {
                    if (waitingCount > 1) {
                        entryCreator.requestCreate(waitingCount - 1)
                    }
                    return entry
                }
            }

            entryCreator.requestCreate(waitingCount)

            return waitForRendezvous(timeout)
        } finally {
            waitingCounter.decrementAndGet()
        }
    }

    private suspend fun waitForRendezvous(timeout: Duration): T? {
        return withTimeoutOrNull(timeout = timeout) {
            while (isActive) {
                val entry = rendezvousChannel.receive()
                if (entry.compareAndSetStatus(EntryStatus.Idle, EntryStatus.InUse)) {
                    return@withTimeoutOrNull entry
                }
            }
            return@withTimeoutOrNull null
        }
    }

    suspend fun giveBack(entry: T) {
        entry.setStatus(EntryStatus.Idle)

        var iterCount = 0
        while (waitingCount > 0) {
            iterCount++
            if (
                entry.getStatus() != EntryStatus.Idle || rendezvousChannel.trySend(entry).isSuccess
            ) {
                return
            }

            // After waiting for 255 iterations, sleep coroutine rather than just yielding since the
            // waiting coroutine is taking long than expected
            if (iterCount >= 255) {
                delay(10.toDuration(DurationUnit.MICROSECONDS))
            } else {
                yield()
            }
        }
    }

    suspend fun addEntry(entry: T) {
        if (isClosed) {
            logger.atInfo { message = "Pool closed, ignoring addEntry operation" }
            return
        }
        entries.add(entry)
        attemptToHandOffEntry(entry)
    }

    fun removeEntry(entry: T): Boolean {
        if (
            !entry.compareAndSetStatus(EntryStatus.Idle, EntryStatus.Removed) &&
                !entry.compareAndSetStatus(EntryStatus.Reserved, EntryStatus.Removed)
        ) {
            return false
        }

        val wasRemoved = entries.remove(entry)
        if (!wasRemoved && !isClosed) {
            logger.atTrace { message = "Could not remove entry: $entry" }
        }
        return wasRemoved
    }

    fun reserveEntry(entry: T): Boolean {
        return entry.compareAndSetStatus(EntryStatus.Idle, EntryStatus.Reserved)
    }

    suspend fun releaseEntry(entry: T) {
        if (!entry.compareAndSetStatus(EntryStatus.Reserved, EntryStatus.Idle)) {
            logger.atWarn { message = "Attempted to release a non-reserved entry: $entry" }
            return
        }

        attemptToHandOffEntry(entry)
    }

    private suspend fun attemptToHandOffEntry(entry: T) {
        while (
            waitingCount > 0 &&
                entry.getStatus() == EntryStatus.Idle &&
                rendezvousChannel.trySend(entry).isFailure
        ) {
            yield()
        }
    }
}
