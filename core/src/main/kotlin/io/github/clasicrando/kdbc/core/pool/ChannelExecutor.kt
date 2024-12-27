package io.github.clasicrando.kdbc.core.pool

import kotlinx.atomicfu.AtomicInt
import kotlinx.atomicfu.atomic
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.isActive
import kotlinx.coroutines.job
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.sync.withPermit
import kotlin.coroutines.CoroutineContext

internal class ChannelExecutor(
    parentScope: CoroutineScope,
    parallelism: Int,
    channelCapacity: Int,
) : CoroutineScope, AutoCloseable {
    private var dispatcher = Dispatchers.Default.limitedParallelism(parallelism)
    private val channelRequestCount: AtomicInt = atomic(0)
    private val requestChannel = Channel<Action>(capacity = channelCapacity)
    private var semaphore = Semaphore(permits = parallelism)

    override val coroutineContext: CoroutineContext =
        SupervisorJob(parent = parentScope.coroutineContext.job)

    init {
        launch(Dispatchers.Default.limitedParallelism(1)) {
            while (isActive) {
                val action = requestChannel.receive()
                this.launch(dispatcher) { semaphore.withPermit { action.call() } }
            }
        }
    }

    val requestCount: Int
        get() = channelRequestCount.value

    fun updateParallelism(parallelism: Int) {
        dispatcher = Dispatchers.Default.limitedParallelism(parallelism)
        semaphore = Semaphore(permits = parallelism)
    }

    fun sendRequest(action: Action) {
        channelRequestCount.incrementAndGet()
        requestChannel.trySend(action)
    }

    override fun close() {
        cancel()
        requestChannel.close()
    }

    interface Action {
        suspend fun call()
    }
}
