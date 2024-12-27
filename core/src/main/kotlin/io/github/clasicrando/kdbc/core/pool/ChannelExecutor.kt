package io.github.clasicrando.kdbc.core.pool

import kotlinx.atomicfu.AtomicInt
import kotlinx.atomicfu.atomic
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.job
import kotlinx.coroutines.launch
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit
import kotlin.coroutines.CoroutineContext

internal class ChannelExecutor(parentScope: CoroutineScope, parallelism: Int, channelCapacity: Int) : CoroutineScope {
    private var dispatcher = Dispatchers.Default.limitedParallelism(parallelism)
    private val channelRequestCount: AtomicInt = atomic(0)
    private val requestChannel = Channel<Action>(capacity = channelCapacity)

    override var coroutineContext: CoroutineContext = SupervisorJob(parent = parentScope.coroutineContext.job) + dispatcher

    init {
        launch(Dispatchers.Default.limitedParallelism(1)) {
            while (true) {
                val action = requestChannel.receive()
                this.launch(dispatcher) { action.call() }
            }
        }
    }

    val requestCount: Int get() = channelRequestCount.value

    fun updateParallelism(parallelism: Int) {
        dispatcher = Dispatchers.Default.limitedParallelism(parallelism)
        coroutineContext = coroutineContext.job + dispatcher
    }

    suspend fun sendRequest(action: Action) {
        ThreadPoolExecutor(1, 1, 1, TimeUnit.MILLISECONDS, LinkedBlockingQueue()).submit({})
        channelRequestCount.incrementAndGet()
        requestChannel.send(action)
    }

    interface Action {
        suspend fun call()
    }
}
