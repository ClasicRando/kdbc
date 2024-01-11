package com.github.clasicrando.postgresql.notification

import com.github.clasicrando.common.Loop
import com.github.clasicrando.common.selectLoop
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.job
import kotlinx.coroutines.selects.SelectClause1

interface PgListener {
    suspend fun listenAll(vararg channelNames: String) {
        for (channelName in channelNames) {
            listen(channelName)
        }
    }
    suspend fun listen(channelName: String)
    suspend fun unlisten(channelName: String)
    suspend fun unlistenAll()
    suspend fun receiveNotification(): PgNotification
    val onReceive: SelectClause1<PgNotification>
    suspend fun close()
}

fun PgListener.asFlow() = flow {
    val ctx = currentCoroutineContext()
    selectLoop {
        onReceive {
            emit(it)
            Loop.Continue
        }
        ctx.job.onJoin {
            Loop.Break
        }
    }
}
