package com.github.clasicrando.postgresql

import com.github.clasicrando.common.Connection
import com.github.clasicrando.common.result.QueryResult
import kotlinx.coroutines.channels.ReceiveChannel
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.FlowCollector
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.flow

interface PgConnection : Connection {
    suspend fun copyIn(copyInStatement: String, data: Flow<ByteArray>): QueryResult
    suspend fun copyIn(
        copyInStatement: String,
        block: suspend FlowCollector<ByteArray>.() -> Unit,
    ): QueryResult {
        return copyIn(copyInStatement, flow(block))
    }
    suspend fun copyIn(copyInStatement: String, data: Sequence<ByteArray>): QueryResult {
        return copyIn(copyInStatement, data.asFlow())
    }
    suspend fun copyIn(
        copyInStatement: String,
        block: suspend SequenceScope<ByteArray>.() -> Unit
    ): QueryResult {
        return copyIn(copyInStatement, sequence(block).asFlow())
    }
    suspend fun copyOutAsFlow(copyOutStatement: String): Flow<ByteArray> {
        val channel = copyOut(copyOutStatement)
        return flow {
            for (bytes in channel) {
                emit(bytes)
            }
        }
    }
    suspend fun copyOut(copyOutStatement: String): ReceiveChannel<ByteArray>
}