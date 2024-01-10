package com.github.clasicrando.postgresql.connection

import com.github.clasicrando.common.connection.Connection
import com.github.clasicrando.common.result.QueryResult
import kotlinx.coroutines.channels.ReceiveChannel
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.FlowCollector
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.consumeAsFlow
import kotlinx.coroutines.flow.flow

interface PgConnection : Connection {
    suspend fun copyIn(copyInStatement: String, data: Flow<ByteArray>): QueryResult
    suspend fun copyOut(copyOutStatement: String): ReceiveChannel<ByteArray>
}

suspend fun PgConnection.copyIn(
    copyInStatement: String,
    block: suspend FlowCollector<ByteArray>.() -> Unit,
): QueryResult {
    return copyIn(copyInStatement, flow(block))
}
suspend fun PgConnection.copyInSequence(
    copyInStatement: String,
    data: Sequence<ByteArray>,
): QueryResult {
    return copyIn(copyInStatement, data.asFlow())
}
suspend fun PgConnection.copyInSequence(
    copyInStatement: String,
    block: suspend SequenceScope<ByteArray>.() -> Unit
): QueryResult {
    return copyIn(copyInStatement, sequence(block).asFlow())
}
suspend fun PgConnection.copyOutAsFlow(copyOutStatement: String): Flow<ByteArray> {
    return copyOut(copyOutStatement).consumeAsFlow()
}
