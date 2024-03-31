package com.github.clasicrando.common.stream

import com.github.clasicrando.common.exceptions.KdbcException
import io.ktor.network.sockets.SocketAddress

/**
 * [Exception] thrown when an [AsyncStream] fails to connect to the host for whatever reason. The
 * original [Throwable] (if any) is suppressed and the message references the host address.
 */
class StreamConnectError(
    socketAddress: SocketAddress,
    throwable: Throwable? = null,
) : KdbcException("Unable to connect to host: $socketAddress") {
    init {
        throwable?.let { addSuppressed(it) }
    }
}
