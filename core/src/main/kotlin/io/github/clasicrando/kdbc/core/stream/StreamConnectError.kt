package io.github.clasicrando.kdbc.core.stream

import io.github.clasicrando.kdbc.core.exceptions.KdbcException
import io.ktor.network.sockets.SocketAddress

/**
 * [Exception] thrown when an [Stream] fails to connect to the host for whatever reason. The
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
