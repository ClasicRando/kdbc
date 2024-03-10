package com.github.clasicrando.common.stream

import java.net.InetSocketAddress

/**
 * [Exception] thrown when an [AsyncStream] fails to connect to the host for whatever reason. The
 * original [Throwable] (if any) is suppressed and the message references the host address.
 */
class StreamConnectError(
    inetSocketAddress: InetSocketAddress,
    throwable: Throwable? = null,
) : Exception("Unable to connect to host: $inetSocketAddress") {
    init {
        throwable?.let { addSuppressed(it) }
    }
}
