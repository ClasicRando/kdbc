package io.github.clasicrando.kdbc.core.stream

import kotlin.time.Duration
import kotlin.time.DurationUnit
import kotlin.time.toDuration
import kotlinx.serialization.Serializable

@Serializable
public data class SocketOptions(
    /** Timeout duration during initial TCP connection establishment */
    val connectTimeout: Duration = 10.toDuration(DurationUnit.SECONDS),
    /** Duration that a socket should wait during a read or write operation before timing out */
    val socketTimeout: Duration = Duration.INFINITE,
    /** Used as the socket's SO_KEEPALIVE option */
    val tcpKeepAlive: Boolean = true,
    /** Used as the socket's TCP_NODELAY */
    val tcpNoDelay: Boolean = true,
) {
    init {
        require(connectTimeout.isPositive()) { "connectTimeout must be positive" }
        require(socketTimeout.isPositive()) { "connectTimeout must be positive" }
    }
}
