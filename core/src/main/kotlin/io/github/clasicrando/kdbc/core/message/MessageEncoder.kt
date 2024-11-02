package io.github.clasicrando.kdbc.core.message

import kotlinx.io.Sink

/**
 * Server message encoder. Enables writing a message of type [T] to the output channel that passes
 * messages to the database server.
 */
interface MessageEncoder<in T> {
    /** Encode the message [value] of type [T] to the [buffer] supplied */
    fun encode(
        value: T,
        buffer: Sink,
    )
}
