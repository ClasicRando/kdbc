package com.github.clasicrando.common.message

import io.ktor.utils.io.core.BytePacketBuilder

/**
 * Server message encoder. Enables writing a message of type [T] to the output channel that passes
 * messages to the database server.
 *
 * TODO
 * - move out of java.nio.ByteBuffer once Ktor uses a multiplatform option or kotlinx-io matures
 */
internal interface MessageEncoder<in T> {
    /** Encode the message [value] of type [T] to the [buffer] supplied */
    fun encode(value: T, buffer: BytePacketBuilder)
}
