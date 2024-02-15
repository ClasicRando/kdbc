package com.github.clasicrando.common.message

import com.github.clasicrando.common.message.MessageSendBuffer

/**
 * Server message encoder. Enables writing a message of type [T] to the output channel that passes
 * messages to the database server.
 */
interface MessageEncoder<in T> {
    /** Encode the message [value] of type [T] to the [buffer] supplied */
    fun encode(value: T, buffer: MessageSendBuffer)
}
