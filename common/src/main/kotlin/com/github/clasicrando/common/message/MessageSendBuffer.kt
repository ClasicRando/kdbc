package com.github.clasicrando.common.message

import com.github.clasicrando.common.buffer.ArrayWriteBuffer
import com.github.clasicrando.common.buffer.WriteBuffer
import com.github.clasicrando.common.buffer.buildBytes
import com.github.clasicrando.common.use
import com.github.clasicrando.common.useSuspend

class MessageSendBuffer @PublishedApi internal constructor() : ArrayWriteBuffer() {
    companion object {
        suspend inline fun useBuffer(crossinline block: suspend MessageSendBuffer.() -> Unit) {
            MessageSendBuffer().useSuspend(block)
        }

        inline fun buildByteArray(crossinline block: MessageSendBuffer.() -> Unit): ByteArray {
            return MessageSendBuffer().buildBytes(block)
        }
    }
}
